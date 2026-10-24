(ns jepsen.fathom.client
  "A list-append client that speaks Hrana v2 over HTTP, the same wire an
  unchanged libSQL client uses. The shard is the Host subdomain.

  Each transaction is ONE Hrana batch with step conditions, the way the libSQL
  clients do it: BEGIN, the micro-ops, then COMMIT only if every step before it
  succeeded, then ROLLBACK only if COMMIT did not. A plain pipeline of executes
  would be wrong here: SQLite leaves a transaction open after a failed
  statement, so an unconditional COMMIT would commit the half that worked."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [slingshot.slingshot :refer [try+]]))

(def domain "fathom.test")

(def direct-ports
  "LB-bypassing Hrana port per rig node (deploy/chaos/docker-compose.yml)."
  {"fathom1" 18081 "fathom2" 18082 "fathom3" 18083 "fathom4" 18084 "fathom5" 18085})

(defn base-url
  "Where a client sends requests. :lb goes through nginx, which routes each
  shard to its one owner; :direct goes to the client's node itself, so ops land
  on non-owners too and exercise the lease refusal and steal paths."
  [test via node]
  (case via
    :lb     (str "http://localhost:" (:lb-port test))
    :direct (str "http://localhost:" (direct-ports node))))

(def ^:private opened (atom 0))

(defn- via-for
  "The target this client uses. :mixed alternates clients between the LB and
  their own node, so owner-routed and non-owner traffic hit a shard at once."
  [test]
  (case (:target test)
    :mixed (if (even? (swap! opened inc)) :lb :direct)
    (:target test)))

(defn- stmt
  ([sql] {:sql sql})
  ([sql args] {:sql sql
               :args (mapv (fn [a]
                             (if (integer? a)
                               {:type "integer" :value (str a)}
                               {:type "text" :value (str a)}))
                           args)}))

(defn- ok-cond [i] {:type "ok" :step i})

(defn mop->stmt
  [[f k v]]
  (case f
    :r      (stmt "SELECT v FROM lists WHERE k = ?" [k])
    :append (stmt (str "INSERT INTO lists (k, v) VALUES (?, ?) "
                       "ON CONFLICT (k) DO UPDATE SET v = v || ',' || excluded.v")
                  [k v])))

(defn txn->batch
  "Step layout: 0 BEGIN, 1 CREATE TABLE, 2..n+1 mops, n+2 COMMIT, n+3 ROLLBACK."
  [txn]
  (let [body  (into [(stmt "CREATE TABLE IF NOT EXISTS lists (k INTEGER PRIMARY KEY, v TEXT NOT NULL)")]
                    (map mop->stmt txn))
        steps (into [{:stmt (stmt "BEGIN IMMEDIATE")}]
                    (map-indexed (fn [i s] {:condition (ok-cond i) :stmt s}) body))
        commit-i (count steps)]
    {:commit-step   commit-i
     :rollback-step (inc commit-i)
     :steps (conj steps
                  {:condition (ok-cond (dec commit-i)) :stmt (stmt "COMMIT")}
                  {:condition {:type "not" :cond (ok-cond commit-i)} :stmt (stmt "ROLLBACK")})}))

(defn parse-read
  "A list value comes back as one comma-joined TEXT cell, or no row at all."
  [step-result]
  (let [cell (-> step-result :rows first first)]
    (if (or (nil? cell) (= "null" (:type cell)))
      []
      (mapv #(Long/parseLong %) (str/split (:value cell) #",")))))

(defn complete-txn
  [txn step-results]
  (mapv (fn [[f k v :as mop] res]
          (if (= :r f) [f k (parse-read res)] mop))
        txn
        (drop 2 step-results)))

(defn classify
  "Turns a batch result into :ok, :fail or :info. Only claim :fail when we KNOW
  the transaction did not commit: BEGIN itself failed, or the ROLLBACK ran and
  succeeded. Everything else (COMMIT errored and ROLLBACK also errored, which is
  what a COMMIT that committed locally but then reported an error looks like) is
  :info, because Jepsen treats a wrong :fail as a lie and a wrong :info as just
  less information."
  [{:keys [commit-step rollback-step]} {:keys [step_results step_errors]}]
  (let [ok? (fn [i] (some? (nth step_results i nil)))
        err (fn [i] (nth step_errors i nil))]
    (cond
      (ok? commit-step)                       [:ok nil]
      (err 0)                                 [:fail (:message (err 0))]
      (ok? rollback-step)                     [:fail (or (some :message step_errors) :aborted)]
      :else                                   [:info (or (some :message step_errors) :unknown)])))

(defn pipeline!
  [test via node shard batch]
  (http/post (str (base-url test via node) "/v2/pipeline")
             {:headers            {"Host" (str shard "." domain)}
              :content-type       :json
              :body               (json/generate-string
                                    {:requests [{:type "batch" :batch {:steps (:steps batch)}}
                                                {:type "close"}]})
              :socket-timeout     (:request-timeout-ms test)
              :connection-timeout 2000
              :throw-exceptions   false}))

(defn shard-name [test k] (str (:shard-prefix test) "-" k))

(defn track-keys!
  "Remembers every key a shard's transactions touched, for the final reads."
  [test k txn]
  (when-let [tracker (:key-tracker test)]
    (swap! tracker update k (fnil into (sorted-set)) (map second txn))))

(defrecord AppendClient [node via]
  client/Client
  (open! [this test node] (assoc this :node node :via (via-for test)))
  (setup! [this test])
  (invoke! [this test op]
    (let [[k txn] (:value op)
          shard   (shard-name test k)
          batch   (txn->batch txn)]
      (track-keys! test k txn)
      (try+
        (let [resp (pipeline! test via node shard batch)
              body (try (json/parse-string (:body resp) true) (catch Exception _ nil))
              ;; Who served it: the client's own node when direct, nginx's upstream address
              ;; (deploy/chaos/nginx.conf) through the LB. Recorded on every completion.
              op   (assoc op :via via :served-by (if (= :direct via)
                                                    node
                                                    (get-in resp [:headers "x-fathom-upstream"])))]
          (if (= 200 (:status resp))
            (let [r (first (:results body))]
              (if (= "ok" (:type r))
                (let [br (-> r :response :result)
                      [t err] (classify batch br)]
                  (case t
                    :ok (assoc op :type :ok
                               :value (independent/tuple k (complete-txn txn (:step_results br))))
                    (assoc op :type t :error err)))
                ;; The pipeline answered but the batch request itself was refused.
                (assoc op :type :info :error [:request-error (-> r :error :message)])))
            ;; Non-200: a stream refusal, an nginx 502/504, a node that died
            ;; mid-request. We cannot tell which happened before or after the
            ;; commit, so none of them are a safe :fail.
            (assoc op :type :info :error [:http (:status resp) (or (:message body) (:body resp))])))
        (catch java.net.ConnectException e
          ;; Nothing was sent, so nothing can have committed.
          (assoc op :type :fail :error [:connect-refused (.getMessage e)]))
        (catch java.net.SocketTimeoutException e
          (assoc op :type :info :error [:timeout (.getMessage e)]))
        (catch java.io.IOException e
          (assoc op :type :info :error [:io (.getMessage e)])))))
  (teardown! [this test])
  (close! [this test]))

(defn client [] (->AppendClient nil nil))
