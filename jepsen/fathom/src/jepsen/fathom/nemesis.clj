(ns jepsen.fathom.nemesis
  "Faults for the deploy/chaos rig, injected from the host with the docker CLI
  and the toxiproxy API rather than over SSH. Same mechanics as chaos.sh:

    :kill       network-disconnect then SIGKILL, so peers see a hang, not a RST
    :pause      SIGSTOP the whole container: the zombie-owner case
    :isolate    disconnect from the compose network: no LB, peers, S3 or Postgres
    :s3-cut     disable the node's toxiproxy S3 proxy: lease renewals stop

  Each fault hits ONE random node at a time, so four of five nodes are always up
  and a shard's quorum (2 of 4 followers) stays reachable."
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [clj-http.client :as http]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]))

(def docker (or (System/getenv "DOCKER") "docker"))
(def network "fathom-chaos_default")
(defn cname [node] (str "fathom-chaos-" node "-1"))

(defn docker!
  "Runs docker; returns trimmed output. A failure is logged and returned, not
  thrown: healing a node that is already healed is the common case."
  [& args]
  (let [{:keys [exit out err]} (apply sh docker args)]
    (when-not (zero? exit)
      (warn "docker" (str/join " " args) "exited" exit (str/trim err)))
    (str/trim (str out err))))

(defn toxi! [test node enabled?]
  (http/post (str (:toxi-url test) "/proxies/s3-" node)
             {:body (str "{\"enabled\": " enabled? "}")
              :throw-exceptions false}))

(defn disconnect! [node] (docker! "network" "disconnect" network (cname node)))
;; --alias: without it the reconnected container has no DNS name and peers
;; dialling "<node>" get nxdomain (chaos.sh revive, 2026-10-13).
(defn connect! [node] (docker! "network" "connect" "--alias" node network (cname node)))

(defn start-fault! [test f node]
  (case f
    :kill    (do (disconnect! node) (docker! "kill" (cname node)))
    :pause   (docker! "pause" (cname node))
    :isolate (disconnect! node)
    :s3-cut  (toxi! test node false)))

(defn stop-fault! [test f node]
  (case f
    ;; `docker start` keeps the env the container was created with, so a
    ;; revived node cannot come back with replication silently off (the trap
    ;; chaos.sh's rpo arm documents for `compose up`).
    :kill    (do (connect! node) (docker! "start" (cname node)))
    :pause   (docker! "unpause" (cname node))
    :isolate (connect! node)
    :s3-cut  (toxi! test node true)))

(defrecord RigNemesis [active]
  nemesis/Nemesis
  (setup! [this test] this)
  (invoke! [this test op]
    (case (:f op)
      :start (if-let [[f node] @active]
               (assoc op :type :info :value [:already f node])
               (let [f    (rand-nth (vec (:faults test)))
                     node (rand-nth (:nodes test))]
                 (info "starting" f "on" node)
                 (start-fault! test f node)
                 (reset! active [f node])
                 (assoc op :type :info :value [f node])))
      :stop  (if-let [[f node] @active]
               (do (info "stopping" f "on" node)
                   (stop-fault! test f node)
                   (reset! active nil)
                   (assoc op :type :info :value [f node]))
               (assoc op :type :info :value :nothing-active))))
  (teardown! [this test]
    (when-let [[f node] @active] (stop-fault! test f node)))
  nemesis/Reflection
  (fs [this] #{:start :stop}))

(defn nemesis [] (->RigNemesis (atom nil)))

(defn generator
  "Fault on for `interval` s, off for `interval` s, forever. Faults last at
  least past the lease TTL + steal margin (10 s + 5 s) by default, so a steal
  can actually happen while the old owner is down or frozen."
  [interval]
  (cycle [(gen/sleep interval) {:type :info :f :start}
          (gen/sleep interval) {:type :info :f :stop}]))
