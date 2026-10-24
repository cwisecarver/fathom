(ns jepsen.fathom
  "Jepsen test for fathom against the deploy/chaos rig.

  The rig is brought up by run.sh (chaos.sh up), not by Jepsen: there is no SSH,
  the DB and OS are no-ops, and faults go through the docker CLI. The workload
  is Elle list-append, one independent history per shard. Every transaction
  stays inside one shard, which is all fathom promises: one SQLite file, one
  writer, so each shard's history should be strict-serializable."
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.os :as os]
            [jepsen.tests :as tests]
            [jepsen.tests.cycle.append :as append]
            [elle.list-append :as la]
            [jepsen.fathom.client :as fclient]
            [jepsen.fathom.nemesis :as fnemesis]))

(def all-faults #{:kill :pause :isolate :s3-cut})

(defn workload
  [opts]
  {:generator (independent/concurrent-generator
                (:threads-per-shard opts)
                (range (:shards opts))
                (fn [_k]
                  (la/gen {:key-count          4
                           :min-txn-length     1
                           :max-txn-length     4
                           :max-writes-per-key 64})))
   :checker   (independent/checker
                (append/checker {:consistency-models [(:consistency-model opts)]
                                 :anomalies          [:G1 :G2 :lost-update]}))})

(defn fathom-test
  [opts]
  (let [w        (workload opts)
        quiet    (:quiet-period opts)
        faulty   (max 0 (- (:time-limit opts) quiet))
        nem-gen  (if (seq (:faults opts))
                   (gen/phases (gen/time-limit faulty (fnemesis/generator (:nemesis-interval opts)))
                               {:type :info :f :stop})
                   nil)]
    (merge tests/noop-test
           opts
           {:name         (str "fathom-" (name (:target opts)) "-"
                               (if (seq (:faults opts))
                                 (str/join "+" (map name (sort (:faults opts))))
                                 "none"))
            ;; The rig's fixed fleet; the client-thread count must be a
            ;; multiple of threads-per-shard for concurrent-generator.
            :nodes        ["fathom1" "fathom2" "fathom3" "fathom4" "fathom5"]
            :concurrency  (* (:shards opts) (:threads-per-shard opts))
            :os           os/noop
            :db           db/noop
            :ssh          {:dummy? true}
            :shard-prefix (str "jep" (quot (System/currentTimeMillis) 1000))
            :client       (fclient/client)
            :nemesis      (fnemesis/nemesis)
            :checker      (checker/compose
                            {:append     (:checker w)
                             :stats      (checker/stats)
                             :exceptions (checker/unhandled-exceptions)})
            :generator    (gen/time-limit
                            (:time-limit opts)
                            (gen/nemesis nem-gen
                                         (->> (:generator w)
                                              (gen/stagger (/ (:rate opts))))))})))

(defn parse-faults [s]
  (if (#{"" "none"} s)
    #{}
    (set (map keyword (str/split s #",")))))

(def cli-opts
  [[nil "--target TARGET" ":lb (through nginx, like a real client) or :direct (random node, forces lease contention)"
    :default :lb
    :parse-fn keyword
    :validate [#{:lb :direct} "must be lb or direct"]]
   [nil "--faults LIST" "Comma list of kill,pause,isolate,s3-cut, or none"
    :default all-faults
    :parse-fn parse-faults
    :validate [#(every? all-faults %) "unknown fault"]]
   [nil "--nemesis-interval SECS" "Seconds each fault lasts, and the gap between faults"
    :default 20
    :parse-fn parse-long]
   [nil "--quiet-period SECS" "Fault-free seconds at the end, so reads can see what survived"
    :default 30
    :parse-fn parse-long]
   [nil "--shards N" "Shards under test at once"
    :default 8
    :parse-fn parse-long]
   [nil "--threads-per-shard N" "Client threads per shard"
    :default 2
    :parse-fn parse-long]
   [nil "--rate HZ" "Target ops/sec across all clients"
    :default 50
    :parse-fn read-string]
   [nil "--consistency-model MODEL" "What Elle checks each shard against"
    :default :strict-serializable
    :parse-fn keyword]
   [nil "--request-timeout-ms MS" "Per-request HTTP timeout"
    :default 10000
    :parse-fn parse-long]
   [nil "--lb-port PORT" "The rig LB's host port (chaos.sh LB_PORT)"
    :default 8080
    :parse-fn parse-long]
   [nil "--toxi-url URL" "toxiproxy control API"
    :default "http://localhost:8474"]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  fathom-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
