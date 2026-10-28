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

(def all-faults (apply disj (into fnemesis/node-faults fnemesis/solo-faults) fnemesis/lazyfs-faults))
;; Faults that need the LazyFS overlay. Not in "all", so `--faults all` still
;; runs on a plain rig; ask for them by name (`--faults all,power-loss`).
(def known-faults (into all-faults fnemesis/lazyfs-faults))
(def default-faults #{:kill :pause :isolate :s3-cut})

(defrecord FinalReads [tracker]
  ;; Reads back every key each shard ever touched, after the faults are healed,
  ;; the way Bufstream's final-read phase rewound consumers to offset 0: a lost
  ;; append on a key the workload had already retired is otherwise invisible.
  ;; Built when first asked, so it sees the whole run's keys.
  gen/Generator
  (op [_ test ctx]
    (gen/op (for [[k ks] (sort @tracker)
                  chunk  (partition-all 8 ks)]
              {:f :txn :value (independent/tuple k (mapv (fn [key] [:r key nil]) chunk))})
            test ctx))
  (update [this _ _ _] this))

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
        faults   (fnemesis/generator (:nemesis-interval opts))
        faults   (if (:hard opts) (fnemesis/hard-generator) faults)
        nem-gen  (if (seq (:faults opts))
                   (gen/phases (gen/time-limit faulty faults)
                               {:type :info :f :stop})
                   nil)
        tracker  (atom {})]
    (merge tests/noop-test
           opts
           {:name         (str "fathom-" (name (:target opts)) "-"
                               (cond
                                 (:hard opts)         (if (:power-loss (:faults opts)) "hard-power" "hard")
                                 (seq (:faults opts)) (str/join "+" (map name (sort (:faults opts))))
                                 :else                "none"))
            ;; The rig's fixed fleet; the client-thread count must be a
            ;; multiple of threads-per-shard for concurrent-generator.
            :nodes        ["fathom1" "fathom2" "fathom3" "fathom4" "fathom5"]
            :concurrency  (* (:shards opts) (:threads-per-shard opts))
            :os           os/noop
            :db           db/noop
            :ssh          {:dummy? true}
            :shard-prefix (str "jep" (quot (System/currentTimeMillis) 1000))
            :key-tracker  tracker
            :client       (fclient/client)
            :nemesis      (fnemesis/nemesis)
            :checker      (checker/compose
                            {:append     (:checker w)
                             :stats      (checker/stats)
                             :exceptions (checker/unhandled-exceptions)})
            :generator    (gen/phases
                            (gen/time-limit
                              (:time-limit opts)
                              (gen/nemesis nem-gen
                                           (->> (:generator w)
                                                (gen/stagger (/ (:rate opts))))))
                            ;; Heal anything still active, then read everything back.
                            (gen/nemesis {:type :info :f :stop})
                            (gen/clients (->FinalReads tracker)))})))

(defn parse-faults [s]
  (if (#{"" "none"} s)
    #{}
    (set (mapcat #(if (= "all" %) all-faults [(keyword %)]) (str/split s #",")))))

(def cli-opts
  [[nil "--target TARGET" ":lb (through nginx, like a real client) or :direct (random node, forces lease contention)"
    :default :lb
    :parse-fn keyword
    :validate [#{:lb :direct :mixed} "must be lb, direct or mixed"]]
   [nil "--faults LIST" (str "Comma list of " (str/join "," (sort (map name known-faults))) ", all, or none; all excludes power-loss")
    :default default-faults
    :parse-fn parse-faults
    :validate [#(every? known-faults %) "unknown fault"]]
   [nil "--hard" "Shifting fault combinations (up to --max-concurrent-faults at once, 3-30 s each) instead of one fault per interval"
    :default false]
   [nil "--max-concurrent-faults N" "With --hard: faults allowed active at once"
    :default 2
    :parse-fn parse-long]
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
