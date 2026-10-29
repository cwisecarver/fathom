(ns jepsen.fathom.liveness-test
  "The liveness checker: every shard writes again after the heal. Written after
  the 2026-10-28 pause run, where Elle passed 16 shards while 15 of them took no
  write for the last ~400 s."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.fathom.liveness :as liveness]))

(def ^:private s 1000000000)

(defn- stop [t v] {:type :info :process :nemesis :f :stop :time (* t s) :value v})
(defn- stop-invoke [t] {:type :info :process :nemesis :f :stop :time (* t s) :value nil})
(defn- w [ty t shard] {:type ty :process 1 :f :txn :time (* t s) :value [shard [[:append 1 1]]]})
(defn- r [ty t shard] {:type ty :process 1 :f :txn :time (* t s) :value [shard [[:r 1 [1]]]]})

(defn- check [shards history & [deadline]]
  (checker/check (liveness/checker deadline) {:shards shards} history {}))

(deftest the-heal-is-the-second-to-last-stop
  ;; The last :stop is the extra heal before the read-only final reads.
  (is (= (* 50 s) (liveness/heal-time [(stop-invoke 10) (stop 10 []) (stop-invoke 50) (stop 50 [])
                                       (stop-invoke 60) (stop 60 [])])))
  (testing "no faulty phase: the whole run is the window"
    (is (= (* 3 s) (liveness/heal-time [(w :ok 3 0) (stop-invoke 60) (stop 60 [])])))))

(deftest a-shard-that-never-writes-after-the-heal-fails
  (let [h [(w :ok 1 0) (w :ok 1 1)
           (stop-invoke 50) (stop 50 [[:pause ["fathom1"]]])
           (w :ok 52 0) (w :fail 53 1) (w :info 54 1)
           ;; A final read is not a write: reads alone do not prove recovery.
           (stop-invoke 60) (stop 60 []) (r :ok 61 1)]]
    (is (= {:valid? false :never-recovered [1] :late [] :max-recovery-s 2.0}
           (select-keys (check 2 h) [:valid? :never-recovered :late :max-recovery-s])))))

(deftest every-shard-writing-again-passes
  (let [h [(stop-invoke 50) (stop 50 []) (w :ok 51 0) (w :ok 57 1) (stop-invoke 60) (stop 60 [])]]
    (is (= {:valid? true :never-recovered [] :recovery-s {0 1.0 1 7.0}}
           (select-keys (check 2 h) [:valid? :never-recovered :recovery-s])))))

(deftest a-write-before-the-heal-does-not-count
  (let [h [(w :ok 40 0) (stop-invoke 50) (stop 50 []) (stop-invoke 60) (stop 60 [])]]
    (is (= [0] (:never-recovered (check 1 h))))))

(deftest the-deadline
  (let [h [(stop-invoke 50) (stop 50 []) (w :ok 51 0) (w :ok 80 1) (stop-invoke 90) (stop 90 [])]]
    (is (:valid? (check 2 h)))
    (is (= {:valid? false :late [1]} (select-keys (check 2 h 20) [:valid? :late])))))
