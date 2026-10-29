(ns jepsen.fathom.nemesis-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.fathom :as fathom]
            [jepsen.fathom.nemesis :as fnemesis]))

(deftest power-loss-is-opt-in
  ;; `--faults all` must still run on a rig without LazyFS, so :power-loss is
  ;; only ever asked for by name.
  (is (not (contains? (fathom/parse-faults "all") :power-loss)))
  (is (contains? (fathom/parse-faults "all,power-loss") :power-loss))
  (is (= (conj fathom/all-faults :power-loss) (fathom/parse-faults "all,power-loss")))
  (is (= #{:power-loss :kill} (fathom/parse-faults "power-loss,kill")))
  (is (= #{} (fathom/parse-faults "none"))))

(def ^:private pick-fault #'fnemesis/pick-fault)

(defn- test-map [faults]
  {:faults faults :nodes ["fathom1" "fathom2" "fathom3" "fathom4" "fathom5"]
   :max-concurrent-faults 2})

(deftest one-power-loss-at-a-time
  ;; Followers ack from RAM, so two nodes losing power together can lose an
  ;; acked write by design. The nemesis must never stack a second one.
  (testing "a second :power-loss is never picked while one is active"
    (dotimes [_ 200]
      (is (nil? (pick-fault (test-map #{:power-loss}) [[:power-loss ["fathom1"]]])))))
  (testing "another node fault can still join it, on a different node"
    (dotimes [_ 200]
      (let [[f [n]] (pick-fault (test-map #{:power-loss :kill}) [[:power-loss ["fathom1"]]])]
        (is (= :kill f))
        (is (not= "fathom1" n)))))
  (testing "alone, it is picked"
    (is (= :power-loss (first (pick-fault (test-map #{:power-loss}) []))))))

(deftest kill-all-takes-every-node-on-a-quiet-cluster
  ;; A coordinated crash is only one if every node goes down together, and a
  ;; solo fault never stacks on another.
  (testing "it targets all five nodes"
    (is (= [:kill-all ["fathom1" "fathom2" "fathom3" "fathom4" "fathom5"]]
           (pick-fault (test-map #{:kill-all}) []))))
  (testing "it never joins an active fault"
    (dotimes [_ 200]
      (is (nil? (pick-fault (test-map #{:kill-all}) [[:pause ["fathom1"]]])))))
  (testing "it is in `all`"
    (is (contains? (fathom/parse-faults "all") :kill-all))))
