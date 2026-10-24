(ns jepsen.fathom.client-test
  "The pure halves of the client. classify is the one that matters: a wrong
  :fail makes Jepsen report anomalies that never happened, or hide real ones."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.fathom.client :as c]))

(def txn [[:r 1 nil] [:append 1 7] [:append 2 8]])

(deftest batch-layout
  (let [b (c/txn->batch txn)]
    (testing "BEGIN, CREATE, 3 mops, COMMIT, ROLLBACK"
      (is (= 7 (count (:steps b))))
      (is (= 5 (:commit-step b)))
      (is (= 6 (:rollback-step b))))
    (testing "every step after BEGIN needs the one before it, so a failed mop skips COMMIT"
      (is (= (for [i (range 0 5)] {:type "ok" :step i})
             (map :condition (subvec (:steps b) 1 6)))))
    (testing "ROLLBACK runs only when COMMIT did not succeed"
      (is (= {:type "not" :cond {:type "ok" :step 5}}
             (:condition (last (:steps b))))))
    (testing "values are bound, never interpolated"
      (is (= [{:type "integer" :value "1"} {:type "integer" :value "7"}]
             (get-in b [:steps 3 :stmt :args]))))))

(deftest read-parsing
  (is (= [] (c/parse-read {:rows []})))
  (is (= [1 2 3] (c/parse-read {:rows [[{:type "text" :value "1,2,3"}]]})))
  (is (= [[:r 1 [4 5]] [:append 1 7] [:append 2 8]]
         (c/complete-txn txn [{} {} {:rows [[{:type "text" :value "4,5"}]]} {} {}]))))

(defn- result [n ok-steps errs]
  {:step_results (mapv #(when (ok-steps %) {:rows []}) (range n))
   :step_errors  (mapv #(when-let [m (errs %)] {:message m}) (range n))})

(deftest classification
  (let [b (c/txn->batch txn)]
    (testing "COMMIT ran: ok"
      (is (= :ok (first (c/classify b (result 7 #{0 1 2 3 4 5} {}))))))
    (testing "BEGIN refused: nothing ran, a safe fail"
      (is (= :fail (first (c/classify b (result 7 #{} {0 "lease held"}))))))
    (testing "a mop failed and the ROLLBACK succeeded: a safe fail"
      (is (= :fail (first (c/classify b (result 7 #{0 1 2 6} {3 "boom"}))))))
    (testing "COMMIT errored and ROLLBACK errored too: maybe committed, so info"
      (is (= :info (first (c/classify b (result 7 #{0 1 2 3 4}
                                                {5 "quorum lost" 6 "no transaction is active"}))))))))
