(ns jepsen.fathom.liveness
  "Every shard must take a write again after the faults are healed.

  Elle judges only what completed. A shard that never commits again after the
  heal leaves Elle nothing to judge, and the run used to come back :unknown
  (the 2026-10-28 S3 run: 6 of 16 shards had no ok transaction at all) rather
  than failing. fathom claims a healed fleet recovers every shard, so a shard
  with no ok append after the heal is a failure.

  The heal is the :stop that ends the faulty phase: the second-to-last nemesis
  :stop, since the last one is the extra heal before the final reads, which
  are read-only. With no faults there is one :stop and the window is the whole
  run."
  (:require [jepsen.checker :as checker]))

(defn- write? [op]
  (some #(= :append (first %)) (second (:value op))))

(defn heal-time
  "When the faulty phase ended: the second-to-last nemesis :stop, or the first
  op's time when the run had no faulty phase."
  [history]
  (let [stops (->> history
                   (filter #(and (= :nemesis (:process %)) (= :stop (:f %)) (= :info (:type %))
                                 (some? (:value %))))
                   (map :time)
                   vec)]
    (if (>= (count stops) 2)
      (nth stops (- (count stops) 2))
      (or (:time (first history)) 0))))

(defn recovery
  "Seconds from the heal to each shard's first ok write after it, keyed by
  shard; nil for a shard that never wrote again."
  [shards history]
  (let [t0    (heal-time history)
        firsts (->> history
                   (filter #(and (= :ok (:type %)) (number? (:process %)) (>= (:time %) t0) (write? %)))
                   (reduce (fn [m op]
                             (let [k (first (:value op))]
                               (if (contains? m k) m (assoc m k (:time op)))))
                           {}))]
    (into (sorted-map)
          (for [k shards]
            [k (when-let [t (get firsts k)] (/ (Math/round (/ (- t t0) 1e7)) 100.0))]))))

(defn checker
  "Fails when any shard has no ok write after the heal, or when one only wrote
  again after `deadline-s` seconds (nil: no deadline)."
  [deadline-s]
  (reify checker/Checker
    (check [_ test history _opts]
      (let [rec     (recovery (range (:shards test)) history)
            never   (vec (keep (fn [[k s]] (when (nil? s) k)) rec))
            late    (if deadline-s
                      (vec (keep (fn [[k s]] (when (and s (> s deadline-s)) k)) rec))
                      [])
            times   (keep val rec)]
        {:valid?           (and (empty? never) (empty? late))
         :never-recovered  never
         :late             late
         :deadline-s       deadline-s
         :max-recovery-s   (when (seq times) (apply max times))
         :recovery-s       rec}))))
