(ns jepsen.fathom.nemesis
  "Faults for the deploy/chaos rig, injected from the host with the docker CLI
  and the toxiproxy API rather than over SSH. Same mechanics as chaos.sh:

    :kill            network-disconnect then SIGKILL, so peers see a hang, not a RST
    :pause           SIGSTOP the whole container: the zombie-owner case
    :isolate         disconnect from the compose network: no LB, peers, S3 or Postgres
    :s3-cut          disable the node's toxiproxy S3 proxy: connections refused
    :s3-blackhole    the node's S3 proxy swallows traffic: requests hang, no RST
    :s3-slow         1-3 s of latency (with jitter) on the node's S3 path
    :repl-cut        disable the proxy in front of the node's follower port, so no
                     primary can ship to it and no recovering peer can ask it
    :repl-blackhole  the same link swallows traffic instead of refusing it
    :kill-majority   kill 3 of 5 nodes at once: no follower quorum is reachable
    :minio-pause     SIGSTOP the object store itself: every node's S3 hangs at once
    :postgres-pause  SIGSTOP the control-plane database
    :flap            cut and restore the node's network every 1-3 s until healed
    :s3-reset        the node's S3 connections are reset (RST) shortly after opening
    :s3-truncate     the node's S3 connections close after ~4-64 KB: torn replies and
                     uploads
    :repl-slow       200-1000 ms of latency on the link into the node's follower port
    :rolling-restart kill and revive every node in turn, like a bad deploy
    :lb-restart      restart nginx
    :power-loss      SIGKILL the container with LazyFS under /data, so every write
                     the node had not fsynced is gone, as after a power failure.
                     Needs the LAZYFS=1 overlay (deploy/chaos/docker-compose.lazyfs.yml).
                     At most one node is power-lost at a time: followers ack from
                     RAM by design (:replication_fsync off), and durability comes
                     from the replica count, so a simultaneous power loss of a
                     shard's quorum is outside what fathom claims to survive.
    :clock-back      step the VM clock 20-60 s BACKWARD, then forward to the host's
    :clock-jump      step the VM clock 20-60 s forward, then back to the host's.
                     Every container shares one kernel clock, so this jumps ALL
                     nodes and MinIO together; per-node skew is not possible on
                     this rig (no CAP_SYS_TIME in the containers)

  On a LazyFS rig, :kill (and the kills inside :kill-majority and
  :rolling-restart) first writes LazyFS's cache through, so it stays a process
  crash that keeps the page cache. Writes in the moment between that flush and
  the SIGKILL are lost, which is harsher than a real crash, never gentler.

  The repl-* faults need the rig brought up with REPL_VIA_TOXI=1, or replication
  bypasses toxiproxy and they are inert.

  Node faults hit distinct nodes, so two concurrent faults never fight over one
  container. :kill-majority, :minio-pause and :postgres-pause run alone.
  Faults lead with what Jepsen's Datomic and Bufstream analyses used: process
  pauses and crashes, partitions between nodes and from storage, and storage
  pauses, mixed in shifting combinations."
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [clj-http.client :as http]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]))

(def docker (or (System/getenv "DOCKER") "docker"))
(def network "fathom-chaos_default")
(defn cname [node] (str "fathom-chaos-" node "-1"))

(def node-faults #{:kill :power-loss :pause :isolate :s3-cut :s3-blackhole :s3-slow :repl-cut :repl-blackhole
                   :flap :s3-reset :s3-truncate :repl-slow})
(def lazyfs-faults #{:power-loss})
(def solo-faults #{:kill-majority :minio-pause :postgres-pause :clock-jump :clock-back
                   :rolling-restart :lb-restart})

;; Background fault loops (:flap, :rolling-restart), keyed by [fault node].
(defonce ^:private loops (atom {}))

(defn- start-loop! [k f]
  (let [run? (atom true)]
    (swap! loops assoc k {:run? run? :fut (future (try (f run?) (catch Throwable t (warn t "fault loop" k))))})))

(defn- stop-loop! [k]
  (when-let [{:keys [run? fut]} (get @loops k)]
    (reset! run? false)
    (deref fut 120000 nil)
    (swap! loops dissoc k)))

(defn docker!
  "Runs docker; returns trimmed output. A failure is logged and returned, not
  thrown: healing a node that is already healed is the common case."
  [& args]
  (let [{:keys [exit out err]} (apply sh docker args)]
    (when-not (zero? exit)
      (warn "docker" (str/join " " args) "exited" exit (str/trim err)))
    (str/trim (str out err))))

(defn- vm! [& cmd]
  (let [{:keys [exit err]} (apply sh "colima" "ssh" "--" "sudo" cmd)]
    (when-not (zero? exit) (warn "colima" (str/join " " cmd) "failed:" (str/trim err)))))

(defn set-vm-clock!
  "Sets the colima VM's clock to `epoch-s` (seconds since the epoch). With
  `hold?`, stops systemd-timesyncd first: it steps a jumped clock back within a
  second, which made the first clock-jump run (2026-10-24) jump almost nothing.
  lima's guest agent still resyncs about once a minute, so a long jump can also
  be stepped back mid-fault, which is a backward jump and fair game."
  ([epoch-s] (set-vm-clock! epoch-s false))
  ([epoch-s hold?]
   (when hold? (vm! "systemctl" "stop" "systemd-timesyncd"))
   (vm! "date" "-s" (str "@" epoch-s))
   (when-not hold? (vm! "systemctl" "start" "systemd-timesyncd"))))

(defn- now-s [] (quot (System/currentTimeMillis) 1000))

(defn- toxi-post! [test path body]
  (http/post (str (:toxi-url test) path)
             {:body body :content-type :json :throw-exceptions false}))

(defn- toxi-delete! [test path]
  (http/delete (str (:toxi-url test) path) {:throw-exceptions false}))

(defn proxy-enabled! [test proxy enabled?]
  (toxi-post! test (str "/proxies/" proxy) (str "{\"enabled\": " enabled? "}")))

(defn add-toxic!
  "One toxic per direction, named jep-up / jep-down, so a heal is two DELETEs."
  [test proxy type attributes]
  (doseq [stream ["upstream" "downstream"]]
    (toxi-post! test (str "/proxies/" proxy "/toxics")
                (str "{\"name\":\"jep-" (subs stream 0 2) "\",\"type\":\"" type "\","
                     "\"stream\":\"" stream "\",\"attributes\":" attributes "}"))))

(defn clear-toxics! [test proxy]
  (doseq [n ["jep-up" "jep-do"]]
    (toxi-delete! test (str "/proxies/" proxy "/toxics/" n))))

(defn disconnect! [node] (docker! "network" "disconnect" network (cname node)))
;; --alias: without it the reconnected container has no DNS name and peers
;; dialling "<node>" get nxdomain (chaos.sh revive, 2026-10-13).
(defn log-ip!
  "Logs the node's current address. A reconnect can hand a node a new IP, and the
  LB's X-Fathom-Upstream header (recorded as :served-by) names an IP, so this
  log is how an op is traced back to a node."
  [node]
  (info "ip" node (docker! "inspect" "-f"
                           (str "{{(index .NetworkSettings.Networks \"" network "\").IPAddress}}")
                           (cname node))))

(defn connect! [node]
  (docker! "network" "connect" "--alias" node network (cname node))
  (log-ip! node))

;; Set at setup!: true when every node's /data is a LazyFS mount.
(defonce ^:private lazyfs? (atom false))

(defn lazyfs-mounted? [node]
  (let [{:keys [exit]} (sh docker "exec" (cname node) "mountpoint" "-q" "/data")]
    (zero? exit)))

(defn- lazyfs! [node cmd]
  (docker! "exec" (cname node) "sh" "-c" (str "echo " cmd " > /tmp/lazyfs.fifo")))

(defn kill!
  "A process crash. On a LazyFS rig the cache is written through first, since a
  real kernel keeps a dead process's dirty pages."
  [node]
  (disconnect! node)
  (when @lazyfs?
    (lazyfs! node "lazyfs::cache-checkpoint")
    (Thread/sleep 500))
  (docker! "kill" (cname node)))

(defn power-off!
  "A power failure: SIGKILL takes LazyFS down with the node, and its cache (every
  un-fsynced write) with it. Then a fresh boot id, as a real power cycle gives the
  node (the overlay points REPLICATION_BOOT_ID_PATH at this file)."
  [node]
  (disconnect! node)
  (docker! "kill" (cname node))
  (let [f (java.io.File/createTempFile "boot_id" "")]
    (try
      (spit f (str (java.util.UUID/randomUUID) "\n"))
      ;; docker cp keeps the mode, and a temp file is 0600: fathom runs as `app`
      ;; and would read no boot id at all, which silently turns the check off.
      (.setReadable f true false)
      (docker! "cp" (.getPath f) (str (cname node) ":/var/lib/fathom/boot_id"))
      (finally (.delete f)))))
;; `docker start` keeps the env the container was created with, so a revived
;; node cannot come back with replication silently off (the trap chaos.sh's rpo
;; arm documents for `compose up`).
(defn revive! [node] (connect! node) (docker! "start" (cname node)))

(defn- flap! [node run?]
  (while @run?
    (disconnect! node)
    (Thread/sleep (long (+ 1000 (rand-int 2000))))
    (connect! node)
    (Thread/sleep (long (+ 1000 (rand-int 2000))))))

(defn- rolling-restart! [nodes run?]
  (doseq [n (shuffle nodes) :while @run?]
    (kill! n)
    (Thread/sleep (long (+ 2000 (rand-int 6000))))
    (revive! n)
    (Thread/sleep (long (+ 3000 (rand-int 5000))))))

(defn start-fault! [test f nodes]
  (let [node (first nodes)]
    (case f
      :kill           (kill! node)
      :power-loss     (power-off! node)
      :pause          (docker! "pause" (cname node))
      :isolate        (disconnect! node)
      :s3-cut         (proxy-enabled! test (str "s3-" node) false)
      :s3-blackhole   (add-toxic! test (str "s3-" node) "timeout" "{\"timeout\":0}")
      :s3-slow        (add-toxic! test (str "s3-" node) "latency"
                                  (str "{\"latency\":" (+ 1000 (rand-int 2000)) ",\"jitter\":500}"))
      :repl-cut       (proxy-enabled! test (str "repl-" node) false)
      :repl-blackhole (add-toxic! test (str "repl-" node) "timeout" "{\"timeout\":0}")
      :kill-majority  (doseq [n nodes] (kill! n))
      :minio-pause    (docker! "pause" "fathom-chaos-minio-1")
      :postgres-pause (docker! "pause" "fathom-chaos-postgres-1")
      :clock-jump     (set-vm-clock! (+ (now-s) 20 (rand-int 41)) true)
      :clock-back     (set-vm-clock! (- (now-s) 20 (rand-int 41)) true)
      :flap           (start-loop! [:flap node] (partial flap! node))
      :s3-reset       (add-toxic! test (str "s3-" node) "reset_peer"
                                  (str "{\"timeout\":" (+ 50 (rand-int 500)) "}"))
      :s3-truncate    (add-toxic! test (str "s3-" node) "limit_data"
                                  (str "{\"bytes\":" (+ 4096 (rand-int 61440)) "}"))
      :repl-slow      (add-toxic! test (str "repl-" node) "latency"
                                  (str "{\"latency\":" (+ 200 (rand-int 800)) ",\"jitter\":100}"))
      :rolling-restart (start-loop! [:rolling nil] (partial rolling-restart! (:nodes test)))
      :lb-restart     (docker! "restart" "fathom-chaos-lb-1"))))

(defn stop-fault! [test f nodes]
  (let [node (first nodes)]
    (case f
      (:kill :power-loss) (revive! node)
      :pause          (docker! "unpause" (cname node))
      :isolate        (connect! node)
      :s3-cut         (proxy-enabled! test (str "s3-" node) true)
      (:s3-blackhole :s3-slow) (clear-toxics! test (str "s3-" node))
      :repl-cut       (proxy-enabled! test (str "repl-" node) true)
      :repl-blackhole (clear-toxics! test (str "repl-" node))
      :kill-majority  (doseq [n nodes] (revive! n))
      :minio-pause    (docker! "unpause" "fathom-chaos-minio-1")
      :postgres-pause (docker! "unpause" "fathom-chaos-postgres-1")
      (:clock-jump :clock-back) (set-vm-clock! (now-s))
      :flap           (do (stop-loop! [:flap node]) (connect! node))
      (:s3-reset :s3-truncate) (clear-toxics! test (str "s3-" node))
      :repl-slow      (clear-toxics! test (str "repl-" node))
      ;; Waits for the round to finish, then makes sure every node is back.
      :rolling-restart (do (stop-loop! [:rolling nil]) (doseq [n (:nodes test)] (revive! n)))
      :lb-restart     nil)))

(defn- pick-fault
  "A fault that may start alongside `active`, or nil when none can. A solo fault
  only starts on a quiet cluster; a node fault takes a node no active fault
  holds, so faults never stack on one container."
  [test active]
  (let [faults (vec (:faults test))
        busy   (set (mapcat second active))
        free   (remove busy (:nodes test))
        solo?  (some (comp solo-faults first) active)
        ;; One power-lost node at a time; see :power-loss in the ns doc.
        faults (if (some (comp #{:power-loss} first) active)
                 (vec (remove #{:power-loss} faults))
                 faults)]
    (when (and (not solo?) (< (count active) (:max-concurrent-faults test)))
      (let [choices (filter #(or (and (node-faults %) (seq free))
                                 (and (solo-faults %) (empty? active)))
                            faults)]
        (when (seq choices)
          (let [f (rand-nth (vec choices))]
            [f (if (= :kill-majority f)
                 (vec (take 3 (shuffle (:nodes test))))
                 (if (node-faults f) [(rand-nth (vec free))] []))]))))))

(defrecord RigNemesis [active]
  nemesis/Nemesis
  (setup! [this test]
    (doseq [n (:nodes test)] (log-ip! n))
    (let [mounted (filter lazyfs-mounted? (:nodes test))]
      (reset! lazyfs? (= (count mounted) (count (:nodes test))))
      (info "lazyfs on" (vec mounted))
      (when (and (:power-loss (:faults test)) (not @lazyfs?))
        (throw (ex-info (str ":power-loss needs LazyFS under /data on every node; mounted on "
                             (vec mounted) ". Bring the rig up with the LAZYFS=1 overlay.")
                        {:mounted mounted}))))
    this)
  (invoke! [this test op]
    (case (:f op)
      :start (if-let [[f nodes :as fault] (pick-fault test @active)]
               (do (info "starting" f "on" nodes)
                   (start-fault! test f nodes)
                   (swap! active conj fault)
                   (assoc op :type :info :value fault))
               (assoc op :type :info :value [:skipped (count @active) :active]))
      ;; Heal in reverse start order.
      :stop  (let [faults @active]
               (doseq [[f nodes] (reverse faults)]
                 (info "stopping" f "on" nodes)
                 (stop-fault! test f nodes))
               (reset! active [])
               (assoc op :type :info :value (vec faults)))))
  (teardown! [this test]
    (doseq [[f nodes] (reverse @active)] (stop-fault! test f nodes))
    (reset! active []))
  nemesis/Reflection
  (fs [this] #{:start :stop}))

(defn nemesis [] (->RigNemesis (atom [])))

(defn generator
  "Fault on for `interval` s, off for `interval` s, forever: one fault at a
  time, each lasting past the lease TTL + steal margin (10 s + 5 s) by default,
  so a steal can actually happen while the old owner is down or frozen."
  [interval]
  (cycle [(gen/sleep interval) {:type :info :f :start}
          (gen/sleep interval) {:type :info :f :stop}]))

(defn hard-generator
  "Shifting combinations, after Bufstream's schedule: start a fault, maybe pile
  a second on 0-10 s later, hold 3-30 s (some heal before the 15 s steal
  threshold, some after), heal everything, rest 0-15 s."
  []
  (repeatedly
    (fn []
      [{:type :info :f :start}
       (gen/sleep (rand-int 10))
       (if (< (rand) 0.6) {:type :info :f :start} [])
       (gen/sleep (+ 3 (rand-int 28)))
       {:type :info :f :stop}
       (gen/sleep (rand-int 16))])))
