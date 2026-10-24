(defproject jepsen.fathom "0.1.0-SNAPSHOT"
  :description "Jepsen tests for fathom, run against the deploy/chaos docker rig"
  :license {:name "Apache-2.0"}
  :main jepsen.fathom
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.15"]
                 [clj-http "3.13.1"]
                 [cheshire "6.2.0"]]
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.fathom})
