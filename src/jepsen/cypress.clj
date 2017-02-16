(ns jepsen.cypress
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [jepsen [db         :as db]
                    [cli        :as cli]
                    [checker    :as checker]
                    [client     :as client]
                    [control    :as c]
                    [generator  :as gen]
                    [nemesis    :as nemesis]
                    [tests      :as tests]
                    [util       :refer [timeout]]]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]
            [jepsen.yt :as yt]))

(def db
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (c/exec :bash "/master/run.sh")
        (info (str node " set up"))))
    (teardown! [_ test node]
      (c/su
        (c/exec :bash "/master/stop.sh")
        (info (str node " teardown!"))))
    db/LogFiles
      (log-files [_ test node]
        ["/master/master.debug.log" "/master/master.log"])))


(defn client
  [con]
  (reify client/Client
    (setup! [this test node]
        (info "waiting for yt")
        (let [sock (yt/start-client)]
          (info "waiting for yt")
          (ysend sock {:f :wait-for-yt})
          (info "yt proxy set up")
          (client sock)))
    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
        (merge op (yt/ysend con op))))
    (teardown! [_ test] (yt/close con))))

(defn r-gen   [_ _] {:type :invoke, :f :read, :value nil})
(defn w-gen   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})


(defn c-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info "Creating test" opts)
  (merge tests/noop-test
         opts
         {:name     "Cypress"
          :os      debian/os
          :db      db
          :client  (client nil)
          :nemesis (nemesis/partition-random-halves)
          :timeout 200
          :generator (->> (gen/mix [r-gen w-gen])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 9)
                                             {:type :info, :f :start}
                                             (gen/sleep 9)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 190))
          :model   (model/register 0)
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear checker/linearizable})
          :ssh {:username "root",
                :strict-host-key-checking false,
                :private-key-path "~/.ssh/yt"}}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn c-test})
                   (cli/serve-cmd))
           args))
