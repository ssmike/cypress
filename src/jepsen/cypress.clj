(ns jepsen.cypress
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [clojure.data.json  :as json]
            [jepsen [db         :as db]
                    [cli        :as cli]
                    [checker    :as checker]
                    [client     :as client]
                    [control    :as c]
                    [net        :as net]
                    [generator  :as gen]
                    [nemesis    :as nemesis]
                    [tests      :as tests]
                    [util       :refer [timeout]]]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]
            [jepsen.yt :as yt]
            [org.httpkit.client :as http]))

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
          (yt/ysend sock {:f :wait-for-yt})
          (info "yt proxy set up")
          (client sock)))
    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
        (merge op (yt/ysend con op))))
    (teardown! [_ test] (yt/close con))))

(defn r-gen   [_ _] {:type :invoke, :f :read, :value nil})
(defn w-gen   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn two-way-drop
  [test src dst]
  (do
    (future (net/drop! (:net test) test src dst))
    (future (net/drop! (:net test) test dst src))))

(defn silence!
  [test src nodes]
  (doseq [dst nodes]
    (future (net/drop! (:net test) test src dst))))

(defn partition-master-nodes
  [nodes]
  (let [monitor-port 20000
        statuses (atom (into {} (for [node nodes]
                                  [node false])))
        poller (delay (future ; we aren't going to start polling right now
                (doseq [node (cycle (cons nil nodes))]
                   (if (nil? node)
                     (Thread/sleep 1300)
                     (do
                       ; guilty until proven innocent
                       (swap! statuses assoc node false)
                       (http/get (str "http://" (name node) ":" monitor-port "/orchid/monitoring/hydra")
                                 {:timeout 1000}
                                 (fn [{:keys [status body error]}]
                                   (cond
                                     error (debug (str "exception thrown while connecting to " node))
                                     (not= status 200) (debug (str node " responded with " status))
                                     (-> body json/read-json :active_leader) (swap! statuses assoc node true)
                                     ; do nothing
                                     :else ()))))))))]

    (reify client/Client
      (setup! [this test node]
        @poller
        this)
      (invoke! [this test op]
        (case (:f op)
          :start
          (let [to-split (->> nodes
                              (filter @statuses)
                              (into #{}))]
            (doseq [node to-split]
              (silence! test node nodes))
            (assoc op :value (str "Cut off " to-split)))
          :stop
          (do (net/heal! (:net test) test)
              (assoc op :value "fully connected"))))
      (teardown! [_ test]
        (future-cancel @poller)))))

(defn c-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info "Creating test" opts)
  (let [pre-test (merge tests/noop-test opts)
        timeout (:time-limit pre-test)]
     (merge pre-test
            {:name     "Cypress"
             :os      debian/os
             :db      db
             :client  (client nil)
             :nemesis (partition-master-nodes (:nodes pre-test))
             :timeout timeout
             :generator (->> (gen/mix [r-gen w-gen])
                             (gen/stagger 0.2)
                             (gen/nemesis
                               (gen/seq (cycle [(gen/sleep 5)
                                                {:type :info, :f :start}
                                                (gen/sleep 5)
                                                {:type :info, :f :stop}])))
                             (gen/time-limit timeout))
             :model   (model/register 0)
             :checker (checker/compose
                        {:perf   (checker/perf)
                         :linear (checker/linearizable :wgl)})
             :ssh {:username "root",
                   :strict-host-key-checking false,
                   :private-key-path "~/.ssh/yt"}})))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn c-test})
                   (cli/serve-cmd))
           args))
