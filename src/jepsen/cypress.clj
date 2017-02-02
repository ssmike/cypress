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
            [clojure.java.shell :only [sh]]))

(defn parse-int [s] (Integer. (re-find  #"^\d+$" s )))

(defn c-get
  [name]
  (let [res (sh "yt2" "get" (str "//tmp/" name))]
        (if (not= (:exit res) 0) {:type :info, :error :aborted}
            (try
              (parse-int (:out res))
              (catch Exception e
                  {:type :info, :error :aborted})))

(defn c-set
  [name val]
  (let [res (sh "yt2" "set" (str "//tmp/" name) (str val))]
        (if (not= res {:exit 0 :out "" :err ""})
            {:type :info, :error :aborted}
            {:type :ok}))

(defn wait-and-set
  [name val]
  (while (not= :ok (:type (c-set name val)))
    (Thread/sleep 5000)))

(def client
  [name value]
  (reify client/Client
    (setup! [this test node] (do ((wait-and-set atom value)
                                  (info "Cypress set up")
                                  this))
    (invoke! [this test op])
      (timeout 5000 (assoc op :type :info, :error :timeout)
        (case (:f op)
          :read  (merge op (c-get name))
          :write (merge op (c-set name (:value op)))))))

(defn r-gen   [_ _] {:type :invoke, :f :read, :value nil})
(defn w-gen   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(def init-val 0)

(defn zk-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info "Creating test" opts)
  (merge tests/noop-test
         opts
         {:name     "Cypress"
          :os      debian/os
          :db      db/noop
          :client  (client "atom" init-val)
          :nemesis (nemesis/partition-random-halves)
          :generator (->> (gen/mix [r-gen w-gen])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 15))
          :model   (model/register init-val)
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear checker/linearizable})}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn zk-test})
                   (cli/serve-cmd))
           args))
