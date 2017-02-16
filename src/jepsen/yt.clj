(ns jepsen.yt
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  [:import java.lang.Runtime])

(defn encode
  [op]
  (let [allowed-keys #{:f :value :req-id}]
    (json/write-str
      (conj {} (filter (fn [[x _]]
                         (contains? allowed-keys x))
                       op)))))

(defn decode
  [msg]
  (let [mp (json/read-str msg)]
    (into {} (map (fn [[a b]] [(keyword a)
                               ((case a
                                  ("value" "req-id")
                                    identity
                                    keyword)
                                 b)])
                  mp))))

(defn ysend
  [{:keys [in cache req-cnt]} msg]
  (let [result (promise)
        id (swap! req-cnt inc)]
    (swap! cache (fn [mp] (assoc mp id result)))
    (binding [*out* in]
      (println (encode (assoc msg :req-id id)))
      (flush))
    (let [value @result]
      (swap! cache (fn [mp] (dissoc mp id)))
      value)))

(defn close
  [{:keys [reader in] :as client}]
  (ysend client {:f :terminate})
  (future-cancel reader))

(def proxy-num (atom 0))

(defn start-client
  []
  (let [cnt (swap! proxy-num inc)
        proc (. (Runtime/getRuntime) exec (into-array ["run-proxy.sh" (str cnt)]))
        cache (atom {})
        out (io/reader (. proc getInputStream))
        worker (fn []
                (binding [*in* out]
                  (loop []
                    (let [{id :req-id :as msg} (decode (read-line))]
                      (deliver (get @cache id) msg))
                    (recur))))]
    {:in (io/writer (. proc getOutputStream))
     :cache cache
     :req-cnt (atom 0)
     :proxy-id cnt
     :reader (future (worker))}))
