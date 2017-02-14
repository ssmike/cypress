(ns jepsen.yt
  (:require [gniazdo.core :as ws]
            [clojure.data.json :as json])
  [:import java.lang.Runtime])

(defn encode
  [op]
  (let [allowed-keys #{:f :value}]
    (json/write-str
      (conj {} (filter (fn [[x _]]
                         (contains? allowed-keys x))
                       op)))))

(defn decode
  [msg]
  (let [mp (json/read-str msg)]
    (into {} (map (fn [[a b]] [(keyword a)
                               ((case a
                                  "value" identity
                                          keyword)
                                 b)])
                  mp))))

(defn connect
  [addr]
  (let [received (atom nil)
        on-receive (fn [msg] (deliver @received msg))
        socket (ws/connect addr :on-receive on-receive)]
        {:socket socket
         :received received}))

(defn ysend
  [{:keys [socket received]} msg]
  (reset! received (promise))
  (ws/send-msg socket (encode msg))
  (decode @@received))

(defn close
  [{socket :socket}]
  (ws/close socket))

(defn local
  [port]
  (str "ws://localhost:" port "/"))

(def free-port (atom 5000))

(defn start-client
  []
  (let [port (swap! free-port inc)
        addr (local port)]
    (. (Runtime/getRuntime) exec (into-array ["run-proxy.sh" (str port)]))
    (Thread/sleep 1500) ;;waiting for proxy)
    (let [sock (connect addr)]
      (ysend sock {:f :wait-for-yt})
       sock)))
