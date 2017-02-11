(ns jepsen.yt
  (:require [gniazdo.core :as ws]
            [clojure.data.json :as json]
            [clojure.java.shell :as sh]))

(def encode json/write-str)
(defn decode
  [msg]
  (let [mp (json/read-str msg)]
    (into {} (map (fn [[a b]] [(keyword a) b]) mp))))

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
    (sh/sh "proxy.py" (str port))
    (let [sock (connect addr)]
      (ysend sock :wait-for-yt)
       sock)))