(ns alda.server
  (:require [alda.util                 :as util]
            [cheshire.core             :as json]
            [me.raynes.conch.low-level :as sh]
            [taoensso.timbre           :as log]
            [zeromq.device             :as zmqd]
            [zeromq.zmq                :as zmq])
  (:import [java.net ServerSocket]
           [java.util.concurrent ConcurrentLinkedQueue]
           [org.zeromq ZFrame ZMQException ZMQ$Error ZMsg]))

(def ^:const HEARTBEAT-INTERVAL 1000)

(def available-workers (util/queue))
(def busy-workers      (ref #{}))
(defn all-workers []   (concat @available-workers @busy-workers))

; (doseq [[x y] [[:available available-workers]
;                [:busy busy-workers]]]
;   (add-watch y :key (fn [_ _ old new]
;                       (when (not= old new)
;                         (prn x new)))))

(def no-workers-available-response
  (json/generate-string
    {:success false
     :body "No workers processes are ready yet. Please wait a minute."}))

(def all-workers-are-busy-response
  (json/generate-string
    {:success false
     :body "All worker processes are currently busy. Please wait until playback is complete and re-submit your request."}))

(defn add-or-requeue-worker
  [address]
  (dosync
    (alter busy-workers disj address)
    (if (util/check-queue available-workers (partial = address))
      (util/re-queue available-workers (partial = address))
      (util/push-queue available-workers address))))

(defn note-that-worker-is-busy
  [address]
  (dosync
    (util/remove-from-queue available-workers (partial = address))
    (alter busy-workers conj address)))

(defn- find-open-port
  []
  (let [tmp-socket (ServerSocket. 0)
        port       (.getLocalPort tmp-socket)]
    (.close tmp-socket)
    port))

(defn start-workers! [workers port]
  (let [program-path (util/program-path)
        cmd (if (re-find #"clojure.*jar$" program-path)
              ; this means we are running the `boot dev` task, and the "program
              ; path" ends up being clojure-<version>.jar instead of alda; in
              ; this scenario, we can use the `boot dev` task to start each
              ; worker
              ["boot" "dev" "--alda-fingerprint" "-a" "worker" "-p" (str port)]
              ; otherwise, use the same program that was used to start the
              ; server (e.g. /usr/local/bin/alda)
              [program-path "-p" (str port) "--alda-fingerprint" "worker"])]
    (dotimes [_ workers]
      (apply sh/proc cmd))))

(defn start-server!
  ([workers frontend-port]
   (start-server! workers frontend-port (find-open-port)))
  ([workers frontend-port backend-port]
   (let [zmq-ctx        (zmq/zcontext)
         poller         (zmq/poller zmq-ctx 2)]
     (log/infof "Binding frontend socket on port %s..." frontend-port)
     (log/infof "Binding backend socket on port %s..." backend-port)
     (with-open [frontend (doto (zmq/socket zmq-ctx :router)
                            (zmq/bind (str "tcp://*:" frontend-port)))
                 backend  (doto (zmq/socket zmq-ctx :router)
                            (zmq/bind (str "tcp://*:" backend-port)))]
       (zmq/register poller frontend :pollin)
       (zmq/register poller backend :pollin)
       (log/infof "Spawning %s workers..." workers)
       (start-workers! workers backend-port)
       (.addShutdownHook (Runtime/getRuntime)
         (Thread. (fn []
                    (log/info "Interrupt (e.g. Ctrl-C) received.")

                    (log/info "Murdering workers...")
                    (doseq [worker (all-workers)]
                      (.send worker backend (+ ZFrame/REUSE ZFrame/MORE))
                      (.send (ZFrame. "KILL") backend 0))

                    (try
                      ((.interrupt (. Thread currentThread))
                       (.join (. Thread currentThread)))
                      (catch InterruptedException e)))))
       (try
         (while true
           (let [heartbeat-time (+ (System/currentTimeMillis) HEARTBEAT-INTERVAL)]
             (zmq/poll poller HEARTBEAT-INTERVAL)
             (when (zmq/check-poller poller 0 :pollin) ; frontend
               (when-let [msg (ZMsg/recvMsg frontend)]
                 (cond
                   (not (empty? @available-workers))
                   (do
                     (log/debug "Receiving message from frontend...")
                     (let [worker (util/pop-queue available-workers)]
                       (log/debugf "Forwarding message to worker %s..." worker)
                       (.push msg worker)
                       (.send msg backend)))

                   (not (empty? @busy-workers))
                   (do
                     (log/debug "All workers are currently busy.")
                     (log/debug "Letting the client know...")
                     (let [envelope (.unwrap msg)
                           msg      (doto (ZMsg/newStringMsg
                                            (into-array String [all-workers-are-busy-response]))
                                      (.wrap envelope))]
                       (.send msg frontend)))

                   :else
                   (do
                     (log/debug "Workers not ready yet.")
                     (log/debug "Letting the client know...")
                     (let [envelope (.unwrap msg)
                           msg      (doto (ZMsg/newStringMsg
                                            (into-array String [no-workers-available-response]))
                                      (.wrap envelope))]
                       (.send msg frontend))))))
             (when (zmq/check-poller poller 1 :pollin) ; backend
               (when-let [msg (ZMsg/recvMsg backend)]
                 (let [address (.unwrap msg)]
                   (if (= 1 (.size msg))
                     (let [frame   (.getFirst msg)
                           data    (-> frame .getData (String.))]
                       (case data
                         "BUSY"      (note-that-worker-is-busy address)
                         "AVAILABLE" (add-or-requeue-worker address)
                         "READY"     (add-or-requeue-worker address)))
                     (do
                       (log/debug "Forwarding backend response to frontend...")
                       (.send msg frontend))))))
             (when (> (System/currentTimeMillis) heartbeat-time)
               (doseq [worker (all-workers)]
                 (.send worker backend (+ ZFrame/REUSE ZFrame/MORE))
                 (.send (ZFrame. "HEARTBEAT") backend 0)))))
         (catch ZMQException e
           (when (= (.getErrorCode e) (.. ZMQ$Error ETERM getCode))
             (.. Thread currentThread interrupt)))
         (finally
           (log/info "Destroying zmq context...")
           (zmq/destroy zmq-ctx)

           (log/info "Exiting.")
           (System/exit 0)))))))

