(ns alda.server
  (:require [taoensso.timbre :as log]
            [zeromq.device   :as zmqd]
            [zeromq.zmq      :as zmq])
  (:import [java.net ServerSocket]
           [org.zeromq ZMQException ZMQ$Error]))

(defn- find-open-port
  []
  (let [tmp-socket (ServerSocket. 0)
        port       (.getLocalPort tmp-socket)]
    (.close tmp-socket)
    port))

(defn start-server!
  ([frontend-port]
   (start-server! frontend-port (find-open-port)))
  ([frontend-port backend-port]
   (start-server! frontend-port backend-port (find-open-port)))
  ([frontend-port backend-port control-port]
   (let [zmq-ctx (zmq/zcontext)]
     (log/infof "Binding router socket on port %s..." frontend-port)
     (log/infof "Binding dealer socket on port %s..." backend-port)
     (log/infof "Binding worker control pub socket on port %s..." control-port)
     (with-open [frontend (doto (zmq/socket zmq-ctx :router)
                            (zmq/bind (str "tcp://*:" frontend-port)))
                 backend  (doto (zmq/socket zmq-ctx :dealer)
                            (zmq/bind (str "tcp://*:" backend-port)))
                 control  (doto (zmq/socket zmq-ctx :pub)
                            (zmq/bind (str "tcp://*:" control-port)))]
       (.addShutdownHook (Runtime/getRuntime)
         (Thread. (fn []
                    (log/info "Interrupt (e.g. Ctrl-C) received.")

                    (log/info "Murdering workers...")
                    (zmq/send-str control "KILL")

                    (log/info "Destroying zmq context...")
                    (zmq/destroy zmq-ctx)

                    (try
                      ((.interrupt (. Thread currentThread))
                       (.join (. Thread currentThread)))
                      (catch InterruptedException e)))))
       (log/info "Proxying requests...")
       ; proxies requests until the end of time (or until interrupted)
       (try
         (zmqd/proxy zmq-ctx frontend backend)
         (catch ZMQException e
           (when (= (.getErrorCode e) (.. ZMQ$Error ETERM getCode))
             (.. Thread currentThread interrupt))))

       (log/info "Exiting.")
       (System/exit 0)))))

