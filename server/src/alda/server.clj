(ns alda.server
  (:require [zeromq.device   :as zmqd]
            [zeromq.zmq      :as zmq]
            [taoensso.timbre :as log])
  (:import [java.net ServerSocket]))

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
   (let [zmq-ctx (zmq/zcontext)]
     (log/infof "Starting Alda zmq routing server on port %s..." frontend-port)
     (log/infof "Starting Alda zmq dealing server on port %s..." backend-port)
     (with-open [frontend (doto (zmq/socket zmq-ctx :router)
                            (zmq/bind (str "tcp://*:" frontend-port)))
                 backend  (doto (zmq/socket zmq-ctx :dealer)
                            (zmq/bind (str "tcp://*:" backend-port)))]
       (log/info "Proxying requests...")
       ; proxies requests until the end of time
       (zmqd/proxy zmq-ctx frontend backend)

       ; if/when the current context is closed, clean up and exit
       (log/info "Cleaning up...")
       (zmq/destroy-socket frontend)
       (zmq/destroy-socket backend)
       (zmq/destroy zmq-ctx)
       (log/info "Exiting...")
       (System/exit 0)))))

