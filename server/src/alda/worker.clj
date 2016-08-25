(ns alda.worker
  (:require [alda.now        :as    now]
            [alda.parser     :refer (parse-input)]
            [alda.sound      :refer (*play-opts*)]
            [alda.sound.midi :as    midi]
            [alda.version    :refer (-version-)]
            [cheshire.core   :as    json]
            [clojure.pprint  :refer (pprint)]
            [taoensso.timbre :as    log]
            [zeromq.zmq      :as    zmq]))

(defn start-alda-environment!
  []
  (midi/fill-midi-synth-pool!)
  (while (not (midi/midi-synth-available?))
    (Thread/sleep 250)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- success-response
  [body]
  {:success true
   :body    (if (map? body)
              (json/generate-string body)
              body)})

(defn- error-response
  [e]
  {:success false
   :body    (if (string? e)
              e
              (.getMessage e))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn handle-code
  [code]
  (try
    (log/debug "Requiring alda.lisp...")
    (require '[alda.lisp :refer :all])
    (if-let [score (try
                     (log/debug "Parsing input...")
                     (parse-input code :map)
                     (catch Throwable e
                       (log/error e e)
                       nil))]
      (do
        (log/debug "Playing score...")
        (now/play-score! score)
        (success-response "Playing..."))
      (error-response "Invalid Alda syntax."))
    (catch Throwable e
      (log/error e e)
      (error-response e))))

(defn handle-code-parse
  [code & {:keys [mode] :or {mode :lisp}}]
  (try
    (require '[alda.lisp :refer :all])
    (success-response (case mode
                        :lisp (let [result (parse-input code mode)]
                                (with-out-str (pprint result)))
                        :map  (parse-input code mode)))
    (catch Throwable e
      (log/error e e)
      (error-response "Invalid Alda syntax."))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti process :command)

(defmethod process :default
  [{:keys [command]}]
  (error-response (format "Unrecognized command: %s" command)))

(defmethod process nil
  [_]
  (error-response "Missing command"))

(defmethod process "parse"
  [{:keys [body options]}]
  (let [{:keys [as]} options]
    (case as
      "lisp" (handle-code-parse body :mode :lisp)
      "map"  (handle-code-parse body :mode :map)
      nil    (error-response "Missing option: as")
      (error-response (format "Invalid format: %s" as)))))

(defmethod process "ping"
  [_]
  (success-response "OK"))

(defmethod process "play"
  [{:keys [body options]}]
  (let [{:keys [from to]} options]
    (binding [*play-opts* (assoc *play-opts*
                                 :from     from
                                 :to       to
                                 :one-off? true)]
      (handle-code body))))

(defmethod process "version"
  [_]
  (success-response -version-))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn start-worker!
  ([dealer-port control-port]
   (log/info "Loading Alda environment...")
   (start-alda-environment!)
   (log/info "Worker reporting for duty!")
   (log/infof "Connecting to dealer socket on port %d..." dealer-port)
   (log/infof "Connecting to control socket on port %d..." control-port)
   (let [zmq-ctx  (zmq/zcontext)
         poller   (zmq/poller zmq-ctx 2)
         running? (atom true)]
     (with-open [work    (doto (zmq/socket zmq-ctx :rep)
                           (zmq/connect (str "tcp://*:" dealer-port)))
                 control (doto (zmq/socket zmq-ctx :sub)
                           (zmq/connect (str "tcp://*:" control-port))
                           (zmq/subscribe ""))]
       (zmq/register poller work :pollin)
       (zmq/register poller control :pollin)
       (while (and @running? (not (.. Thread currentThread isInterrupted)))
         (log/debug "Polling for messages...")
         (zmq/poll poller)

         (when (zmq/check-poller poller 1 :pollin)
           (log/debug "Receiving control signal from server...")
           (let [signal (zmq/receive-str control)]
             (log/debug "Signal received.")
             (if (= signal "KILL")
               (do
                 (log/info "Received KILL signal from server.")
                 (reset! running? false))
               (log/errorf "Unrecognized signal: %s" signal))))

         (when (zmq/check-poller poller 0 :pollin)
           (log/debug "Receiving request...")
           (let [req (zmq/receive-str work)]
             (log/debug "Request received.")
             (try
               (let [msg (json/parse-string req true)
                     _   (log/debug "Processing message...")
                     res (process msg)]
                 (log/debug "Sending response...")
                 (zmq/send-str work (json/generate-string res))
                 (log/debug "Response sent."))
               (catch Throwable e
                 (log/error e e)
                 (zmq/send-str work (json/generate-string
                                      (error-response e)))))))))
     (log/info "Shutting down.")
     (System/exit 0))))

