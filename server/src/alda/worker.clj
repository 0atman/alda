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
  [port]
  (log/info "Loading Alda environment...")
  (start-alda-environment!)
  (log/infof
    "Worker reporting for duty! Connecting to zmq socket on port %d..."
    port)
  (let [zmq-ctx (zmq/zcontext)]
    (with-open [socket (doto (zmq/socket zmq-ctx :rep)
                         (zmq/connect (str "tcp://*:" port)))]
      (while (not (.. Thread currentThread isInterrupted))
        (log/debug "Receiving request...")
        (let [req (zmq/receive-str socket)]
          (log/debug "Request received.")
          (try
            (let [msg (json/parse-string req true)
                  _   (log/debug "Processing message...")
                  res (process msg)]
              (log/debug "Sending response...")
              (zmq/send-str socket (json/generate-string res))
              (log/debug "Response sent."))
            (catch Throwable e
              (log/error e e)
              (zmq/send-str socket (json/generate-string (error-response e)))))))))
  (System/exit 0))

