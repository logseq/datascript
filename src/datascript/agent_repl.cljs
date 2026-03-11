(ns datascript.agent-repl
  (:require
    [datascript.core]))

(def default-host
  "127.0.0.1")

(def default-port
  8787)

(defn parse-port [value]
  (let [port (js/parseInt value 10)]
    (if (js/Number.isNaN port)
      default-port
      port)))

(defn env [name fallback]
  (or (some-> js/process .-env (aget name))
      fallback))

(defn json-response [res status payload]
  (.writeHead res status #js {"Content-Type" "application/json; charset=utf-8"})
  (.end res (js/JSON.stringify (clj->js payload))))

(defn request-body [req callback]
  (let [chunks (atom [])]
    (.on req "data" (fn [chunk] (swap! chunks conj chunk)))
    (.on req "end" (fn [] (callback (apply str @chunks))))))

(defn cljs-eval-fn []
  (aget js/globalThis "cljs_eval"))

(defn health-payload []
  {:ok true
   :service "datascript-agent-repl"
   :cljs-eval-ready (boolean (cljs-eval-fn))})

(defn handle-health [res]
  (json-response res 200 (health-payload)))

(defn handle-not-found [res]
  (json-response res 404 {:ok false
                          :error "not found"}))

(defn eval-payload [body]
  (let [payload (js->clj (js/JSON.parse body) :keywordize-keys true)]
    {:code (:code payload)
     :ns   (or (:ns payload) "cljs.user")}))

(defn handle-eval [res body]
  (let [{:keys [code ns]} (eval-payload body)
        cljs-eval (cljs-eval-fn)]
    (cond
      (not (string? code))
      (json-response res 400 {:ok false
                              :error "body.code must be a string"})

      (nil? cljs-eval)
      (json-response res 503 {:ok false
                              :error "cljs_eval is not ready yet"})

      :else
      (-> (.call cljs-eval js/globalThis code #js {:ns ns})
          (.then (fn [result]
                   (json-response res 200 {:ok true
                                           :ns ns
                                           :result (pr-str result)})))
          (.catch (fn [error]
                    (json-response res 500 {:ok false
                                            :ns ns
                                            :error (str error)})))))))

(defn request-handler [_server]
  (fn [req res]
    (cond
      (and (= "GET" (.-method req))
           (= "/health" (.-url req)))
      (handle-health res)

      (and (= "POST" (.-method req))
           (= "/eval" (.-url req)))
      (request-body req #(handle-eval res %))

      :else
      (handle-not-found res))))

(defn main []
  (let [http   (js/require "http")
        host   (env "CLJS_EVAL_HOST" default-host)
        port   (parse-port (env "CLJS_EVAL_PORT" (str default-port)))
        server (.createServer http (request-handler nil))]
    (.listen server port host
      (fn []
        (println (str "datascript agent repl listening on http://" host ":" port))
        (println "POST /eval with JSON {\"code\":\"(+ 1 2)\",\"ns\":\"cljs.user\"}")
        (println "GET /health for readiness")))))
