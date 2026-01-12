(ns kafka-do.client
  "Kafka client for .do services.

   Provides connection management and RPC access.

   Example:
     (def client (connect \"https://kafka.do\"))
     (close! client)"
  (:require [kafka-do.types :as types]))

;; ============================================================================
;; Client Protocol
;; ============================================================================

(defprotocol IKafkaClient
  "Protocol for Kafka client operations."
  (-connected? [this] "Check if client is connected.")
  (-close! [this] "Close the client connection.")
  (-rpc [this] "Get the underlying RPC client."))

;; ============================================================================
;; Client Implementation
;; ============================================================================

(defrecord KafkaClient [url api-key rpc-client connected options]
  IKafkaClient
  (-connected? [_]
    @connected)

  (-close! [_]
    (when @connected
      (reset! connected false)
      (when-let [rpc @rpc-client]
        (try
          (.close rpc)
          (catch Exception _))))
    nil)

  (-rpc [_]
    (when-not @connected
      (throw (types/connection-error "Client is not connected. Call connect first.")))
    @rpc-client))

;; ============================================================================
;; Public API
;; ============================================================================

(defn connect
  "Connect to a Kafka service.

   Args:
     config - Configuration map or URI string:
              :url - Service URL (e.g., \"https://kafka.do\")
              :api-key - API key for authentication
              :timeout - Request timeout in milliseconds (default: 30000)
              :retries - Number of retries (default: 3)

   If a string is provided, it's treated as the URL.
   URL and API key can also be set via environment variables:
     KAFKA_DO_URL
     KAFKA_DO_API_KEY

   Returns:
     Connected KafkaClient instance.

   Example:
     (def client (connect \"https://kafka.do\"))
     (def client (connect {:url \"https://kafka.do\"
                           :api-key \"your-api-key\"
                           :timeout 60000}))"
  ([]
   (connect {}))
  ([config]
   (let [config (if (string? config) {:url config} config)
         url (or (:url config)
                 (System/getenv "KAFKA_DO_URL")
                 "https://kafka.do")
         api-key (or (:api-key config)
                     (System/getenv "KAFKA_DO_API_KEY"))
         client (->KafkaClient url
                               api-key
                               (atom nil)
                               (atom false)
                               config)]
     ;; Connect via RPC
     (try
       ;; In real implementation, would use rpc-do library:
       ;; (require '[rpc-do.core :as rpc])
       ;; (reset! (:rpc-client client) (rpc/connect url config))
       (reset! (:connected client) true)
       client
       (catch Exception e
         (throw (types/connection-error
                 (str "Failed to connect to " url ": " (.getMessage e)))))))))

(defn close!
  "Close the client connection.

   Example:
     (close! client)"
  [client]
  (-close! client))

(defn connected?
  "Check if the client is connected.

   Example:
     (when (connected? client)
       (produce! client topic message))"
  [client]
  (-connected? client))

;; ============================================================================
;; with-client Macro
;; ============================================================================

(defmacro with-client
  "Execute body with a connected client, ensuring cleanup.

   Example:
     (with-client [client {:url \"https://kafka.do\"}]
       (produce! client \"orders\" {:order-id \"123\"}))"
  [[binding config] & body]
  `(let [~binding (connect ~config)]
     (try
       ~@body
       (finally
         (close! ~binding)))))

;; ============================================================================
;; Default Client
;; ============================================================================

(def ^:dynamic *default-client*
  "Dynamic var for default client.
   Can be bound to provide implicit client for operations."
  nil)

(defmacro with-default-client
  "Bind the default client for operations.

   Example:
     (with-default-client (connect)
       (produce! \"orders\" {:order-id \"123\"}))"
  [client & body]
  `(binding [*default-client* ~client]
     ~@body))

(defn default-client
  "Get the default client, throwing if not set."
  []
  (or *default-client*
      (throw (types/connection-error
              "No default client. Use with-default-client or pass client explicitly."))))
