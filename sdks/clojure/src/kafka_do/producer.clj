(ns kafka-do.producer
  "Kafka producer for sending messages.

   Provides synchronous and asynchronous message sending with
   support for batching and transactions.

   Example:
     ;; Simple produce
     (produce! \"orders\" {:order-id \"123\" :amount 99.99})

     ;; With key for partitioning
     (produce! \"orders\" {:order-id \"123\"} {:key \"customer-456\"})

     ;; Batch produce
     (produce-batch! \"orders\"
       [{:order-id \"124\"} {:order-id \"125\"}])"
  (:require [kafka-do.types :as types]
            [kafka-do.client :as client]))

;; ============================================================================
;; Producer Operations
;; ============================================================================

(defn produce!
  "Send a single message to a Kafka topic.

   Args:
     topic - Topic name
     value - Message value (will be serialized)
     opts - Optional configuration:
            :key - Message key for partitioning
            :partition - Specific partition to send to
            :timestamp - Message timestamp (milliseconds since epoch)
            :headers - Map of header name to value
            :client - Kafka client (uses default if not provided)

   Returns:
     RecordMetadata map with :topic, :partition, :offset, :timestamp

   Example:
     (produce! \"orders\" {:order-id \"123\"})
     (produce! \"orders\" {:order-id \"123\"} {:key \"customer-456\"})
     (produce! \"orders\" order {:headers {\"correlation-id\" \"abc-123\"}})"
  ([topic value]
   (produce! topic value {}))
  ([topic value opts]
   (let [kafka-client (or (:client opts) (client/default-client))
         rpc (client/-rpc kafka-client)
         record (types/producer-record topic value
                                       :key (:key opts)
                                       :partition (:partition opts)
                                       :timestamp (:timestamp opts)
                                       :headers (:headers opts))
         serialized (types/serialize-record record)]
     (try
       (let [result (-> rpc .-kafka .-producer (.send serialized))]
         (types/deserialize-record-metadata result))
       (catch Exception e
         (throw (types/producer-error (str "Failed to send message: " (.getMessage e)))))))))

(defn produce-async!
  "Send a message asynchronously, returning a promise.

   Args:
     topic - Topic name
     value - Message value
     opts - Same options as produce!

   Returns:
     Promise that will contain RecordMetadata when complete.

   Example:
     (def result (produce-async! \"orders\" {:order-id \"123\"}))
     @result ;; Dereference to get result"
  ([topic value]
   (produce-async! topic value {}))
  ([topic value opts]
   (future (produce! topic value opts))))

(defn produce-batch!
  "Send a batch of messages.

   Args:
     topic - Topic name
     messages - Vector of messages. Each message can be:
                - A value map
                - A map with :key and :value keys
     opts - Optional configuration:
            :client - Kafka client

   Returns:
     BatchResult map with :successful and :failed vectors.

   Example:
     (produce-batch! \"orders\"
       [{:order-id \"124\" :amount 49.99}
        {:order-id \"125\" :amount 149.99}])

     ;; With keys
     (produce-batch! \"orders\"
       [{:key \"cust-1\" :value {:order-id \"124\"}}
        {:key \"cust-2\" :value {:order-id \"125\"}}])"
  ([topic messages]
   (produce-batch! topic messages {}))
  ([topic messages opts]
   (let [kafka-client (or (:client opts) (client/default-client))
         rpc (client/-rpc kafka-client)
         records (mapv (fn [msg]
                         (if (and (map? msg) (contains? msg :value))
                           (types/producer-record topic (:value msg)
                                                  :key (:key msg)
                                                  :partition (:partition msg)
                                                  :timestamp (:timestamp msg)
                                                  :headers (:headers msg))
                           (types/producer-record topic msg)))
                       messages)
         serialized (mapv types/serialize-record records)]
     (try
       (let [results (-> rpc .-kafka .-producer (.sendBatch serialized))
             batch-result (types/batch-result)]
         (reduce
          (fn [result [i res]]
            (if (:error res)
              (update result :failed conj
                      [(nth records i)
                       (types/producer-error (get-in res [:error :message] "Unknown error"))])
              (update result :successful conj (types/deserialize-record-metadata res))))
          batch-result
          (map-indexed vector results)))
       (catch Exception e
         (types/batch-result
          :failed (mapv #(vector % (types/producer-error (str "Batch failed: " (.getMessage e))))
                        records)))))))

;; ============================================================================
;; Transactional Producer
;; ============================================================================

(defrecord Transaction [client tx-id active]
  java.io.Closeable
  (close [this]
    (when @active
      (reset! active false))))

(defn begin-transaction!
  "Begin a new transaction.

   Returns:
     Transaction object for use with tx-produce!"
  [& {:keys [client]}]
  (let [kafka-client (or client (client/default-client))
        tx-id (str (java.util.UUID/randomUUID))]
    (->Transaction kafka-client tx-id (atom true))))

(defn tx-produce!
  "Send a message within a transaction.

   Args:
     tx - Transaction object
     topic - Topic name
     value - Message value
     opts - Same options as produce! (without :client)

   Example:
     (with-transaction tx
       (tx-produce! tx \"orders\" {:order-id \"123\" :status :created})
       (tx-produce! tx \"orders\" {:order-id \"123\" :status :validated}))"
  ([tx topic value]
   (tx-produce! tx topic value {}))
  ([tx topic value opts]
   (when-not @(:active tx)
     (throw (types/producer-error "Transaction is not active")))
   (produce! topic value (assoc opts
                                :client (:client tx)
                                :transaction-id (:tx-id tx)))))

(defn commit-transaction!
  "Commit a transaction."
  [tx]
  (when @(:active tx)
    (let [rpc (client/-rpc (:client tx))]
      (try
        (-> rpc .-kafka .-producer (.commitTransaction (:tx-id tx)))
        (finally
          (reset! (:active tx) false))))))

(defn abort-transaction!
  "Abort a transaction."
  [tx]
  (when @(:active tx)
    (let [rpc (client/-rpc (:client tx))]
      (try
        (-> rpc .-kafka .-producer (.abortTransaction (:tx-id tx)))
        (finally
          (reset! (:active tx) false))))))

(defmacro with-transaction
  "Execute body within a transaction.

   Automatically commits on success, aborts on exception.

   Example:
     (with-transaction [tx]
       (tx-produce! tx \"orders\" {:order-id \"123\" :status :created})
       (tx-produce! tx \"orders\" {:order-id \"123\" :status :validated}))"
  [[tx-binding & opts] & body]
  `(let [~tx-binding (begin-transaction! ~@opts)]
     (try
       (let [result# (do ~@body)]
         (commit-transaction! ~tx-binding)
         result#)
       (catch Exception e#
         (abort-transaction! ~tx-binding)
         (throw e#)))))

;; ============================================================================
;; Producer Configuration
;; ============================================================================

(def default-producer-config
  "Default producer configuration."
  {:acks :all
   :retries 3
   :retry-backoff-ms 100
   :batch-size 16384
   :linger-ms 5
   :compression :none})

(defn producer-config
  "Create a producer configuration map.

   Options:
     :acks - Acknowledgment mode (:all, :1, :0)
     :retries - Number of retries
     :retry-backoff-ms - Backoff between retries
     :batch-size - Maximum batch size in bytes
     :linger-ms - Time to wait before sending
     :compression - Compression type (:none, :gzip, :snappy, :lz4, :zstd)"
  [& opts]
  (merge default-producer-config (apply hash-map opts)))
