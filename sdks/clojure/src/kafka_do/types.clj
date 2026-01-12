(ns kafka-do.types
  "Type definitions and error handling for kafka-do SDK.

   All operations return plain Clojure maps for records and metadata.
   Errors are represented as ex-info exceptions with data maps."
  (:require [clojure.string :as str])
  (:import [java.util Base64]))

;; ============================================================================
;; Record Types (as maps)
;; ============================================================================

(defn record-metadata
  "Create a RecordMetadata map for a sent message.

   Keys:
     :topic - The topic the record was sent to
     :partition - The partition the record was sent to
     :offset - The offset of the record in the partition
     :timestamp - The timestamp (milliseconds since epoch)
     :serialized-key-size - Size of serialized key in bytes
     :serialized-value-size - Size of serialized value in bytes"
  [& {:keys [topic partition offset timestamp
             serialized-key-size serialized-value-size]
      :or {serialized-key-size 0 serialized-value-size 0}}]
  {:topic topic
   :partition partition
   :offset offset
   :timestamp timestamp
   :serialized-key-size serialized-key-size
   :serialized-value-size serialized-value-size})

(defn consumer-record
  "Create a ConsumerRecord map for a consumed message.

   Keys:
     :topic - The topic this record was received from
     :partition - The partition from which the record was received
     :offset - The position of this record in the partition
     :timestamp - The timestamp (milliseconds since epoch)
     :key - The key of the record (may be nil)
     :value - The value of the record (auto-decoded from JSON if possible)
     :headers - Map of header name to value"
  [& {:keys [topic partition offset timestamp key value headers]
      :or {headers {}}}]
  {:topic topic
   :partition partition
   :offset offset
   :timestamp timestamp
   :key key
   :value value
   :headers headers})

(defn producer-record
  "Create a ProducerRecord map for sending.

   Keys:
     :topic - The topic to send to
     :value - The message value
     :key - Optional message key
     :partition - Optional specific partition
     :timestamp - Optional timestamp
     :headers - Optional headers map"
  [topic value & {:keys [key partition timestamp headers]}]
  (cond-> {:topic topic :value value}
    key (assoc :key key)
    partition (assoc :partition partition)
    timestamp (assoc :timestamp timestamp)
    headers (assoc :headers headers)))

(defn topic-partition
  "Create a TopicPartition map.

   Keys:
     :topic - The topic name
     :partition - The partition number"
  [topic partition]
  {:topic topic :partition partition})

(defn topic-config
  "Create a TopicConfig map for creating topics.

   Keys:
     :name - Topic name
     :partitions - Number of partitions (default: 1)
     :replication-factor - Replication factor (default: 1)
     :config - Additional topic configuration"
  [name & {:keys [partitions replication-factor config]
           :or {partitions 1 replication-factor 1 config {}}}]
  {:name name
   :partitions partitions
   :replication-factor replication-factor
   :config config})

(defn topic-metadata
  "Create a TopicMetadata map."
  [& {:keys [name partitions replication-factor config]
      :or {config {}}}]
  {:name name
   :partitions partitions
   :replication-factor replication-factor
   :config config})

(defn consumer-group-metadata
  "Create a ConsumerGroupMetadata map."
  [& {:keys [group-id state members coordinator]
      :or {members []}}]
  {:group-id group-id
   :state state
   :members members
   :coordinator coordinator})

;; ============================================================================
;; Batch Result
;; ============================================================================

(defn batch-result
  "Create a BatchResult map for batch operations.

   Keys:
     :successful - Vector of RecordMetadata for successful sends
     :failed - Vector of [record error] tuples for failed sends"
  [& {:keys [successful failed]
      :or {successful [] failed []}}]
  {:successful (vec successful)
   :failed (vec failed)})

;; ============================================================================
;; Error Types
;; ============================================================================

(defn kafka-error
  "Create a Kafka error as ExceptionInfo.

   Args:
     message - Error message
     type - Error type keyword
     code - Optional error code
     retriable - Whether the error is retriable (default: false)"
  [message type & {:keys [code retriable] :or {retriable false}}]
  (ex-info message
           {:type type
            :code code
            :retriable? retriable}))

(defn connection-error
  "Create a connection error."
  [message]
  (kafka-error message :connection :retriable true))

(defn producer-error
  "Create a producer error."
  [message & {:keys [code retriable]}]
  (kafka-error message :producer :code code :retriable retriable))

(defn consumer-error
  "Create a consumer error."
  [message & {:keys [code retriable]}]
  (kafka-error message :consumer :code code :retriable retriable))

(defn topic-not-found-error
  "Create a topic not found error."
  [topic]
  (kafka-error (str "Topic not found: " topic) :topic-not-found :code "TOPIC_NOT_FOUND"))

(defn topic-exists-error
  "Create a topic already exists error."
  [topic]
  (kafka-error (str "Topic already exists: " topic) :topic-exists :code "TOPIC_ALREADY_EXISTS"))

(defn timeout-error
  "Create a timeout error."
  [message]
  (kafka-error message :timeout :code "TIMEOUT" :retriable true))

(defn serialization-error
  "Create a serialization error."
  [message]
  (kafka-error message :serialization :code "SERIALIZATION"))

;; ============================================================================
;; Error Predicates
;; ============================================================================

(defn kafka-error?
  "Check if an exception is a Kafka error."
  [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (contains? (ex-data e) :type)))

(defn connection-error?
  "Check if an exception is a connection error."
  [e]
  (and (kafka-error? e)
       (= :connection (:type (ex-data e)))))

(defn producer-error?
  "Check if an exception is a producer error."
  [e]
  (and (kafka-error? e)
       (= :producer (:type (ex-data e)))))

(defn consumer-error?
  "Check if an exception is a consumer error."
  [e]
  (and (kafka-error? e)
       (= :consumer (:type (ex-data e)))))

(defn topic-not-found-error?
  "Check if an exception is a topic not found error."
  [e]
  (and (kafka-error? e)
       (= :topic-not-found (:type (ex-data e)))))

(defn retriable?
  "Check if an error is retriable."
  [e]
  (and (kafka-error? e)
       (:retriable? (ex-data e))))

;; ============================================================================
;; Serialization Helpers
;; ============================================================================

(defn encode-base64
  "Encode bytes to base64 string."
  [^bytes data]
  (when data
    (.encodeToString (Base64/getEncoder) data)))

(defn decode-base64
  "Decode base64 string to bytes."
  [^String s]
  (when s
    (.decode (Base64/getDecoder) s)))

(defn serialize-record
  "Serialize a ProducerRecord for RPC transmission."
  [record]
  (let [value-bytes (if (bytes? (:value record))
                      (:value record)
                      (.getBytes (pr-str (:value record)) "UTF-8"))
        key-bytes (when (:key record)
                    (if (bytes? (:key record))
                      (:key record)
                      (.getBytes (str (:key record)) "UTF-8")))]
    (cond-> {:topic (:topic record)
             :value (encode-base64 value-bytes)}
      key-bytes (assoc :key (encode-base64 key-bytes))
      (:partition record) (assoc :partition (:partition record))
      (:timestamp record) (assoc :timestamp (:timestamp record))
      (:headers record) (assoc :headers
                               (into {}
                                     (map (fn [[k v]]
                                            [k (encode-base64
                                                (if (bytes? v)
                                                  v
                                                  (.getBytes (str v) "UTF-8")))])
                                          (:headers record)))))))

(defn deserialize-consumer-record
  "Deserialize a ConsumerRecord from RPC response."
  [data]
  (let [value-bytes (decode-base64 (:value data))
        key-bytes (when (:key data) (decode-base64 (:key data)))
        headers (when (:headers data)
                  (into {}
                        (map (fn [[k v]] [k (decode-base64 v)])
                             (:headers data))))]
    (consumer-record
     :topic (:topic data)
     :partition (:partition data)
     :offset (:offset data)
     :timestamp (:timestamp data)
     :key key-bytes
     :value (try
              (read-string (String. value-bytes "UTF-8"))
              (catch Exception _
                value-bytes))
     :headers headers)))

(defn deserialize-record-metadata
  "Deserialize RecordMetadata from RPC response."
  [data]
  (record-metadata
   :topic (:topic data)
   :partition (:partition data)
   :offset (:offset data)
   :timestamp (:timestamp data)
   :serialized-key-size (get data :serializedKeySize 0)
   :serialized-value-size (get data :serializedValueSize 0)))

;; ============================================================================
;; Offset Types
;; ============================================================================

(def offset-earliest :earliest)
(def offset-latest :latest)

(defn offset-timestamp
  "Create a timestamp-based offset."
  [timestamp]
  [:timestamp timestamp])

(defn timestamp-offset?
  "Check if an offset is timestamp-based."
  [offset]
  (and (vector? offset)
       (= :timestamp (first offset))))
