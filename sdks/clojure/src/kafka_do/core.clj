(ns kafka-do.core
  "Kafka SDK for Clojure - Event streaming with lazy sequences.

   Provides idiomatic Clojure access to Kafka via RPC with:
   - Lazy sequences for consuming messages
   - Transducer support for stream processing
   - core.async integration
   - Pure data structures (maps)

   Example:
     (require '[kafka-do.core :as kafka])

     ;; Produce
     (kafka/produce! \"orders\" {:order-id \"123\" :amount 99.99})

     ;; Consume as lazy sequence
     (doseq [record (kafka/consumer-seq \"orders\" \"processor\")]
       (println (:value record))
       (kafka/commit! record))

     ;; Stream processing
     (->> (kafka/consumer-seq \"orders\" \"processor\")
          (filter #(> (get-in % [:value :amount]) 100))
          (map :value)
          (take 10)
          (run! process-order))"
  (:require [kafka-do.producer :as producer]
            [kafka-do.consumer :as consumer]
            [kafka-do.admin :as admin]
            [kafka-do.types :as types]
            [kafka-do.client :as client]))

;; Re-export core functions for convenience

;; Client functions
(def connect client/connect)
(def close! client/close!)
(def connected? client/connected?)

;; Producer functions
(def produce! producer/produce!)
(def produce-async! producer/produce-async!)
(def produce-batch! producer/produce-batch!)
(def with-transaction producer/with-transaction)
(def tx-produce! producer/tx-produce!)

;; Consumer functions
(def consume! consumer/consume!)
(def consumer-seq consumer/consumer-seq)
(def consume-batch! consumer/consume-batch!)
(def commit! consumer/commit!)
(def subscribe! consumer/subscribe!)
(def unsubscribe! consumer/unsubscribe!)
(def seek! consumer/seek!)
(def seek-to-beginning! consumer/seek-to-beginning!)
(def seek-to-end! consumer/seek-to-end!)

;; Admin functions
(def create-topic! admin/create-topic!)
(def list-topics admin/list-topics)
(def describe-topic admin/describe-topic)
(def alter-topic! admin/alter-topic!)
(def delete-topic! admin/delete-topic!)
(def list-groups admin/list-groups)
(def describe-group admin/describe-group)
(def reset-offsets! admin/reset-offsets!)

;; Error types
(def kafka-error? types/kafka-error?)
(def connection-error? types/connection-error?)
(def producer-error? types/producer-error?)
(def consumer-error? types/consumer-error?)
(def topic-not-found-error? types/topic-not-found-error?)

;; ============================================================================
;; Stream Processing DSL
;; ============================================================================

(defn stream
  "Create a stream from a topic for processing.

   Returns a map that can be used with stream processing functions.

   Example:
     (-> (stream \"orders\")
         (filter-stream #(> (:amount %) 100))
         (map-stream #(assoc % :tier \"premium\"))
         (to-topic! \"high-value-orders\"))"
  [topic]
  {:type :stream
   :source topic
   :transforms []})

(defn filter-stream
  "Add a filter transformation to the stream.

   Example:
     (filter-stream stream #(= (:status %) \"active\"))"
  [stream pred]
  (update stream :transforms conj {:type :filter :pred pred}))

(defn map-stream
  "Add a map transformation to the stream.

   Example:
     (map-stream stream #(assoc % :processed-at (System/currentTimeMillis)))"
  [stream f]
  (update stream :transforms conj {:type :map :fn f}))

(defn to-topic!
  "Send stream results to a destination topic.

   This materializes the stream and processes all messages.

   Example:
     (to-topic! stream \"processed-orders\")"
  [stream topic]
  (let [xf (apply comp
                  (map (fn [{:keys [type pred fn]}]
                         (case type
                           :filter (filter pred)
                           :map (map fn)))
                       (:transforms stream)))]
    {:type :materialized-stream
     :source (:source stream)
     :destination topic
     :transducer xf}))

(defn for-each!
  "Execute a side-effect function for each stream element.

   Example:
     (for-each! stream #(println \"Received:\" %))"
  [stream f]
  (update stream :transforms conj {:type :for-each :fn f}))
