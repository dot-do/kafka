# kafka-do

> Event Streaming for Clojure. Lazy Sequences. Transducers. Zero Ops.

```clojure
(kafka/produce! "orders" {:order-id "123" :amount 99.99})
```

Immutable data. Functional composition. The Clojure way.

## Installation

### Leiningen/Boot

```clojure
[com.dotdo/kafka "0.1.0"]
```

### deps.edn

```clojure
com.dotdo/kafka {:mvn/version "0.1.0"}
```

Requires Clojure 1.11+.

## Quick Start

```clojure
(ns myapp.core
  (:require [com.dotdo.kafka :as kafka]))

;; Produce
(kafka/produce! "orders" {:order-id "123" :amount 99.99})

;; Consume
(kafka/consume! "orders" "my-processor"
  (fn [record]
    (println "Received:" (:value record))
    :commit))
```

## Configuration

```clojure
;; Using environment variables
;; KAFKA_DO_URL=https://kafka.do
;; KAFKA_DO_API_KEY=your-api-key

;; Or programmatic configuration
(def client
  (kafka/client
    {:url "https://kafka.do"
     :api-key "your-api-key"
     :timeout 30000
     :retries 3}))
```

## Producing Messages

### Simple Producer

```clojure
;; Send single message
(kafka/produce! "orders" {:order-id "123" :amount 99.99})

;; Send with key for partitioning
(kafka/produce! "orders" {:order-id "123" :amount 99.99}
  {:key "customer-456"})

;; Send with headers
(kafka/produce! "orders" {:order-id "123"}
  {:key "customer-456"
   :headers {"correlation-id" "abc-123"}})

;; Async produce (returns a promise)
(def result (kafka/produce-async! "orders" {:order-id "123"}))
@result ;; Dereference to get result
```

### Batch Producer

```clojure
;; Send batch
(kafka/produce-batch! "orders"
  [{:order-id "124" :amount 49.99}
   {:order-id "125" :amount 149.99}
   {:order-id "126" :amount 29.99}])

;; Send batch with keys
(kafka/produce-batch! "orders"
  [{:key "cust-1" :value {:order-id "124"}}
   {:key "cust-2" :value {:order-id "125"}}])
```

### Transactional Producer

```clojure
(kafka/with-transaction "orders"
  (fn [tx]
    (kafka/tx-produce! tx {:order-id "123" :status :created})
    (kafka/tx-produce! tx {:order-id "123" :status :validated})))
;; Automatically commits on success, aborts on exception
```

## Consuming Messages

### Basic Consumer with Callback

```clojure
(kafka/consume! "orders" "order-processor"
  (fn [record]
    (println "Topic:" (:topic record))
    (println "Partition:" (:partition record))
    (println "Offset:" (:offset record))
    (println "Key:" (:key record))
    (println "Value:" (:value record))
    (println "Timestamp:" (:timestamp record))
    (println "Headers:" (:headers record))

    (process-order (:value record))
    :commit)) ;; Return :commit to commit offset
```

### Consumer as Lazy Sequence

```clojure
;; Get lazy sequence of records
(def records (kafka/consumer-seq "orders" "processor"))

;; Process with standard sequence functions
(->> records
     (filter #(> (get-in % [:value :amount]) 100))
     (map :value)
     (take 10)
     (run! process-high-value-order))

;; Don't forget to commit!
(doseq [r (take 10 records)]
  (process-order (:value r))
  (kafka/commit! r))
```

### Consumer with Transducers

```clojure
(def xf
  (comp
    (filter #(> (get-in % [:value :amount]) 100))
    (map :value)
    (take 10)))

(transduce xf
  (fn
    ([] [])
    ([result] result)
    ([result order]
     (process-order order)
     (conj result order)))
  []
  (kafka/consumer-seq "orders" "processor"))
```

### Consumer with Configuration

```clojure
(kafka/consume! "orders" "processor"
  {:offset :earliest
   :auto-commit true
   :max-poll-records 100
   :session-timeout 30000}
  (fn [record]
    (process-order (:value record))
    :ok)) ;; Auto-committed
```

### Consumer from Timestamp

```clojure
(require '[java-time :as t])

(def yesterday (t/minus (t/instant) (t/days 1)))

(kafka/consume! "orders" "replay"
  {:offset [:timestamp yesterday]}
  (fn [record]
    (println "Replaying:" (:value record))
    :commit))
```

### Batch Consumer

```clojure
(kafka/consume-batch! "orders" "batch-processor"
  {:batch-size 100
   :batch-timeout 5000}
  (fn [batch]
    (doseq [record batch]
      (process-order (:value record)))
    :commit-all))
```

### Parallel Consumer with core.async

```clojure
(require '[clojure.core.async :as a])

(let [ch (a/chan 100)
      records (kafka/consumer-seq "orders" "parallel-processor")]

  ;; Start workers
  (dotimes [_ 10]
    (a/go-loop []
      (when-let [record (a/<! ch)]
        (process-order (:value record))
        (kafka/commit! record)
        (recur))))

  ;; Feed records to channel
  (doseq [record records]
    (a/>!! ch record)))
```

## Stream Processing

### Filter and Transform

```clojure
(-> (kafka/stream "orders")
    (kafka/filter-stream #(> (:amount %) 100))
    (kafka/map-stream #(assoc % :tier "premium"))
    (kafka/to-topic! "high-value-orders"))
```

### Windowed Aggregations

```clojure
(-> (kafka/stream "orders")
    (kafka/window {:type :tumbling :duration 300000}) ;; 5 minutes
    (kafka/group-by :customer-id)
    (kafka/count-stream)
    (kafka/for-each!
      (fn [[key window]]
        (println "Customer" key ":" (:value window)
                 "orders in" (:start window) "-" (:end window)))))
```

### Joins

```clojure
(def orders (kafka/stream "orders"))
(def customers (kafka/stream "customers"))

(-> (kafka/join-streams orders customers
      {:on (fn [order customer]
             (= (:customer-id order) (:id customer)))
       :window 3600000}) ;; 1 hour
    (kafka/for-each!
      (fn [[order customer]]
        (println "Order by" (:name customer)))))
```

### Branching

```clojure
(-> (kafka/stream "orders")
    (kafka/branch!
      [[#(= (:region %) "us") "us-orders"]
       [#(= (:region %) "eu") "eu-orders"]
       [(constantly true) "other-orders"]]))
```

### Aggregations with reduce

```clojure
(-> (kafka/stream "orders")
    (kafka/group-by :customer-id)
    (kafka/reduce-stream 0 #(+ %1 (:amount %2)))
    (kafka/for-each!
      (fn [[customer-id total]]
        (println "Customer" customer-id "total: $" total))))
```

## Topic Administration

```clojure
;; Create topic
(kafka/create-topic! "orders"
  {:partitions 3
   :retention-ms (* 7 24 60 60 1000)}) ;; 7 days

;; List topics
(doseq [topic (kafka/list-topics)]
  (println (:name topic) ":" (:partitions topic) "partitions"))

;; Describe topic
(let [info (kafka/describe-topic "orders")]
  (println "Partitions:" (:partitions info))
  (println "Retention:" (:retention-ms info) "ms"))

;; Alter topic
(kafka/alter-topic! "orders"
  {:retention-ms (* 30 24 60 60 1000)}) ;; 30 days

;; Delete topic
(kafka/delete-topic! "old-events")
```

## Consumer Groups

```clojure
;; List groups
(doseq [group (kafka/list-groups)]
  (println "Group:" (:id group) ", Members:" (:member-count group)))

;; Describe group
(let [info (kafka/describe-group "order-processor")]
  (println "State:" (:state info))
  (println "Members:" (count (:members info)))
  (println "Total Lag:" (:total-lag info)))

;; Reset offsets
(kafka/reset-offsets! "order-processor" "orders" :earliest)

;; Reset to timestamp
(kafka/reset-offsets! "order-processor" "orders" [:timestamp yesterday])
```

## Error Handling

```clojure
(try
  (kafka/produce! "orders" order)
  (catch clojure.lang.ExceptionInfo e
    (let [data (ex-data e)]
      (case (:type data)
        :topic-not-found
        (println "Topic not found")

        :message-too-large
        (println "Message too large")

        :timeout
        (println "Request timed out")

        ;; Default
        (do
          (println "Kafka error:" (:code data) "-" (:message data))
          (when (:retriable? data)
            ;; Safe to retry
            (retry-with-backoff #(kafka/produce! "orders" order))))))))
```

### Error Types

```clojure
;; Exception info data contains:
;; :type - one of:
;;   :topic-not-found
;;   :partition-not-found
;;   :message-too-large
;;   :not-leader
;;   :offset-out-of-range
;;   :group-coordinator
;;   :rebalance-in-progress
;;   :unauthorized
;;   :quota-exceeded
;;   :timeout
;;   :disconnected
;;   :serialization
;;
;; :retriable? - boolean indicating if retry is safe
;; :code - error code string
;; :message - human-readable message
```

### Dead Letter Queue Pattern

```clojure
(kafka/consume! "orders" "processor"
  (fn [record]
    (try
      (process-order (:value record))
      :commit
      (catch Exception e
        (kafka/produce! "orders-dlq"
          {:original-record (:value record)
           :error (.getMessage e)
           :timestamp (java.time.Instant/now)}
          {:headers {"original-topic" (:topic record)
                     "original-partition" (str (:partition record))
                     "original-offset" (str (:offset record))}})
        :commit)))) ;; Commit to avoid reprocessing
```

### Retry with Exponential Backoff

```clojure
(defn with-retry
  [f & {:keys [max-attempts base-delay]
        :or {max-attempts 3 base-delay 1000}}]
  (loop [attempt 1]
    (let [result (try
                   {:ok (f)}
                   (catch clojure.lang.ExceptionInfo e
                     (if (and (:retriable? (ex-data e))
                              (< attempt max-attempts))
                       {:retry true :error e}
                       {:error e})))]
      (cond
        (:ok result) (:ok result)
        (:retry result)
        (do
          (let [delay (* base-delay (Math/pow 2 (dec attempt)))
                jitter (rand-int (/ delay 10))]
            (Thread/sleep (+ delay jitter)))
          (recur (inc attempt)))
        :else (throw (:error result))))))

(with-retry #(kafka/produce! "orders" order))
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Application Configuration

```clojure
;; resources/config.edn
{:kafka {:url "https://kafka.do"
         :api-key #env KAFKA_DO_API_KEY
         :timeout 30000
         :retries 3}

 :producer {:batch-size 16384
            :linger-ms 5
            :compression :gzip
            :acks :all
            :retries 3
            :retry-backoff-ms 100}

 :consumer {:offset :latest
            :auto-commit false
            :fetch-min-bytes 1
            :fetch-max-wait-ms 500
            :max-poll-records 500
            :session-timeout 30000
            :heartbeat-interval 3000}}
```

## Component/Mount Integration

### Component

```clojure
(ns myapp.kafka
  (:require [com.stuartsierra.component :as component]
            [com.dotdo.kafka :as kafka]))

(defrecord KafkaComponent [config client]
  component/Lifecycle
  (start [this]
    (assoc this :client (kafka/client config)))
  (stop [this]
    (when client
      (kafka/close! client))
    (assoc this :client nil)))

(defn new-kafka [config]
  (map->KafkaComponent {:config config}))
```

### Mount

```clojure
(ns myapp.kafka
  (:require [mount.core :refer [defstate]]
            [com.dotdo.kafka :as kafka]))

(defstate kafka-client
  :start (kafka/client (get-in config [:kafka]))
  :stop (kafka/close! kafka-client))
```

## Testing

### Mock Client

```clojure
(ns myapp.test.kafka
  (:require [clojure.test :refer :all]
            [com.dotdo.kafka.testing :as mock]))

(deftest process-orders-test
  (let [kafka (mock/mock-client)]
    ;; Seed test data
    (mock/seed! kafka "orders"
      [{:order-id "123" :amount 99.99}
       {:order-id "124" :amount 149.99}])

    ;; Process
    (let [processed (atom [])]
      (doseq [record (take 2 (kafka/consumer-seq kafka "orders" "test"))]
        (swap! processed conj (:value record))
        (kafka/commit! record))

      (is (= 2 (count @processed)))
      (is (= "123" (-> @processed first :order-id))))))

(deftest producer-test
  (let [kafka (mock/mock-client)]
    (kafka/produce! kafka "orders" {:order-id "125"})

    (let [messages (mock/get-messages kafka "orders")]
      (is (= 1 (count messages)))
      (is (= "125" (-> messages first :order-id))))))
```

### Integration Testing

```clojure
(ns myapp.integration-test
  (:require [clojure.test :refer :all]
            [com.dotdo.kafka :as kafka]))

(def ^:dynamic *kafka* nil)
(def ^:dynamic *topic* nil)

(use-fixtures :once
  (fn [f]
    (binding [*kafka* (kafka/client
                        {:url (System/getenv "TEST_KAFKA_URL")
                         :api-key (System/getenv "TEST_KAFKA_API_KEY")})
              *topic* (str "test-orders-" (java.util.UUID/randomUUID))]
      (try
        (f)
        (finally
          (kafka/delete-topic! *kafka* *topic*))))))

(deftest ^:integration end-to-end-test
  ;; Produce
  (kafka/produce! *kafka* *topic* {:order-id "123"})

  ;; Consume
  (let [record (first (kafka/consumer-seq *kafka* *topic* "test"))]
    (is (= "123" (-> record :value :order-id)))))
```

## API Reference

### Core Functions

```clojure
;; Client
(kafka/client config) ;; => Client

;; Producing
(kafka/produce! topic value) ;; => RecordMetadata
(kafka/produce! topic value opts) ;; => RecordMetadata
(kafka/produce-async! topic value) ;; => Promise<RecordMetadata>
(kafka/produce-batch! topic values) ;; => [RecordMetadata]

;; Consuming
(kafka/consume! topic group handler) ;; => nil (blocking)
(kafka/consume! topic group opts handler) ;; => nil (blocking)
(kafka/consumer-seq topic group) ;; => LazySeq<Record>
(kafka/consume-batch! topic group opts handler) ;; => nil (blocking)

;; Transactions
(kafka/with-transaction topic f) ;; => result of f

;; Streams
(kafka/stream topic) ;; => Stream
(kafka/filter-stream pred stream) ;; => Stream
(kafka/map-stream f stream) ;; => Stream
(kafka/to-topic! stream topic) ;; => nil

;; Admin
(kafka/create-topic! topic config) ;; => nil
(kafka/list-topics) ;; => [Topic]
(kafka/describe-topic topic) ;; => TopicInfo
(kafka/alter-topic! topic config) ;; => nil
(kafka/delete-topic! topic) ;; => nil
(kafka/list-groups) ;; => [Group]
(kafka/describe-group group) ;; => GroupInfo
(kafka/reset-offsets! group topic offset) ;; => nil

;; Utility
(kafka/commit! record) ;; => nil
(kafka/close! client) ;; => nil
```

### Record Structure

```clojure
{:topic "orders"
 :partition 0
 :offset 123
 :key "customer-456"
 :value {:order-id "123" :amount 99.99}
 :timestamp #inst "2024-01-01T00:00:00Z"
 :headers {"correlation-id" "abc-123"}}
```

## License

MIT
