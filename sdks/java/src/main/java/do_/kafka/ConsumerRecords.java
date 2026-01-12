package do_.kafka;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A container that holds the list of ConsumerRecord per partition for a particular topic.
 * There is one ConsumerRecords per poll of the consumer.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {

    private static final ConsumerRecords<?, ?> EMPTY = new ConsumerRecords<>(Collections.emptyMap());

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private final int count;

    /**
     * Creates ConsumerRecords from a map of partition to record list.
     */
    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        this.records = records != null ? new HashMap<>(records) : new HashMap<>();
        this.count = this.records.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Returns an empty ConsumerRecords.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> ConsumerRecords<K, V> empty() {
        return (ConsumerRecords<K, V>) EMPTY;
    }

    /**
     * Creates ConsumerRecords from a list of records.
     */
    public static <K, V> ConsumerRecords<K, V> fromList(List<ConsumerRecord<K, V>> recordList) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> map = new HashMap<>();
        for (ConsumerRecord<K, V> record : recordList) {
            TopicPartition tp = record.topicPartition();
            map.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
        }
        return new ConsumerRecords<>(map);
    }

    /**
     * Returns the records for the given partition.
     */
    public List<ConsumerRecord<K, V>> records(TopicPartition partition) {
        List<ConsumerRecord<K, V>> list = records.get(partition);
        return list != null ? Collections.unmodifiableList(list) : Collections.emptyList();
    }

    /**
     * Returns the records for the given topic.
     */
    public Iterable<ConsumerRecord<K, V>> records(String topic) {
        List<ConsumerRecord<K, V>> result = new ArrayList<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : records.entrySet()) {
            if (entry.getKey().topic().equals(topic)) {
                result.addAll(entry.getValue());
            }
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Returns the set of partitions with data in this record set.
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(records.keySet());
    }

    /**
     * Returns the number of records for all topics.
     */
    public int count() {
        return count;
    }

    /**
     * Returns true if this record set contains no records.
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * Returns a stream of all records.
     */
    public Stream<ConsumerRecord<K, V>> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return new Iterator<>() {
            private final Iterator<List<ConsumerRecord<K, V>>> partitionIterator = records.values().iterator();
            private Iterator<ConsumerRecord<K, V>> currentIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                while (!currentIterator.hasNext() && partitionIterator.hasNext()) {
                    currentIterator = partitionIterator.next().iterator();
                }
                return currentIterator.hasNext();
            }

            @Override
            public ConsumerRecord<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentIterator.next();
            }
        };
    }

    @Override
    public String toString() {
        return "ConsumerRecords{" +
                "partitions=" + records.keySet() +
                ", count=" + count +
                '}';
    }
}
