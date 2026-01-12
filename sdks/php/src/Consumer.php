<?php

declare(strict_types=1);

namespace KafkaDo;

use KafkaDo\Exception\ConsumerException;
use Generator;
use DateTimeImmutable;

/**
 * Consumer - Kafka message consumer with generator support.
 *
 * Provides memory-efficient message consumption using PHP generators.
 *
 * @example
 * ```php
 * // Using the Kafka class helper
 * foreach ($kafka->consumer('orders', 'my-group') as $record) {
 *     processOrder($record->value);
 *     $record->commit();
 * }
 *
 * // With options
 * $consumer = $kafka->consumer('orders', 'my-group', [
 *     'offset' => 'earliest',
 *     'auto_commit' => false,
 *     'max_poll_records' => 100,
 * ]);
 * ```
 */
class Consumer
{
    private RpcTransport $transport;
    private string $topic;
    private string $group;
    private array $options;
    private bool $subscribed = false;
    private bool $closed = false;
    private int $currentOffset = 0;

    /**
     * Create a new Consumer.
     *
     * @param RpcTransport $transport RPC transport
     * @param string $topic Topic name
     * @param string $group Consumer group ID
     * @param array $options Consumer options
     */
    public function __construct(RpcTransport $transport, string $topic, string $group, array $options = [])
    {
        $this->transport = $transport;
        $this->topic = $topic;
        $this->group = $group;
        $this->options = array_merge([
            'offset' => 'latest',
            'auto_commit' => false,
            'max_poll_records' => 100,
            'poll_timeout_ms' => 1000,
            'session_timeout_ms' => 30000,
            'heartbeat_interval_ms' => 3000,
        ], $options);
    }

    /**
     * Get the topic name.
     */
    public function getTopic(): string
    {
        return $this->topic;
    }

    /**
     * Get the consumer group ID.
     */
    public function getGroup(): string
    {
        return $this->group;
    }

    /**
     * Subscribe to the topic.
     *
     * @throws ConsumerException
     */
    public function subscribe(): void
    {
        if ($this->subscribed) {
            return;
        }

        try {
            $this->transport->call('consumer.subscribe', $this->topic, $this->group, $this->options);
            $this->subscribed = true;
        } catch (\Exception $e) {
            throw new ConsumerException('Failed to subscribe: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Unsubscribe from the topic.
     */
    public function unsubscribe(): void
    {
        if (!$this->subscribed) {
            return;
        }

        $this->transport->call('consumer.unsubscribe', $this->topic, $this->group);
        $this->subscribed = false;
    }

    /**
     * Consume messages as a generator.
     *
     * This is the primary method for consuming messages. It returns a generator
     * that yields Record objects one at a time, providing memory efficiency
     * for high-volume consumption.
     *
     * @return Generator<int, Record>
     * @throws ConsumerException
     */
    public function consume(): Generator
    {
        $this->subscribe();

        while (!$this->closed) {
            try {
                $records = $this->transport->call('consumer.poll', $this->topic, $this->group, [
                    'max_records' => $this->options['max_poll_records'],
                    'timeout_ms' => $this->options['poll_timeout_ms'],
                    'auto_commit' => $this->options['auto_commit'],
                ]);

                if (empty($records)) {
                    // No messages, continue polling
                    continue;
                }

                foreach ($records as $data) {
                    $record = $this->createRecord($data);
                    $this->currentOffset = $record->offset;
                    yield $record;
                }
            } catch (\Exception $e) {
                throw new ConsumerException('Poll failed: ' . $e->getMessage(), 0, $e);
            }
        }
    }

    /**
     * Consume messages in batches.
     *
     * @return Generator<int, RecordBatch>
     * @throws ConsumerException
     */
    public function consumeBatch(): Generator
    {
        $this->subscribe();

        $batchSize = $this->options['batch_size'] ?? 100;
        $batchTimeout = $this->options['batch_timeout'] ?? 5.0;

        while (!$this->closed) {
            $batch = [];
            $startTime = microtime(true);

            while (count($batch) < $batchSize) {
                $elapsed = microtime(true) - $startTime;
                if ($elapsed >= $batchTimeout && !empty($batch)) {
                    break;
                }

                try {
                    $records = $this->transport->call('consumer.poll', $this->topic, $this->group, [
                        'max_records' => $batchSize - count($batch),
                        'timeout_ms' => (int) (($batchTimeout - $elapsed) * 1000),
                        'auto_commit' => false,
                    ]);

                    if (empty($records)) {
                        if (!empty($batch)) {
                            break;
                        }
                        continue;
                    }

                    foreach ($records as $data) {
                        $batch[] = $this->createRecord($data);
                    }
                } catch (\Exception $e) {
                    throw new ConsumerException('Batch poll failed: ' . $e->getMessage(), 0, $e);
                }
            }

            if (!empty($batch)) {
                yield new RecordBatch($this, $batch);
            }
        }
    }

    /**
     * Commit offsets.
     *
     * @param array|null $offsets Specific offsets to commit, or null for current position
     * @throws ConsumerException
     */
    public function commit(?array $offsets = null): void
    {
        try {
            $this->transport->call('consumer.commit', $this->topic, $this->group, $offsets ?? []);
        } catch (\Exception $e) {
            throw new ConsumerException('Commit failed: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Seek to a specific offset.
     *
     * @param int $offset Target offset
     * @param int $partition Partition number
     */
    public function seek(int $offset, int $partition = 0): void
    {
        $this->transport->call('consumer.seek', $this->topic, $this->group, $partition, $offset);
        $this->currentOffset = $offset;
    }

    /**
     * Seek to the beginning of the topic.
     */
    public function seekToBeginning(): void
    {
        $this->transport->call('consumer.seekToBeginning', $this->topic, $this->group);
        $this->currentOffset = 0;
    }

    /**
     * Seek to the end of the topic.
     */
    public function seekToEnd(): void
    {
        $this->transport->call('consumer.seekToEnd', $this->topic, $this->group);
    }

    /**
     * Get the current position (offset).
     */
    public function position(): int
    {
        return $this->transport->call('consumer.position', $this->group, $this->topic);
    }

    /**
     * Get the committed offset.
     */
    public function committed(): int
    {
        return $this->transport->call('consumer.committed', $this->group, $this->topic);
    }

    /**
     * Close the consumer.
     */
    public function close(): void
    {
        $this->closed = true;
        $this->unsubscribe();
    }

    /**
     * Check if the consumer is closed.
     */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * Create a Record from raw data.
     */
    private function createRecord(array $data): Record
    {
        return new Record(
            consumer: $this,
            topic: $data['topic'],
            partition: $data['partition'],
            offset: $data['offset'],
            key: $data['key'],
            value: $data['value'],
            timestamp: new DateTimeImmutable($data['timestamp']),
            headers: $data['headers'] ?? [],
        );
    }
}
