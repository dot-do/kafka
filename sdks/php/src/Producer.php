<?php

declare(strict_types=1);

namespace KafkaDo;

use KafkaDo\Exception\ProducerException;

/**
 * Producer - Kafka message producer.
 *
 * Sends messages to a Kafka topic with optional key-based partitioning
 * and header support.
 *
 * @example
 * ```php
 * $producer = $kafka->producer('orders');
 *
 * // Send simple message
 * $producer->send(['order_id' => '123', 'amount' => 99.99]);
 *
 * // Send with key for partitioning
 * $producer->send(
 *     value: ['order_id' => '123'],
 *     key: 'customer-456'
 * );
 *
 * // Send batch
 * $producer->sendBatch([
 *     ['value' => ['order_id' => '124']],
 *     ['value' => ['order_id' => '125'], 'key' => 'cust-1'],
 * ]);
 * ```
 */
class Producer
{
    private RpcTransport $transport;
    private string $topic;
    private array $options;

    /**
     * Create a new Producer.
     *
     * @param RpcTransport $transport RPC transport
     * @param string $topic Topic name
     * @param array $options Producer options
     */
    public function __construct(RpcTransport $transport, string $topic, array $options = [])
    {
        $this->transport = $transport;
        $this->topic = $topic;
        $this->options = array_merge([
            'batch_size' => 16384,
            'linger_ms' => 5,
            'compression' => 'none',
            'acks' => 'all',
            'retries' => 3,
            'retry_backoff_ms' => 100,
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
     * Send a single message.
     *
     * @param array $value Message value (will be JSON encoded)
     * @param string|null $key Message key for partitioning
     * @param array $headers Message headers
     * @param int|null $partition Specific partition (overrides key-based partitioning)
     * @return RecordMetadata
     * @throws ProducerException
     */
    public function send(
        array $value,
        ?string $key = null,
        array $headers = [],
        ?int $partition = null,
    ): RecordMetadata {
        $message = [
            'value' => $value,
            'key' => $key,
            'headers' => $headers,
        ];

        if ($partition !== null) {
            $message['partition'] = $partition;
        }

        try {
            $result = $this->transport->call('producer.send', $this->topic, $message);

            return new RecordMetadata(
                topic: $result['topic'],
                partition: $result['partition'],
                offset: $result['offset'],
                timestamp: new \DateTimeImmutable($result['timestamp']),
            );
        } catch (\Exception $e) {
            throw new ProducerException('Failed to send message: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Send multiple messages in a batch.
     *
     * @param array<array> $messages Array of messages
     * @return array<RecordMetadata>
     * @throws ProducerException
     */
    public function sendBatch(array $messages): array
    {
        $normalizedMessages = [];
        foreach ($messages as $message) {
            if (isset($message['value'])) {
                $normalizedMessages[] = $message;
            } else {
                // Treat the whole array as the value
                $normalizedMessages[] = ['value' => $message];
            }
        }

        try {
            $results = $this->transport->call('producer.sendBatch', $this->topic, $normalizedMessages);

            return array_map(
                fn($result) => new RecordMetadata(
                    topic: $result['topic'],
                    partition: $result['partition'],
                    offset: $result['offset'],
                    timestamp: new \DateTimeImmutable($result['timestamp']),
                ),
                $results
            );
        } catch (\Exception $e) {
            throw new ProducerException('Failed to send batch: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Flush pending messages.
     *
     * @param float|null $timeout Timeout in seconds
     */
    public function flush(?float $timeout = null): void
    {
        $this->transport->call('producer.flush', $timeout);
    }
}
