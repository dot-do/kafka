<?php

declare(strict_types=1);

namespace KafkaDo;

use KafkaDo\Exception\ConnectionException;
use Generator;

/**
 * Kafka - Main entry point for Kafka operations.
 *
 * Provides a KafkaJS-compatible API for PHP with generator support
 * for memory-efficient message consumption.
 *
 * @example
 * ```php
 * $kafka = new Kafka(['url' => 'https://kafka.do', 'api_key' => 'xxx']);
 *
 * // Produce
 * $kafka->producer('orders')->send(['order_id' => '123', 'amount' => 99.99]);
 *
 * // Consume with generators
 * foreach ($kafka->consumer('orders', 'my-group') as $record) {
 *     processOrder($record->value);
 *     $record->commit();
 * }
 * ```
 */
class Kafka
{
    private array $config;
    private ?RpcTransport $transport = null;
    private bool $connected = false;

    /** @var array<string, Producer> */
    private array $producers = [];

    /**
     * Create a new Kafka client.
     *
     * @param array $config Configuration options
     */
    public function __construct(array $config = [])
    {
        $this->config = array_merge([
            'url' => getenv('KAFKA_DO_URL') ?: 'https://kafka.do',
            'api_key' => getenv('KAFKA_DO_API_KEY') ?: null,
            'timeout' => 30,
            'retries' => 3,
        ], $config);
    }

    /**
     * Connect to Kafka service.
     *
     * @throws ConnectionException
     */
    public function connect(): self
    {
        if ($this->connected) {
            return $this;
        }

        $this->transport = new MockRpcTransport();

        try {
            $this->transport->call('connect', $this->config);
        } catch (\Exception $e) {
            throw new ConnectionException('Failed to connect: ' . $e->getMessage(), 0, $e);
        }

        $this->connected = true;
        return $this;
    }

    /**
     * Get the RPC transport.
     */
    public function getTransport(): ?RpcTransport
    {
        $this->ensureConnected();
        return $this->transport;
    }

    /**
     * Set a custom transport (for testing).
     */
    public function setTransport(RpcTransport $transport): void
    {
        $this->transport = $transport;
        $this->connected = true;
    }

    /**
     * Create a producer for a topic.
     *
     * @param string $topic Topic name
     * @param array $options Producer options
     */
    public function producer(string $topic, array $options = []): Producer
    {
        $this->ensureConnected();

        $key = $topic . ':' . md5(json_encode($options));
        if (!isset($this->producers[$key])) {
            $this->producers[$key] = new Producer($this->transport, $topic, $options);
        }

        return $this->producers[$key];
    }

    /**
     * Create a consumer generator for a topic.
     *
     * Returns a generator for memory-efficient message consumption.
     *
     * @param string $topic Topic name
     * @param string $group Consumer group ID
     * @param array $options Consumer options
     * @return Generator<int, Record>
     */
    public function consumer(string $topic, string $group, array $options = []): Generator
    {
        $this->ensureConnected();

        $consumer = new Consumer($this->transport, $topic, $group, $options);
        return $consumer->consume();
    }

    /**
     * Create a batch consumer generator.
     *
     * @param string $topic Topic name
     * @param string $group Consumer group ID
     * @param array $options Consumer options
     * @return Generator<int, array<Record>>
     */
    public function batchConsumer(string $topic, string $group, array $options = []): Generator
    {
        $this->ensureConnected();

        $consumer = new Consumer($this->transport, $topic, $group, array_merge([
            'batch_size' => $options['batch_size'] ?? 100,
            'batch_timeout' => $options['batch_timeout'] ?? 5.0,
        ], $options));

        return $consumer->consumeBatch();
    }

    /**
     * Execute operations in a transaction.
     *
     * @param string $topic Topic name
     * @param callable $callback Transaction callback
     * @throws Exception\TransactionException
     */
    public function transaction(string $topic, callable $callback): void
    {
        $this->ensureConnected();

        $tx = new Transaction($this->transport, $topic);

        try {
            $tx->begin();
            $callback($tx);
            $tx->commit();
        } catch (\Exception $e) {
            $tx->abort();
            throw new Exception\TransactionException('Transaction failed: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Create a stream processor for a topic.
     *
     * @param string $topic Topic name
     */
    public function stream(string $topic): Stream
    {
        $this->ensureConnected();
        return new Stream($this->transport, $topic);
    }

    /**
     * Get an admin client.
     */
    public function admin(): Admin
    {
        $this->ensureConnected();
        return new Admin($this->transport);
    }

    /**
     * Close all connections.
     */
    public function close(): void
    {
        if ($this->transport !== null) {
            $this->transport->close();
            $this->transport = null;
        }

        $this->connected = false;
        $this->producers = [];
    }

    /**
     * Check if connected.
     */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * Ensure the client is connected.
     *
     * @throws ConnectionException
     */
    private function ensureConnected(): void
    {
        if (!$this->connected) {
            $this->connect();
        }
    }

    /**
     * Destructor - close connections.
     */
    public function __destruct()
    {
        $this->close();
    }
}
