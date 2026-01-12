<?php

declare(strict_types=1);

namespace KafkaDo;

use KafkaDo\Exception\TransportException;
use DateTimeImmutable;

/**
 * MockRpcTransport - In-memory mock transport for testing.
 *
 * Implements an in-memory Kafka-like message store for testing
 * without requiring an actual Kafka cluster.
 */
class MockRpcTransport implements RpcTransport
{
    /** @var array<string, array<array<string, mixed>>> */
    private array $topics = [];

    /** @var array<string, array<string, int>> */
    private array $offsets = [];

    /** @var array<string, array{partitions: int, retention_ms: int}> */
    private array $topicConfigs = [];

    /** @var array<string, array<string, array{members: int, lag: int}>> */
    private array $consumerGroups = [];

    private bool $closed = false;
    private int $nextOffset = 0;

    /** @var array<array{method: string, args: array<mixed>}> */
    private array $callLog = [];

    /**
     * Get the call log for testing.
     *
     * @return array<array{method: string, args: array<mixed>}>
     */
    public function getCallLog(): array
    {
        return $this->callLog;
    }

    /**
     * Clear the call log.
     */
    public function clearCallLog(): void
    {
        $this->callLog = [];
    }

    /**
     * Seed messages for testing.
     *
     * @param string $topic Topic name
     * @param array<array<string, mixed>> $messages Messages to seed
     */
    public function seed(string $topic, array $messages): void
    {
        if (!isset($this->topics[$topic])) {
            $this->topics[$topic] = [];
        }

        foreach ($messages as $message) {
            $this->topics[$topic][] = [
                'topic' => $topic,
                'partition' => $message['partition'] ?? 0,
                'offset' => $this->nextOffset++,
                'key' => $message['key'] ?? null,
                'value' => $message['value'] ?? $message,
                'timestamp' => $message['timestamp'] ?? (new DateTimeImmutable())->format('c'),
                'headers' => $message['headers'] ?? [],
            ];
        }
    }

    /**
     * Get all messages for a topic.
     *
     * @param string $topic Topic name
     * @return array<array<string, mixed>>
     */
    public function getMessages(string $topic): array
    {
        return $this->topics[$topic] ?? [];
    }

    /**
     * {@inheritdoc}
     */
    public function call(string $method, mixed ...$args): mixed
    {
        if ($this->closed) {
            throw new TransportException('Transport is closed');
        }

        $this->callLog[] = ['method' => $method, 'args' => $args];

        return match ($method) {
            'connect' => ['ok' => 1],
            'ping' => ['ok' => 1],

            // Producer methods
            'producer.send' => $this->handleProducerSend($args[0], $args[1]),
            'producer.sendBatch' => $this->handleProducerSendBatch($args[0], $args[1]),
            'producer.flush' => ['ok' => 1],

            // Consumer methods
            'consumer.subscribe' => $this->handleConsumerSubscribe($args[0], $args[1], $args[2] ?? []),
            'consumer.unsubscribe' => ['ok' => 1],
            'consumer.poll' => $this->handleConsumerPoll($args[0], $args[1], $args[2] ?? []),
            'consumer.commit' => $this->handleConsumerCommit($args[0], $args[1], $args[2] ?? []),
            'consumer.seek' => ['ok' => 1],
            'consumer.seekToBeginning' => ['ok' => 1],
            'consumer.seekToEnd' => ['ok' => 1],
            'consumer.position' => $this->getOffset($args[0], $args[1]),
            'consumer.committed' => $this->getOffset($args[0], $args[1]),
            'consumer.pause' => ['ok' => 1],
            'consumer.resume' => ['ok' => 1],

            // Admin methods
            'admin.createTopic' => $this->handleCreateTopic($args[0], $args[1] ?? []),
            'admin.deleteTopic' => $this->handleDeleteTopic($args[0]),
            'admin.listTopics' => $this->handleListTopics(),
            'admin.describeTopic' => $this->handleDescribeTopic($args[0]),
            'admin.alterTopic' => $this->handleAlterTopic($args[0], $args[1]),
            'admin.listGroups' => $this->handleListGroups(),
            'admin.describeGroup' => $this->handleDescribeGroup($args[0]),
            'admin.resetOffsets' => $this->handleResetOffsets($args[0], $args[1], $args[2]),

            // Transaction methods
            'tx.begin' => ['ok' => 1],
            'tx.commit' => ['ok' => 1],
            'tx.abort' => ['ok' => 1],
            'tx.send' => $this->handleProducerSend($args[0], $args[1]),

            default => throw new TransportException("Unknown method: {$method}"),
        };
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        $this->closed = true;
    }

    /**
     * {@inheritdoc}
     */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    // =========================================================================
    // Private handler methods
    // =========================================================================

    private function handleProducerSend(string $topic, array $message): array
    {
        if (!isset($this->topics[$topic])) {
            $this->topics[$topic] = [];
        }

        $partition = $message['partition'] ?? 0;
        $offset = $this->nextOffset++;

        $record = [
            'topic' => $topic,
            'partition' => $partition,
            'offset' => $offset,
            'key' => $message['key'] ?? null,
            'value' => $message['value'],
            'timestamp' => (new DateTimeImmutable())->format('c'),
            'headers' => $message['headers'] ?? [],
        ];

        $this->topics[$topic][] = $record;

        return [
            'topic' => $topic,
            'partition' => $partition,
            'offset' => $offset,
            'timestamp' => $record['timestamp'],
        ];
    }

    private function handleProducerSendBatch(string $topic, array $messages): array
    {
        $results = [];
        foreach ($messages as $message) {
            $results[] = $this->handleProducerSend($topic, $message);
        }
        return $results;
    }

    private function handleConsumerSubscribe(string $topic, string $group, array $options): array
    {
        if (!isset($this->consumerGroups[$group])) {
            $this->consumerGroups[$group] = [];
        }
        if (!isset($this->consumerGroups[$group][$topic])) {
            $this->consumerGroups[$group][$topic] = ['members' => 1, 'lag' => 0];
        }

        // Initialize offset if not set
        if (!isset($this->offsets[$group][$topic])) {
            $this->offsets[$group][$topic] = match ($options['offset'] ?? 'latest') {
                'earliest' => 0,
                'latest' => count($this->topics[$topic] ?? []),
                default => 0,
            };
        }

        return ['ok' => 1];
    }

    private function handleConsumerPoll(string $topic, string $group, array $options): array
    {
        $messages = $this->topics[$topic] ?? [];
        $offset = $this->offsets[$group][$topic] ?? 0;
        $maxRecords = $options['max_records'] ?? 100;

        $results = [];
        for ($i = $offset; $i < count($messages) && count($results) < $maxRecords; $i++) {
            $results[] = $messages[$i];
        }

        // Update offset
        if (!empty($results) && ($options['auto_commit'] ?? false)) {
            $this->offsets[$group][$topic] = $offset + count($results);
        }

        return $results;
    }

    private function handleConsumerCommit(string $topic, string $group, array $offsets): array
    {
        if (empty($offsets)) {
            // Commit current position
            $currentOffset = $this->offsets[$group][$topic] ?? 0;
            $this->offsets[$group][$topic] = $currentOffset + 1;
        } else {
            foreach ($offsets as $partition => $offset) {
                $this->offsets[$group][$topic] = $offset;
            }
        }

        return ['ok' => 1];
    }

    private function getOffset(string $group, string $topic): int
    {
        return $this->offsets[$group][$topic] ?? 0;
    }

    private function handleCreateTopic(string $name, array $options): array
    {
        $this->topicConfigs[$name] = [
            'partitions' => $options['partitions'] ?? 1,
            'retention_ms' => $options['retention_ms'] ?? 604800000,
        ];
        $this->topics[$name] = $this->topics[$name] ?? [];

        return ['ok' => 1];
    }

    private function handleDeleteTopic(string $name): array
    {
        unset($this->topics[$name], $this->topicConfigs[$name]);
        return ['ok' => 1];
    }

    private function handleListTopics(): array
    {
        return array_map(
            fn($name) => [
                'name' => $name,
                'partitions' => $this->topicConfigs[$name]['partitions'] ?? 1,
            ],
            array_keys($this->topics)
        );
    }

    private function handleDescribeTopic(string $name): array
    {
        return [
            'name' => $name,
            'partitions' => $this->topicConfigs[$name]['partitions'] ?? 1,
            'retentionMs' => $this->topicConfigs[$name]['retention_ms'] ?? 604800000,
            'messageCount' => count($this->topics[$name] ?? []),
        ];
    }

    private function handleAlterTopic(string $name, array $config): array
    {
        if (isset($this->topicConfigs[$name])) {
            $this->topicConfigs[$name] = array_merge($this->topicConfigs[$name], $config);
        }
        return ['ok' => 1];
    }

    private function handleListGroups(): array
    {
        return array_map(
            fn($name, $topics) => [
                'id' => $name,
                'memberCount' => array_sum(array_column($topics, 'members')),
            ],
            array_keys($this->consumerGroups),
            $this->consumerGroups
        );
    }

    private function handleDescribeGroup(string $name): array
    {
        $topics = $this->consumerGroups[$name] ?? [];
        return [
            'id' => $name,
            'state' => 'Stable',
            'members' => array_sum(array_column($topics, 'members')),
            'totalLag' => array_sum(array_column($topics, 'lag')),
        ];
    }

    private function handleResetOffsets(string $group, string $topic, mixed $offset): array
    {
        if ($offset === 'earliest') {
            $this->offsets[$group][$topic] = 0;
        } elseif ($offset === 'latest') {
            $this->offsets[$group][$topic] = count($this->topics[$topic] ?? []);
        } elseif ($offset instanceof DateTimeImmutable) {
            // Find first message after timestamp
            $messages = $this->topics[$topic] ?? [];
            foreach ($messages as $i => $msg) {
                if (new DateTimeImmutable($msg['timestamp']) >= $offset) {
                    $this->offsets[$group][$topic] = $i;
                    break;
                }
            }
        } else {
            $this->offsets[$group][$topic] = (int) $offset;
        }

        return ['ok' => 1];
    }
}
