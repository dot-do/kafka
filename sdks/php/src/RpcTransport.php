<?php

declare(strict_types=1);

namespace KafkaDo;

/**
 * RpcTransport - Interface for RPC communication.
 */
interface RpcTransport
{
    /**
     * Make an RPC call.
     *
     * @param string $method Method name
     * @param mixed ...$args Method arguments
     * @return mixed Call result
     */
    public function call(string $method, mixed ...$args): mixed;

    /**
     * Close the transport.
     */
    public function close(): void;

    /**
     * Check if the transport is closed.
     */
    public function isClosed(): bool;
}
