package do_.kafka;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for the underlying RPC transport.
 * This is implemented by the kafka.do RPC client but can be mocked for testing.
 */
public interface RpcClient extends AutoCloseable {

    /**
     * Calls a remote method with the given arguments.
     *
     * @param method The method name to call
     * @param args   The arguments to pass to the method
     * @return A CompletableFuture that will complete with the result
     */
    CompletableFuture<Object> call(String method, Object... args);

    /**
     * Closes the RPC client and releases resources.
     */
    @Override
    void close();

    /**
     * Returns true if the client is connected.
     */
    boolean isConnected();
}
