package do_.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for KafkaException and its subclasses.
 */
class KafkaExceptionTest {

    // =========================================================================
    // KafkaException Tests
    // =========================================================================

    @Test
    void shouldCreateWithMessage() {
        KafkaException ex = new KafkaException("Something went wrong");

        assertThat(ex.getMessage()).isEqualTo("Something went wrong");
        assertThat(ex.code()).isNull();
        assertThat(ex.isRetriable()).isFalse();
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void shouldCreateWithMessageAndCause() {
        Exception cause = new RuntimeException("Root cause");
        KafkaException ex = new KafkaException("Wrapper", cause);

        assertThat(ex.getMessage()).isEqualTo("Wrapper");
        assertThat(ex.getCause()).isEqualTo(cause);
    }

    @Test
    void shouldCreateWithCodeAndMessage() {
        KafkaException ex = new KafkaException("TOPIC_NOT_FOUND", "Topic not found: my-topic");

        assertThat(ex.code()).isEqualTo("TOPIC_NOT_FOUND");
        assertThat(ex.getMessage()).contains("TOPIC_NOT_FOUND");
        assertThat(ex.getMessage()).contains("Topic not found: my-topic");
    }

    @Test
    void shouldCreateWithAllFields() {
        Exception cause = new RuntimeException("Root");
        KafkaException ex = new KafkaException("TIMEOUT", "Request timed out", true, cause);

        assertThat(ex.code()).isEqualTo("TIMEOUT");
        assertThat(ex.getMessage()).contains("TIMEOUT");
        assertThat(ex.isRetriable()).isTrue();
        assertThat(ex.getCause()).isEqualTo(cause);
    }

    @Test
    void shouldFormatMessageWithCode() {
        KafkaException ex = new KafkaException("ERR_CODE", "Error message");

        assertThat(ex.getMessage()).isEqualTo("[ERR_CODE] Error message");
    }

    @Test
    void shouldNotFormatMessageWhenCodeIsNull() {
        KafkaException ex = new KafkaException(null, "Error message", false, null);

        assertThat(ex.getMessage()).isEqualTo("Error message");
    }

    @Test
    void shouldNotFormatMessageWhenCodeIsEmpty() {
        KafkaException ex = new KafkaException("", "Error message", false, null);

        assertThat(ex.getMessage()).isEqualTo("Error message");
    }

    @Test
    void shouldNotBeConnectionError() {
        KafkaException ex = new KafkaException("Some error");

        assertThat(ex.isConnectionError()).isFalse();
    }

    @Test
    void shouldNotBeTimeoutError() {
        KafkaException ex = new KafkaException("Some error");

        assertThat(ex.isTimeoutError()).isFalse();
    }

    // =========================================================================
    // ConnectionException Tests
    // =========================================================================

    @Test
    void shouldCreateConnectionException() {
        ConnectionException ex = new ConnectionException("Failed to connect");

        assertThat(ex.getMessage()).isEqualTo("Failed to connect");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    @Test
    void connectionExceptionShouldBeConnectionError() {
        ConnectionException ex = new ConnectionException("Failed");

        assertThat(ex.isConnectionError()).isTrue();
    }

    // =========================================================================
    // TimeoutException Tests
    // =========================================================================

    @Test
    void shouldCreateTimeoutException() {
        TimeoutException ex = new TimeoutException("Request timed out");

        assertThat(ex.getMessage()).isEqualTo("Request timed out");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    @Test
    void timeoutExceptionShouldBeTimeoutError() {
        TimeoutException ex = new TimeoutException("Timed out");

        assertThat(ex.isTimeoutError()).isTrue();
    }

    // =========================================================================
    // TopicNotFoundException Tests
    // =========================================================================

    @Test
    void shouldCreateTopicNotFoundException() {
        TopicNotFoundException ex = new TopicNotFoundException("my-topic");

        assertThat(ex.topic()).isEqualTo("my-topic");
        assertThat(ex.getMessage()).contains("my-topic");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    // =========================================================================
    // TopicAlreadyExistsException Tests
    // =========================================================================

    @Test
    void shouldCreateTopicAlreadyExistsException() {
        TopicAlreadyExistsException ex = new TopicAlreadyExistsException("existing-topic");

        assertThat(ex.topic()).isEqualTo("existing-topic");
        assertThat(ex.getMessage()).contains("existing-topic");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    // =========================================================================
    // SerializationException Tests
    // =========================================================================

    @Test
    void shouldCreateSerializationException() {
        SerializationException ex = new SerializationException("Failed to serialize");

        assertThat(ex.getMessage()).isEqualTo("Failed to serialize");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    @Test
    void shouldCreateSerializationExceptionWithCause() {
        Exception cause = new RuntimeException("Encoding error");
        SerializationException ex = new SerializationException("Failed", cause);

        assertThat(ex.getCause()).isEqualTo(cause);
    }

    // =========================================================================
    // PartitionNotFoundException Tests
    // =========================================================================

    @Test
    void shouldCreatePartitionNotFoundException() {
        TopicPartition tp = new TopicPartition("topic", 5);
        PartitionNotFoundException ex = new PartitionNotFoundException(tp);

        assertThat(ex.partition()).isEqualTo(tp);
        assertThat(ex.getMessage()).contains("topic");
        assertThat(ex.getMessage()).contains("5");
    }

    // =========================================================================
    // MessageTooLargeException Tests
    // =========================================================================

    @Test
    void shouldCreateMessageTooLargeException() {
        MessageTooLargeException ex = new MessageTooLargeException("Message exceeds max size");

        assertThat(ex.getMessage()).isEqualTo("Message exceeds max size");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    @Test
    void shouldCreateMessageTooLargeExceptionWithSize() {
        MessageTooLargeException ex = new MessageTooLargeException(1048576, 524288);

        assertThat(ex.messageSize()).isEqualTo(1048576);
        assertThat(ex.maxSize()).isEqualTo(524288);
        assertThat(ex.getMessage()).contains("1048576");
        assertThat(ex.getMessage()).contains("524288");
    }

    // =========================================================================
    // OffsetOutOfRangeException Tests
    // =========================================================================

    @Test
    void shouldCreateOffsetOutOfRangeException() {
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetOutOfRangeException ex = new OffsetOutOfRangeException(tp, 100L);

        assertThat(ex.partition()).isEqualTo(tp);
        assertThat(ex.offset()).isEqualTo(100L);
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    @Test
    void shouldCreateOffsetOutOfRangeExceptionWithMessage() {
        OffsetOutOfRangeException ex = new OffsetOutOfRangeException("Offset out of bounds");

        assertThat(ex.getMessage()).contains("Offset out of bounds");
        assertThat(ex.partition()).isNull();
        assertThat(ex.offset()).isEqualTo(-1);
    }

    // =========================================================================
    // UnauthorizedException Tests
    // =========================================================================

    @Test
    void shouldCreateUnauthorizedException() {
        UnauthorizedException ex = new UnauthorizedException("Access denied");

        assertThat(ex.getMessage()).isEqualTo("Access denied");
        assertThat(ex).isInstanceOf(KafkaException.class);
    }

    // =========================================================================
    // Exception Inheritance Tests
    // =========================================================================

    @Test
    void allExceptionsShouldExtendKafkaException() {
        assertThat(new ConnectionException("")).isInstanceOf(KafkaException.class);
        assertThat(new TimeoutException("")).isInstanceOf(KafkaException.class);
        assertThat(new TopicNotFoundException("t")).isInstanceOf(KafkaException.class);
        assertThat(new TopicAlreadyExistsException("t")).isInstanceOf(KafkaException.class);
        assertThat(new SerializationException("")).isInstanceOf(KafkaException.class);
        assertThat(new PartitionNotFoundException("t", 0)).isInstanceOf(KafkaException.class);
        assertThat(new MessageTooLargeException("")).isInstanceOf(KafkaException.class);
        assertThat(new OffsetOutOfRangeException("t", 0, 0L)).isInstanceOf(KafkaException.class);
        assertThat(new UnauthorizedException("")).isInstanceOf(KafkaException.class);
    }

    @Test
    void allExceptionsShouldExtendRuntimeException() {
        assertThat(new KafkaException("")).isInstanceOf(RuntimeException.class);
    }
}
