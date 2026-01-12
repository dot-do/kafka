module Kafka
  # Main Kafka client for connecting to the .do platform
  class Client
    getter config : Config
    @http_client : HTTP::Client?
    @connected : Bool = false

    # Create a new client with default configuration
    def initialize
      @config = Config.new
      @http_client = create_http_client
      @connected = true
    end

    # Create a new client with custom configuration
    def initialize(@config : Config)
      @http_client = create_http_client
      @connected = true
    end

    # Create a producer for a specific topic
    #
    # Example:
    #   producer = client.producer("orders")
    #   producer.send(Order.new("123", 99.99))
    def producer(topic : String, config : ProducerConfig = ProducerConfig.default) : Producer(JSON::Any)
      Producer(JSON::Any).new(self, topic, config)
    end

    # Create a typed producer for a specific topic
    #
    # Example:
    #   producer = client.producer(Order, "orders")
    #   producer.send(Order.new("123", 99.99))
    def producer(type : T.class, topic : String, config : ProducerConfig = ProducerConfig.default) : Producer(T) forall T
      Producer(T).new(self, topic, config)
    end

    # Create a consumer for a topic
    #
    # Example:
    #   for record in client.consume("orders", group: "my-processor")
    #     puts record.value
    #     record.commit
    #   end
    def consume(topic : String, *, group : String, config : ConsumerConfig = ConsumerConfig.default) : Consumer
      Consumer.new(self, topic, group, config)
    end

    # Create a consumer alias
    def consumer(topic : String, *, group : String, config : ConsumerConfig = ConsumerConfig.default) : Consumer
      consume(topic, group: group, config: config)
    end

    # Create a batch consumer
    #
    # Example:
    #   for batch in client.consume_batch("orders", group: "processor", config: batch_config)
    #     batch.each { |record| process(record) }
    #     batch.commit
    #   end
    def consume_batch(topic : String, *, group : String, config : BatchConfig = BatchConfig.default) : BatchConsumer
      BatchConsumer.new(self, topic, group, config)
    end

    # Execute a transaction
    #
    # Example:
    #   client.transaction("orders") do |tx|
    #     tx.send(Order.new("123", status: :created))
    #     tx.send(Order.new("123", status: :validated))
    #   end
    def transaction(topic : String, &) : Array(RecordMetadata)
      tx = Transaction(JSON::Any).new(self, topic)
      begin
        yield tx
        tx.commit
      rescue ex
        tx.abort
        raise ex
      end
    end

    # Create a stream for the given topic
    #
    # Example:
    #   client.stream("orders")
    #     .filter { |o| o["amount"].as_f > 100 }
    #     .map { |o| o["customer_id"] }
    #     .to("high-value-customers")
    def stream(topic : String) : Stream(JSON::Any)
      Stream(JSON::Any).new(self, topic)
    end

    # Get admin client
    def admin : Admin
      Admin.new(self)
    end

    # Check if connected
    def connected? : Bool
      @connected
    end

    # Close the client
    def close : Nil
      @http_client.try(&.close)
      @http_client = nil
      @connected = false
    end

    # Produce a message directly (convenience method)
    #
    # Example:
    #   client.produce("orders", Order.new("123", 99.99))
    def produce(topic : String, value, *, key : String? = nil, headers : Hash(String, String) = {} of String => String) : RecordMetadata
      producer(topic).send(
        case value
        when JSON::Any
          value
        else
          JSON.parse(value.to_json)
        end,
        key: key,
        headers: headers
      )
    end

    # Commit an offset (internal use by Record)
    def commit_offset(topic : String, partition : Int32, offset : Int64) : Nil
      rpc_call("consumer.commit", {
        "topic"     => topic,
        "partition" => partition,
        "offset"    => offset,
      })
    end

    # Internal: Make an RPC call to the server
    def rpc_call(method : String, params : Hash(String, _) | NamedTuple = {} of String => String) : JSON::Any
      raise ConnectionError.new("Client is not connected") unless @connected

      client = @http_client
      raise ConnectionError.new("HTTP client not initialized") unless client

      body = {
        "jsonrpc" => "2.0",
        "method"  => method,
        "params"  => params,
        "id"      => Random.new.hex(8),
      }.to_json

      headers = @config.headers

      response = client.post("/rpc", headers: headers, body: body)

      unless response.success?
        handle_http_error(response)
      end

      result = JSON.parse(response.body)

      if error = result["error"]?
        handle_rpc_error(error)
      end

      result["result"]? || JSON::Any.new(nil)
    end

    # Create the HTTP client
    private def create_http_client : HTTP::Client
      parsed = URI.parse(@config.url)
      host = parsed.host || "localhost"
      port = parsed.port

      client = if parsed.scheme == "https"
                 HTTP::Client.new(host, port || 443, tls: true)
               else
                 HTTP::Client.new(host, port || 80)
               end

      client.connect_timeout = @config.timeout
      client.read_timeout = @config.timeout
      client
    end

    # Handle HTTP errors
    private def handle_http_error(response : HTTP::Client::Response) : NoReturn
      case response.status_code
      when 401, 403
        raise UnauthorizedError.new("Authentication failed: #{response.status_code}")
      when 404
        raise ConnectionError.new("Endpoint not found")
      when 408, 504
        raise TimeoutError.new("Request timed out")
      when 429
        raise QuotaExceededError.new("Rate limit exceeded")
      when 500..599
        raise ConnectionError.new("Server error: #{response.status_code}")
      else
        raise Error.new("HTTP error: #{response.status_code} - #{response.body}")
      end
    end

    # Handle RPC errors
    private def handle_rpc_error(error : JSON::Any) : NoReturn
      code = error["code"]?.try(&.as_s) || "UNKNOWN"
      message = error["message"]?.try(&.as_s) || "Unknown error"

      case code
      when "TOPIC_NOT_FOUND"
        raise TopicNotFoundError.new(message)
      when "MESSAGE_TOO_LARGE"
        raise MessageTooLargeError.new(0_i64, 0_i64)
      when "UNAUTHORIZED"
        raise UnauthorizedError.new(message)
      when "TIMEOUT"
        raise TimeoutError.new(message)
      when "QUOTA_EXCEEDED"
        raise QuotaExceededError.new(message)
      when "REBALANCE_IN_PROGRESS"
        raise RebalanceInProgressError.new
      else
        raise Error.new(message, code)
      end
    end
  end

  # Module-level helper to create a client
  def self.connect(url : String? = nil, api_key : String? = nil) : Client
    config = Config.new
    config.url = url if url
    config.api_key = api_key if api_key
    Client.new(config)
  end

  # Create client from environment
  def self.connect : Client
    Client.new
  end
end
