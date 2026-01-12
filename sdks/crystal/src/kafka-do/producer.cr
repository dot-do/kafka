module Kafka
  # Producer for sending messages to Kafka topics
  class Producer(T)
    getter topic : String
    getter config : ProducerConfig
    @client : Client

    def initialize(@client : Client, @topic : String, @config : ProducerConfig = ProducerConfig.default)
    end

    # Send a single message
    #
    # Example:
    #   producer.send(Order.new("123", 99.99))
    #   producer.send(order, key: "customer-456")
    def send(
      value : T,
      *,
      key : String? = nil,
      headers : Hash(String, String) = {} of String => String
    ) : RecordMetadata
      response = @client.rpc_call("producer.send", {
        "topic"   => @topic,
        "key"     => key,
        "value"   => serialize(value),
        "headers" => headers,
        "acks"    => @config.acks.value,
      })

      RecordMetadata.from_json(response)
    end

    # Send a batch of messages
    #
    # Example:
    #   producer.send_batch([order1, order2, order3])
    def send_batch(values : Array(T)) : Array(RecordMetadata)
      messages = values.map { |v| {"value" => serialize(v)} }
      do_send_batch(messages)
    end

    # Send a batch of messages with keys and headers
    #
    # Example:
    #   producer.send_batch([
    #     Kafka::Message.new(key: "cust-1", value: order1),
    #     Kafka::Message.new(key: "cust-2", value: order2),
    #   ])
    def send_batch(messages : Array(Message(T))) : Array(RecordMetadata)
      message_data = messages.map do |m|
        {
          "key"     => m.key,
          "value"   => serialize(m.value),
          "headers" => m.headers,
        }
      end
      do_send_batch(message_data)
    end

    # Internal batch send
    private def do_send_batch(messages : Array) : Array(RecordMetadata)
      response = @client.rpc_call("producer.sendBatch", {
        "topic"    => @topic,
        "messages" => messages,
        "acks"     => @config.acks.value,
      })

      results = response["results"]?.try(&.as_a) || [] of JSON::Any
      results.map { |r| RecordMetadata.from_json(r) }
    end

    # Serialize value to JSON
    private def serialize(value : T) : JSON::Any
      case value
      when JSON::Any
        value
      else
        JSON.parse(value.to_json)
      end
    end
  end

  # Transaction for atomic message sending
  class Transaction(T)
    @client : Client
    @topic : String
    @messages : Array(Hash(String, JSON::Any | String | Nil | Hash(String, String))) = [] of Hash(String, JSON::Any | String | Nil | Hash(String, String))
    @committed : Bool = false

    def initialize(@client : Client, @topic : String)
    end

    # Add a message to the transaction
    def send(
      value : T,
      *,
      key : String? = nil,
      headers : Hash(String, String) = {} of String => String
    ) : Nil
      raise TransactionError.new("Transaction already committed") if @committed

      @messages << {
        "key"     => key,
        "value"   => serialize(value),
        "headers" => headers,
      }
    end

    # Commit the transaction
    def commit : Array(RecordMetadata)
      raise TransactionError.new("Transaction already committed") if @committed

      response = @client.rpc_call("producer.transaction", {
        "topic"    => @topic,
        "messages" => @messages,
      })

      @committed = true

      results = response["results"]?.try(&.as_a) || [] of JSON::Any
      results.map { |r| RecordMetadata.from_json(r) }
    end

    # Abort the transaction
    def abort : Nil
      @messages.clear
      @committed = true
    end

    private def serialize(value : T) : JSON::Any
      case value
      when JSON::Any
        value
      else
        JSON.parse(value.to_json)
      end
    end
  end
end
