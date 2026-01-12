# frozen_string_literal: true

require 'kafka_do'

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.example_status_persistence_file_path = '.rspec_status'
  config.disable_monkey_patching!

  config.order = :random
  Kernel.srand config.seed
end

# Mock RPC Promise that returns a predefined result
class MockRpcPromise
  def initialize(result = nil, error: nil)
    @result = result
    @error = error
  end

  def await
    raise @error if @error

    @result
  end
end

# Mock RPC Client for testing
class MockRpcClient
  attr_reader :calls

  def initialize
    @calls = []
    @responses = {}
    @closed = false
  end

  # Set up a mock response for a specific method
  def mock_response(method, result = nil, error: nil)
    @responses[method] = { result: result, error: error }
  end

  def call(method, *args)
    @calls << { method: method, args: args }

    response = @responses[method] || { result: nil }
    MockRpcPromise.new(response[:result], error: response[:error])
  end

  def close
    @closed = true
  end

  def closed?
    @closed
  end

  # Find calls matching a method name
  def calls_for(method)
    @calls.select { |c| c[:method] == method }
  end

  # Get the last call for a method
  def last_call_for(method)
    calls_for(method).last
  end
end

# Helper to create a configured Kafka client with mock RPC
def create_mock_kafka(brokers = ['broker1:9092'])
  kafka = KafkaDo::Kafka.new(brokers)
  mock_client = MockRpcClient.new
  kafka.rpc_client = mock_client
  [kafka, mock_client]
end
