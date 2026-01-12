# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name          = 'kafka.do'
  spec.version       = '0.1.0'
  spec.authors       = ['DotDo Team']
  spec.email         = ['team@dotdo.dev']

  spec.summary       = 'Kafka.do - Event Streaming for Ruby'
  spec.description   = 'Kafka client SDK for Ruby with blocks, Enumerables, and zero ops. ' \
                       'Provides producers, consumers with each_message, and admin operations.'
  spec.homepage      = 'https://github.com/dotdo/kafka'
  spec.license       = 'MIT'
  spec.required_ruby_version = '>= 2.6.0'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/dotdo/kafka'
  spec.metadata['changelog_uri'] = 'https://github.com/dotdo/kafka/blob/main/CHANGELOG.md'

  spec.files = Dir.chdir(__dir__) do
    Dir.glob('{lib,spec}/**/*') + ['kafka.do.gemspec', 'Gemfile', 'Rakefile']
  end.select { |f| File.file?(File.join(__dir__, f)) }

  spec.bindir = 'exe'
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'json', '~> 2.0'

  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'rubocop', '~> 1.0'
end
