# Package

version       = "0.1.0"
author        = ".do"
description   = "Kafka SDK for the .do platform - Event Streaming for Nim"
license       = "MIT"
srcDir        = "src"

# Dependencies

requires "nim >= 2.0.0"
requires "ws >= 0.5.0"

# Tasks

task test, "Run tests":
  exec "nim c -r tests/test_kafka_do.nim"
