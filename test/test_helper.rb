require "codeclimate-test-reporter"
CodeClimate::TestReporter.start

require 'fluent/test'
require 'fluent/plugin/in_dynamodb_streams'
require 'aws-sdk'
