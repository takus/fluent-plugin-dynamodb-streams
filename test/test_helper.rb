require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_dynamodb_streams'
require 'aws-sdk-dynamodb'
require 'aws-sdk-dynamodbstreams'

module DynamoDBStreamsTestHelper

  TEST_TABLE_NAME = "in_dynamodb_streams"

  private
  def dynamodb
    @ddb ||= Aws::DynamoDB::Client.new(
      region: 'ap-northeast-1',
      access_key_id: 'dummy',
      secret_access_key: 'dummy',
      endpoint: 'http://localhost:8000',
    )
  end

  def create_table
    @stream_arn = dynamodb.create_table({
      table_name: TEST_TABLE_NAME,
      key_schema: [
        {
          attribute_name: "key",
          key_type: "HASH",
        }
      ],
      attribute_definitions: [
        {
          attribute_name: "key",
          attribute_type: "S",
        }
      ],
      provisioned_throughput: {
        read_capacity_units: 1,
        write_capacity_units: 1,
      },
      stream_specification: {
        stream_enabled: true,
        stream_view_type: "NEW_AND_OLD_IMAGES",
      }
    }).table_description.latest_stream_arn
  end
  
  def put_records(records)
    records.each do |r|
      dynamodb.put_item(
        table_name: TEST_TABLE_NAME,
        item: r,
      )
    end
  end
  
  def delete_table
    dynamodb.list_tables.table_names.each do |t|
      if t == TEST_TABLE_NAME
        dynamodb.delete_table({
          table_name: TEST_TABLE_NAME,
        })
      end
    end
  end
end
