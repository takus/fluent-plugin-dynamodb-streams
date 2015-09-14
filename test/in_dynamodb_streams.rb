require "test_helper"

class DynamoDBStreamsInputTest < Test::Unit::TestCase

  TEST_TABLE_NAME = "in_dynamodb_streams"

  def setup
    Fluent::Test.setup

    @ddb = Aws::DynamoDB::Client.new(
      region: 'ap-northeast-1',
      endpoint: 'http://localhost:8000',
    )
  end

  def create_driver(conf = CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::DynamoDBStreamsInput).configure(conf)
  end

  def create_table
    @stream_arn = @ddb.create_table({
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
      @ddb.put_item(
        table_name: TEST_TABLE_NAME,
        item: r,
      )
    end
  end

  def delete_table
    @ddb.list_tables.table_names.each do |t|
      if t == TEST_TABLE_NAME
        @ddb.delete_table({
          table_name: TEST_TABLE_NAME,
        })
      end
    end
  end

  def test_configure
    d = create_driver(
      %[
        tag             streams
        aws_key_id      test_key_id
        aws_sec_key     test_sec_key
        aws_region      ap-northeast-1
        stream_arn      arn:aws:dynamodb:ap-northeast-1:123456789012:table/fluent-plugin-dynamodb-streams/stream/2015-09-01T00:00:00.000
        pos_file        /tmp/fluent-plugin-dynamodb-streams.pos
        fetch_interval  5
        fetch_size      100
      ]
    )

    assert_equal 'test_key_id', d.instance.aws_key_id
    assert_equal 'test_sec_key', d.instance.aws_sec_key
    assert_equal 'ap-northeast-1', d.instance.aws_region
    assert_equal 'arn:aws:dynamodb:ap-northeast-1:123456789012:table/fluent-plugin-dynamodb-streams/stream/2015-09-01T00:00:00.000', d.instance.stream_arn
    assert_equal '/tmp/fluent-plugin-dynamodb-streams.pos', d.instance.pos_file
    assert_equal 5.0, d.instance.fetch_interval
    assert_equal 100, d.instance.fetch_size
  end

  def test_emit
    delete_table
    create_table

    time_ms = (Time.now.to_f * 1000).floor

    d = create_driver(
      %[
        tag        test
        aws_region ddblocal
        stream_arn #{@stream_arn}
      ]
    )

    d.run do
      sleep 1
      put_records([
        {key: "k1", timestamp: time_ms},
        {key: "k2", timestamp: time_ms},
      ])
      sleep 1
    end

    emits = d.emits

    assert_equal(2, emits.size)

    # Expected output:
    # {
    #   "aws_region"=>"ddblocal",
    #   "dynamodb"=>
    #    {"keys"=>{"key"=>"k2"},
    #     "new_image"=>{"key"=>"k2", "timestamp"=>1442225594551},
    #     "sequence_number"=>"000000000000000000081",
    #     "size_bytes"=>27,
    #     "stream_view_type"=>"NEW_AND_OLD_IMAGES"},
    #   "event_id"=>"a5d8b042-e83f-40a6-8e58-580a10d976f3",
    #   "event_name"=>"INSERT",
    #   "event_source"=>"aws:dynamodb",
    #   "event_version"=>"1.0"
    # }

    assert_equal("test", emits[0][0])
    assert_equal({"key" => "k1", "timestamp" => time_ms}, emits[0][2]["dynamodb"]["new_image"])

    assert_equal("test", emits[1][0])
    assert_equal({"key" => "k2", "timestamp" => time_ms}, emits[1][2]["dynamodb"]["new_image"])

    assert_equal("ddblocal", emits[0][2]["aws_region"])
    assert_true(emits[0][2]["event_id"].size > 0)
    assert_equal("aws:dynamodb", emits[0][2]["event_source"])
    assert_true(emits[0][2]["event_version"].size > 0)
    assert_equal("INSERT", emits[0][2]["event_name"])
    assert_true(emits[0][2]["dynamodb"]["sequence_number"].size > 0)
    assert_equal("NEW_AND_OLD_IMAGES", emits[0][2]["dynamodb"]["stream_view_type"])
    assert_equal(27, emits[0][2]["dynamodb"]["size_bytes"])

    delete_table
  end
end
