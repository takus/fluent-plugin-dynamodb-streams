require "test_helper"

class DynamoDBStreamsInputTest < Test::Unit::TestCase
  include DynamoDBStreamsTestHelper

  def setup
    Fluent::Test.setup
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
        tag         test
        aws_key_id  dummy
        aws_sec_key dummy
        aws_region  ddblocal
        stream_arn  #{@stream_arn}
      ]
    )

    d.run(expect_emits: 2) do
      put_records([
        {key:"k1", timestamp:time_ms, bool:true, hash:{k:"v"}, l:[{k:"v"}], ns:[1,2,3], ss:["1","2","3"]},
        {key:"k2", timestamp:time_ms},
      ])
    end

    emits = d.events

    assert_equal(2, emits.size)

    assert_equal("test", emits[0][0])
    assert_equal(
      {"key" => "k1", "timestamp" => time_ms, "bool" => true, "hash" => {"k" => "v"}, "l" => [{"k" => "v"}], "ns" => [1,2,3], "ss" => ["1","2","3"]},
      emits[0][2]["dynamodb"]["new_image"],
    )

    assert_equal("test", emits[1][0])
    assert_equal({"key" => "k2", "timestamp" => time_ms}, emits[1][2]["dynamodb"]["new_image"])

    assert_equal("ddblocal", emits[0][2]["aws_region"])
    assert_true(emits[0][2]["event_id"].size > 0)
    assert_equal("aws:dynamodb", emits[0][2]["event_source"])
    assert_true(emits[0][2]["event_version"].size > 0)
    assert_equal("INSERT", emits[0][2]["event_name"])
    assert_true(emits[0][2]["dynamodb"]["sequence_number"].size > 0)
    assert_equal("NEW_AND_OLD_IMAGES", emits[0][2]["dynamodb"]["stream_view_type"])
    assert_equal(78, emits[0][2]["dynamodb"]["size_bytes"])

    delete_table
  end

  private
  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::DynamoDBStreamsInput).configure(conf)
  end

end
