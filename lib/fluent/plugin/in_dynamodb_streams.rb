require 'fluent/input'
module Fluent
  class DynamoDBStreamsInput < Input
    Fluent::Plugin.register_input('dynamodb_streams', self)

    # Define `router` method of v0.12 to support v0.10 or earlier
    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def initialize
      super
      require 'aws-sdk'
      require 'bigdecimal'
    end

    config_param :tag, :string
    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true
    config_param :aws_region, :string, :default => "ap-northeast-1"
    config_param :stream_arn, :string
    config_param :fetch_interval, :time, :default => 1
    config_param :fetch_size, :integer, :default => 1
    config_param :pos_file, :string, :default => nil

    def configure(conf)
      super

      if @aws_region == "ddblocal"
        @aws_region = "ap-northeast-1" # dummy settings
        @stream_endpoint = "http://localhost:8000"
      else
        @stream_endpoint = "https://streams.dynamodb.#{@aws_region}.amazonaws.com"
      end

      unless @pos_file
        log.warn "dynamodb-streams: 'pos_file PATH' parameter is not set to a 'dynamodb-streams' source."
        log.warn "dynamodb-streams: this parameter is highly recommended to save the position to resume."
      end
    end

    def start
      super

      unless @pos_file
        @pos_memory = {}
      end

      options = {}
      options[:region] = @aws_region if @aws_region
      options[:credentials] = Aws::Credentials.new(@aws_key_id, @aws_sec_key) if @aws_key_id && @aws_sec_key
      options[:endpoint] = @stream_endpoint
      @client = Aws::DynamoDBStreams::Client.new(options)

      @iterator = {}

      @running = true
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @running = false
      @thread.join
    end

    def run
      while @running
        sleep @fetch_interval

        get_shards.each do |s|
          if s.sequence_number_range.ending_sequence_number
            remove_sequence(s.shard_id)
            next
          end

          set_iterator(s.shard_id) unless @iterator.key? s.shard_id

          resp = @client.get_records({
            shard_iterator: @iterator[s.shard_id],
            limit: @fetch_size,
          })

          resp.records.each do |r|
            begin
              emit(r)
            rescue => e
              log.error "dynamodb-streams: error has occoured.", error: e.message, error_class: e.class
            end
            save_sequence(s.shard_id, r.dynamodb.sequence_number)
          end

          if resp.next_shard_iterator
            @iterator[s.shard_id] = resp.next_shard_iterator
          else
            @iterator.delete s.shard_id
          end
        end
      end
    end

    def get_shards()
      shards = []

      last_shard_id = nil
      begin
        s = @client.describe_stream({
          stream_arn: @stream_arn,
          exclusive_start_shard_id: last_shard_id,
        }).stream_description

        shards = shards + s.shards

        if s.last_evaluated_shard_id == last_shard_id then
          break
        end
        last_shard_id = s.last_evaluated_shard_id
      end while last_shard_id

      shards
    end

    def set_iterator(shard_id)
      if load_sequence(shard_id)
        @iterator[shard_id] = @client.get_shard_iterator({
          stream_arn: @stream_arn,
          shard_id: shard_id,
          shard_iterator_type: "AFTER_SEQUENCE_NUMBER",
          sequence_number: load_sequence(shard_id),
        }).shard_iterator
      else
        @iterator[shard_id] = @client.get_shard_iterator({
          stream_arn: @stream_arn,
          shard_id: shard_id,
          shard_iterator_type: "TRIM_HORIZON",
        }).shard_iterator
      end
    end

    def load_sequence(shard_id)
      if @pos_file
        return nil unless File.exist?("#{@pos_file}.#{shard_id}")
        File.read("#{@pos_file}.#{shard_id}").chomp
      else
        return nil unless @pos_memory[shard_id]
        @pos_memory[shard_id]
      end
    end

    def save_sequence(shard_id, sequence)
      if @pos_file
        open("#{@pos_file}.#{shard_id}", 'w') do |f|
          f.write sequence
        end
      else
        @pos_memory[shard_id] = sequence
      end
      sequence
    end

    def remove_sequence(shard_id)
      if @pos_file
        return unless File.exist?("#{@pos_file}.#{shard_id}")
        File.unlink("#{@pos_file}.#{shard_id}")
      else
        @pos_memory[shard_id] = nil
      end
    end

    def emit(r)
      record = {
        "aws_region" => r.aws_region,
        "event_source" => r.event_source,
        "event_version" => r.event_version,
        "event_id" => r.event_id,
        "event_name" => r.event_name,
        "dynamodb" => {
          "stream_view_type" => r.dynamodb.stream_view_type,
          "sequence_number" => r.dynamodb.sequence_number,
          "size_bytes" => r.dynamodb.size_bytes,
        }
      }
      record["dynamodb"]["keys"] = dynamodb_to_hash(r.dynamodb.keys) if r.dynamodb.keys
      record["dynamodb"]["old_image"] = dynamodb_to_hash(r.dynamodb.old_image) if r.dynamodb.old_image
      record["dynamodb"]["new_image"] = dynamodb_to_hash(r.dynamodb.new_image) if r.dynamodb.new_image
      router.emit(@tag, Time.now.to_i, record)
    end

    def dynamodb_to_hash(hash)
      hash.each do |k, v|
        # delete binary attributes
        if v.b || v.bs
          hash.delete(k)
        else
          hash[k] = format_attribute_value(v)
        end
      end
      return hash
    end

    def format_attribute_value(v)
      if v.m
        return dynamodb_to_hash(v.m)
      elsif v.l
        return v.l.map {|i| format_attribute_value(i) }
      elsif v.ns
        return v.ns.map {|i| BigDecimal.new(i).to_i }
      elsif v.ss
        return v.ss
      elsif v.null
        return null
      elsif v.bool
        return v.bool
      elsif v.n
        return BigDecimal.new(v.n).to_i
      elsif v.s
        return v.s
      else
        log.warn "dynamodb-streams: unknown attribute value."
      end
    end

  end
end
