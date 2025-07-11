module ActiveRecordADBCAdapter
  class Result
    include Enumerable

    def initialize(statement, record_batch_reader)
      @statement = statement
      @record_batch_reader = record_batch_reader
      @schema = @record_batch_reader.schema
    end

    def columns
      @columns ||= fields.collect(&:name)
    end

    def column_types
      @column_types ||= fields.inject({}) do |types, field|
        types[field.name] = resolve_type(field.data_type)
        types
      end
    end

    def includes_column?(name)
      columns.include?(name)
    end

    def rows
      @rows ||= to_arrow.raw_records
    end

    def length
      to_arrow.length
    end

    def empty?
      length.zero?
    end

    def each(&block)
      return to_enum(__method__) unless block_given?

      rows.each do |record|
        yield(Hash[@columns.zip(record)])
      end
    end

    def indexed_rows
      @indexed_rows ||= to_a
    end

    def to_arrow
      @table ||= consume_record_batch_reader do
        @record_batch_reader.read_all
      end
    end

    def each_record_batch
      return to_enum(__method__) unless block_given?

      consume_record_batch_reader do
        loop do
          record_batch = @record_batch_reader.read_next
          break if record_batch.nil?
          yield(record_batch)
        end
      end
    end

    private
    def fields
      @fields ||= @schema.fields
    end

    def consume_record_batch_reader
      begin
        yield
      ensure
        @record_batch_reader = nil
        @statement.release
        @statement = nil
      end
    end

    def resolve_type(data_type)
      case data_type
      when Arrow::Int32DataType
        ActiveRecord::Type::Integer.new(limit: 4)
      when Arrow::Int64DataType
        ActiveRecord::Type::Integer.new(limit: 8)
      when Arrow::StringDataType
        ActiveRecord::Type::String.new
      else
        raise "Unknown: #{data_type.inspect}"
      end
    end
  end
end
