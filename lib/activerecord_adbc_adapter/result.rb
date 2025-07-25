module ActiveRecordADBCAdapter
  class Result
    include Enumerable

    def initialize(table)
      @table = table
      @schema = @table.schema
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
      @table
    end

    def each_record_batch
      return to_enum(__method__) unless block_given?

      reader = Arrow::TableBatchReader.new(@table)
      loop do
        record_batch = reader.read_next
        break if record_batch.nil?
        yield(record_batch)
      end
    end

    private
    def fields
      @fields ||= @schema.fields
    end

    def resolve_type(data_type)
      case data_type
      when Arrow::Int32DataType
        ActiveRecord::Type::Integer.new(limit: 4)
      when Arrow::Int64DataType
        ActiveRecord::Type::Integer.new(limit: 8)
      when Arrow::FloatDataType
        ActiveRecord::Type::Float.new(limit: 24)
      when Arrow::DoubleDataType
        ActiveRecord::Type::Float.new
      when Arrow::BinaryDataType
        ActiveRecord::Type::Binary.new
      when Arrow::StringDataType
        ActiveRecord::Type::String.new
      when Arrow::Date32DataType
        ActiveRecord::Type::Date.new
      when Arrow::TimestampDataType
        ActiveRecord::Type::DateTime.new
      else
        raise "Unknown: #{data_type.inspect}"
      end
    end
  end
end
