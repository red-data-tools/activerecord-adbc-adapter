module ActiveRecordADBCAdapter
  class Result
    include Enumerable

    def initialize(backend, table)
      @backend = backend
      @table = table
      @schema = @table.schema
    end

    # This must be called before calling other methods.
    def attach_model(model)
      return unless @backend == "sqlite"

      model_columns_hash = model.columns_hash
      casted = false
      new_chunked_arrays = []
      new_fields = []
      @table.columns.zip(@schema.fields) do |column, field|
        chunked_array = nil
        model_column = model_columns_hash[field.name]
        if model_column
          casted_type = nil
          case model_column.sql_type_metadata.type
          when :boolean
            case field.data_type
            when Arrow::IntegerDataType
              casted_type = Arrow::BooleanDataType.new
            end
          when :date
            case field.data_type
            when Arrow::StringDataType
              casted_type = Arrow::Date32DataType.new
            end
          when :datetime
            case field.data_type
            when Arrow::StringDataType
              casted_type = Arrow::TimestampDataType.new(:nano)
            end
          when :time
            case field.data_type
            when Arrow::StringDataType
              # Parse time strings from SQLite's format (2000-01-01 HH:MM:SS.SSSSSS) and convert to Time64Array
              # Format is defined in Rails' SQLite3 adapter:
              # https://github.com/rails/rails/blob/90a1eaa1b30ba1f2d524e197460e549c03cf5698/activerecord/lib/active_record/connection_adapters/sqlite3/quoting.rb#L74-L77
              chunks = column.data.chunks.collect do |chunk|
                ruby_array = chunk.collect do |time_str|
                  if time_str.nil?
                    nil
                  else
                    dt = time_str.to_time
                    (dt.seconds_since_midnight * 1_000_000).to_i
                  end
                end
                Arrow::Time64Array.new(:micro, ruby_array)
              end
              chunked_array = Arrow::ChunkedArray.new(chunks)
              field = Arrow::Field.new(field.name, Arrow::Time64DataType.new(:micro))
              casted = true
            end
          end
          if casted_type
            chunked_array = column.cast(casted_type)
            field = Arrow::Field.new(field.name, casted_type)
            casted = true
          end
        end
        new_chunked_arrays << (chunked_array || column.data)
        new_fields << field
      end
      return unless casted

      @schema = Arrow::Schema.new(new_fields)
      @table = Arrow::Table.new(@schema, new_chunked_arrays)
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

    def cast_values(type_overrides = {})
      # TODO: type_overrides support
      if fields.size == 1
        rows.map(&:first)
      else
        rows
      end
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
      when Arrow::BooleanDataType
        ActiveRecord::Type::Boolean.new
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
      when Arrow::Time64DataType
        ActiveRecord::Type::Time.new
      else
        raise "Unknown: #{data_type.inspect}"
      end
    end
  end
end
