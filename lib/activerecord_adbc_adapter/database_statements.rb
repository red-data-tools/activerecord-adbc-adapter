module ActiveRecordADBCAdapter
  module DatabaseStatements
    def perform_query(raw_connection,
                      sql,
                      binds,
                      type_casted_binds,
                      prepare:,
                      notification_payload:,
                      batch:)
      raw_connection.open_statement do |statement|
        statement.sql_query = sql
        if binds.empty?
          statement.execute[0]
        else
          statement.prepare
          raw_records = {}
          binds.zip(type_casted_binds).each_with_index do |(bind, type_casted_bind), i|
            case type_casted_bind
            when String
              if type_casted_bind.encoding == Encoding::ASCII_8BIT
                array = Arrow::BinaryArray.new([type_casted_bind])
              else
                array = Arrow::StringArray.new([type_casted_bind])
              end
            when DateTime
              array = Arrow::TimestampArray.new(:micro,
                                                [type_casted_bind.dup.localtime])
            when Date
              array = Arrow::Date32Array.new([type_casted_bind])
            when ActiveRecord::Type::Time::Value
              local_time = type_casted_bind.dup.localtime
              time_value = (local_time.seconds_since_midnight * 1_000_000).to_i
              array = Arrow::Time64Array.new(:micro, [time_value])
            else
              array = [type_casted_bind]
            end
            # Some binds (e.g. BETWEEN range values) aren't QueryAttributes and
            # have no #name. Position `i` is the real key; name just aids debugging.
            name = bind.respond_to?(:name) ? bind.name : "p"
            raw_records["#{i}_#{name}"] = array
          end
          record_batch = Arrow::RecordBatch.new(raw_records)
          statement.bind(record_batch) do
            statement.execute[0]
          end
        end
      end
    end

    def cast_result(arrow_table)
      Result.new(backend, arrow_table, self)
    end

    # Borrowed from
    # ActiveRecord::ConnectionAdapters::PostgreSQL::DatabaseStatements.
    #
    # Copyright (c) David Heinemeier Hansson
    #
    # The MIT license.
    READ_QUERY =
      ActiveRecord::ConnectionAdapters::AbstractAdapter.build_read_query_regexp(
        :close, :declare, :fetch, :move, :set, :show
      ) #:nodoc:
    private_constant :READ_QUERY
    def write_query?(sql) # :nodoc:
      !READ_QUERY.match?(sql)
    rescue ArgumentError # Invalid encoding
      !READ_QUERY.match?(sql.b)
    end
  end
end
