module ActiveRecordADBCAdapter
  module DatabaseStatements
    def perform_query(raw_connection,
                      sql,
                      binds,
                      type_casted_binds,
                      prepare:,
                      notification_payload:,
                      batch:)
      statement = raw_connection.open_statement
      begin
        statement.sql_query = sql
        if binds.empty?
          reader = statement.execute do |r,|
            r
          end
        else
          statement.prepare
          raw_records = {}
          binds.zip(type_casted_binds) do |bind, type_casted_bind|
            raw_records[bind.name] = [type_casted_bind]
          end
          record_batch = Arrow::RecordBatch.new(raw_records)
          reader = statement.bind(record_batch) do
            statement.execute do |r,|
              r
            end
          end
        end
      rescue
        statement.release
        raise
      else
        Result.new(statement, reader)
      end
    end

    def cast_result(result)
      result
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
