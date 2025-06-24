module ActiveRecord
  module ConnectionAdapters
    module ADBC
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
              binds.zip(type_casted_binds) do |bind, type_casted_bind|
                raw_records[bind.name] = [type_casted_bind]
              end
              record_batch = Arrow::RecordBatch.new(raw_records)
              statement.bind(record_batch) do
                statement.execute[0]
              end
            end
          end
        end

        def cast_result(raw_result)
          Result.new(raw_result)
        end
      end
    end
  end
end
