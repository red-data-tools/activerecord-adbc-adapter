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
          raw_connection.query(sql, type_casted_binds)
        end

        def cast_result(raw_result)
          Result.new(raw_result)
        end
      end
    end
  end
end
