module ActiveRecord
  module ConnectionAdapters
    module ADBC
      module DatabaseStatements
        def internal_exec_query(sql,
                                name = "SQL",
                                binds = [],
                                prepare: false,
                                async: false,
                                allow_retry: false,
                                materialize_transactions: true) # :nodoc:
          casted_binds = type_casted_binds(binds)
          log(sql, name, binds, casted_binds, async: async) do
            with_raw_connection do |conn|
              build_arrow_result(conn.query(sql, casted_binds))
            end
          end
        end
      end
    end
  end
end
