module ActiveRecord
  module ConnectionAdapters
    module ADBC
      module SchemaStatements
        def tables
          with_raw_connection do |conn|
            conn.tables
          end
        end

        def views
          with_raw_connection do |conn|
            conn.views
          end
        end

        def primary_keys(table_name)
          with_raw_connection do |conn|
            conn.primary_keys(table_name)
          end
        end

        def column_definitions(table_name)
          with_raw_connection do |conn|
            conn.column_definitions(table_name)
          end
        end

        private
        def new_column_from_field(table_name, field, definitions)
          ADBC::Column.new(field["column_name"],
                           field["xdbc_column_def"],
                           nil,
                           field["xdbc_nullable"] == 1,
                           nil,
                           collation: nil,
                           comment: nil)
        end
      end
    end
  end
end
