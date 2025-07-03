module ActiveRecord
  module ConnectionAdapters
    module ADBC
      module SchemaStatements
        NATIVE_DATABASE_TYPES = {
          "sqlite" => {
            primary_key: "integer PRIMARY KEY AUTOINCREMENT NOT NULL",
          },
          "duckdb" => {
            primary_key: "bigint PRIMARY KEY",
          },
        }

        def native_database_types
          NATIVE_DATABASE_TYPES[backend] || super
        end

        def adbc_catalog
          case backend
          when "duckdb"
            path = @connection_parameters[:path]
            if path
              File.basename(path, ".*")
            else
              "memory"
            end
          else
            nil
          end
        end

        def adbc_db_schema
          case backend
          when "duckdb"
            "main"
          else
            nil
          end
        end

        def adbc_table_type
          case backend
          when "duckdb"
            "BASE TABLE"
          else
            "table"
          end
        end

        def adbc_view_type
          case backend
          when "duckdb"
            "VIEW"
          else
            "view"
          end
        end

        def tables
          type = adbc_table_type
          with_raw_connection do |conn|
            conn.get_objects(:tables,
                             adbc_catalog,
                             adbc_db_schema,
                             nil,
                             [type]) do |table|
              tables = []
              table.raw_records.each do |_catalog_name, db_schemas|
                db_schemas.each do |db_schema|
                  db_schema_tables = db_schema["db_schema_tables"]
                  next if db_schema_tables.nil?
                  db_schema_tables.each do |t|
                    # Some drivers may ignore table_types
                    next unless t["table_type"] == type
                    tables << t["table_name"]
                  end
                end
              end
              tables
            end
          end
        end

        def views
          type = adbc_view_type
          with_raw_connection do |conn|
            conn.get_objects(:tables,
                             adbc_catalog,
                             adbc_db_schema,
                             nil,
                             [type]) do |table|
              views = []
              table.raw_records.each do |_catalog_name, db_schemas|
                db_schemas.each do |db_schema|
                  db_schema_tables = db_schema["db_schema_tables"]
                  next if db_schema_tables.nil?
                  db_schema_tables.each do |t|
                    # Some drivers may ignore table_types
                    next unless t["table_type"] == type
                    views << t["table_name"]
                  end
                end
              end
              views
            end
          end
        end

        def column_definitions(table_name)
          with_raw_connection do |conn|
            conn.get_objects(:all,
                             adbc_catalog,
                             adbc_db_schema,
                             table_name) do |table|
              table.raw_records.each do |_catalog_name, db_schemas|
                db_schemas.each do |db_schema|
                  db_schema_tables = db_schema["db_schema_tables"]
                  next if db_schema_tables.nil?
                  db_schema_tables.each do |t|
                    return t["table_columns"]
                  end
                end
              end
              [] # raise?
            end
          end
        end

        def primary_keys(table_name)
          with_raw_connection do |conn|
            conn.get_objects(:all,
                             adbc_catalog,
                             adbc_db_schema,
                             table_name) do |table|
              table.raw_records.each do |_catalog_name, db_schemas|
                db_schemas.each do |db_schema|
                  db_schema_tables = db_schema["db_schema_tables"]
                  next if db_schema_tables.nil?
                  db_schema_tables.each do |t|
                    constraint = t["table_constraints"].find do |constraint|
                      constraint["constraint_type"] == "PRIMARY KEY"
                    end
                    return [] if constraint.nil?
                    return constraint["constraint_column_names"] || []
                  end
                end
              end
              []
            end
          end
        end

        private
        def create_table_definition(name, **options)
          ADBC::TableDefinition.new(self, name, **options)
        end

        def schema_creation
          ADBC::SchemaCreation.new(self)
        end

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
