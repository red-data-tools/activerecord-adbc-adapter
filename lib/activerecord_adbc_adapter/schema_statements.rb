module ActiveRecordADBCAdapter
  module SchemaStatements
    NATIVE_DATABASE_TYPES = {
      "duckdb" => {
        primary_key: "bigint PRIMARY KEY",
      },
      "postgresql" => {
        primary_key: "bigserial PRIMARY KEY",
        string: {name: "character varying"},
        binary: {name: "bytea"},
        datetime: {name: "timestamp without time zone"},
      },
      "sqlite" => {
        primary_key: "integer PRIMARY KEY AUTOINCREMENT NOT NULL",
        # INTEGER storage class can store 8 bytes value:
        # https://www.sqlite.org/datatype3.html#storage_classes_and_datatypes
        integer: {name: "integer", limit: 8},
        bigint: {name: "bigint", limit: 8},
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
        objects = conn.get_objects(depth: :tables,
                                   catalog: adbc_catalog,
                                   db_schema: adbc_db_schema,
                                   table_types: [type])
        tables = []
        objects.raw_records.each do |_catalog_name, db_schemas|
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

    def views
      type = adbc_view_type
      with_raw_connection do |conn|
        objects = conn.get_objects(depth: :tables,
                                   catalog: adbc_catalog,
                                   db_schema: adbc_db_schema,
                                   table_types: [type])
        views = []
        objects.raw_records.each do |_catalog_name, db_schemas|
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

    def column_definitions(table_name)
      with_raw_connection do |conn|
        objects = conn.get_objects(catalog: adbc_catalog,
                                   db_schema: adbc_db_schema,
                                   table_name: table_name)
        objects.raw_records.each do |_catalog_name, db_schemas|
          db_schemas.each do |db_schema|
            db_schema_tables = db_schema["db_schema_tables"]
            next if db_schema_tables.nil?
            db_schema_tables.each do |table|
              return table["table_columns"]
            end
          end
        end
        [] # raise?
      end
    end

    def primary_keys(table_name)
      with_raw_connection do |conn|
        objects = conn.get_objects(catalog: adbc_catalog,
                                   db_schema: adbc_db_schema,
                                   table_name: table_name)
        objects.raw_records.each do |_catalog_name, db_schemas|
          db_schemas.each do |db_schema|
            db_schema_tables = db_schema["db_schema_tables"]
            next if db_schema_tables.nil?
            db_schema_tables.each do |table|
              constraint = table["table_constraints"].find do |constraint|
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

    private
    def create_table_definition(name, **options)
      TableDefinition.new(self, name, **options)
    end

    def schema_creation
      SchemaCreation.new(self)
    end

    def new_column_from_field(table_name, field, definitions)
      xdbc_type_name = field["xdbc_type_name"]
      if xdbc_type_name
        type_metadata = fetch_type_metadata(xdbc_type_name)
      else
        type_metadata = nil
      end
      Column.new(field["column_name"],
                 field["xdbc_column_def"],
                 type_metadata,
                 field["xdbc_nullable"] == 1,
                 nil,
                 collation: nil,
                 comment: nil)
    end
  end
end
