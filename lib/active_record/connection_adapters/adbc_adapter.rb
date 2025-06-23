require "adbc"

require "active_record/connection_adapters/adbc/column"
require "active_record/connection_adapters/adbc/database_statements"
require "active_record/connection_adapters/adbc/quoting"
require "active_record/connection_adapters/adbc/result"
require "active_record/connection_adapters/adbc/schema_statements"

module ActiveRecord
  module ConnectionHandling # :nodoc:
    def adbc_adapter_class
      ConnectionAdapters::ADBCAdapter
    end

    def adbc_connection(config)
      adbc_adapter_class.new(config)
    end
  end

  module ConnectionAdapters
    # = Active Record ADBC Adapter
    #
    # ...
    #
    # Options:
    #
    # * ...
    class ADBCAdapter < AbstractAdapter
      ADAPTER_NAME = "ADBC"

      class Connection
        def initialize(**params)
          @database = ::ADBC::Database.open(**params)
          @connection = @database.connect
        end

        def close
          if @connection
            @connection.release
            @connection = nil
          end
          @database.release
          @database = nil
        end

        def reconnect
          if @connection
            @connection.release
            @connection = @database.connect
          end
        end

        def query(sql, binds)
          @connection.open_statement do |statement|
            statement.sql_query = sql
            if binds.empty?
              statement.execute[0]
            else
              statement.prepare
              table = Arrow::Table.new(limit: binds)
              statement.bind(table) do
                statement.execute[0]
              end
            end
          end
        end

        def tables
          get_objects(:tables, nil, nil, nil, ["table"]) do |table|
            table.raw_records[0][1][0]["db_schema_tables"].collect do |table|
              table["table_name"]
            end
          end
        end

        def views
          get_objects(:tables, nil, nil, nil, ["view"]) do |table|
            table.raw_records[0][1][0]["db_schema_tables"].collect do |table|
              table["table_name"]
            end
          end
        end

        def column_definitions(table_name)
          get_objects(:all, nil, nil, table_name) do |table|
            table.raw_records[0][1][0]["db_schema_tables"][0]["table_columns"]
          end
        end

        def primary_keys(table_name)
          get_objects(:all, nil, nil, table_name) do |table|
            table = table.raw_records[0][1][0]["db_schema_tables"][0]
            constraint = table["table_constraints"].find do |constraint|
              constraint["constraint_type"] == "PRIMARY KEY"
            end
            return [] if constraint.nil?
            constraint["constraint_column_names"] || []
          end
        end

        private
        def get_objects(*args)
          reader = @connection.get_objects(*args)
          begin
            yield(reader.read_all)
          ensure
            reader.unref
          end
        end
      end

      class << self
        def new_client(params)
          Connection.new(**params)
        end
      end

      include ADBC::DatabaseStatements
      include ADBC::Quoting
      include ADBC::SchemaStatements

      FEATURES = [
        :supports_insert_on_duplicate_skip,
      ]

      def initialize(...)
        super

        @connection_parameters = @config.compact
        @connection_parameters.delete(:adapter)

        @raw_connection = nil

        @features = {}
      end

      FEATURES.each do |feature|
        define_method("#{feature}?") do
          @features[feature]
        end
      end

      def active?
        @lock.synchronize do
          return false unless @raw_connection
        end
        true
      end

      def disconnect!
        @lock.synchronize do
          super
          @raw_connection&.close rescue nil
          @raw_connection = nil
        end
      end

      def column_definitions(table_name)
        with_raw_connection do |conn|
          conn.column_definitions(table_name)
        end
      end

      # Borrowed from
      # ActiveRecord::ConnectionAdapters::PostgreSQLAdapter#build_insert_sql.
      #
      # Copyright (c) David Heinemeier Hansson
      #
      # The MIT license.
      def build_insert_sql(insert)
        sql = +"INSERT #{insert.into} #{insert.values_list}"

        if insert.skip_duplicates?
          sql << " ON CONFLICT #{insert.conflict_target} DO NOTHING"
        elsif insert.update_duplicates?
          sql << " ON CONFLICT #{insert.conflict_target} DO UPDATE SET "
          if insert.raw_update_sql?
            sql << insert.raw_update_sql
          else
            sql << insert.touch_model_timestamps_unless { |column| "#{insert.model.quoted_table_name}.#{column} IS NOT DISTINCT FROM excluded.#{column}" }
            sql << insert.updatable_columns.map { |column| "#{column}=excluded.#{column}" }.join(",")
          end
        end

        sql << " RETURNING #{insert.returning}" if insert.returning
        sql
      end

      private
      def connect
        @raw_connection = self.class.new_client(@connection_parameters)
        detect_features
        @raw_connection
      end

      def detect_features
        backend = @connection_parameters[:driver].gsub(/\Aadbc_driver_/, "")
        detect_features_method = "detect_features_#{backend}"
        if respond_to?(detect_features_method, true)
          __send__(detect_features_method)
        end
      end

      def detect_features_sqlite
        @features[:supports_insert_on_duplicate_skip] = true
      end

      def reconnect
        @lock.synchronize do
          @raw_connection&.reconnect
        end

        connect unless @raw_connection
      end
    end
    ActiveSupport.run_load_hooks(:active_record_adbcadapter, ADBCAdapter)
  end
end
