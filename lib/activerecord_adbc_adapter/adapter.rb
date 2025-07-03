require "adbc"

require_relative "column"
require_relative "database_statements"
require_relative "quoting"
require_relative "result"
require_relative "schema_creation"
require_relative "schema_definitions"
require_relative "schema_statements"

module ActiveRecordADBCAdapter
  # = Active Record ADBC Adapter
  #
  # ...
  #
  # Options:
  #
  # * ...
  class Adapter < ActiveRecord::ConnectionAdapters::AbstractAdapter
    ADAPTER_NAME = "ADBC"

    class Connection
      def initialize(**params)
        @database = ADBC::Database.open(**params)
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

      def open_statement(&block)
        @connection.open_statement(&block)
      end

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

    include DatabaseStatements
    include Quoting
    include SchemaStatements

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

    def connect
      @raw_connection = self.class.new_client(@connection_parameters)
      detect_features
      @raw_connection
    end

    def reconnect
      @lock.synchronize do
        @raw_connection&.reconnect
      end

      connect unless @raw_connection
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

    def ingest(table_name, attributes, name: nil)
      log(table_name, name) do |notification_payload|
        with_raw_connection do |raw_connection|
          raw_connection.open_statement do |statement|
            statement.ingest(table_name, attributes, mode: :append)
          end
        end
      end
    end

    def backend
      @connection_parameters[:driver].gsub(/\Aadbc_driver_/, "")
    end

    private
    def detect_features
      detect_features_method = "detect_features_#{backend}"
      if respond_to?(detect_features_method, true)
        __send__(detect_features_method)
      end
    end

    def detect_features_duckdb
      @features[:supports_insert_on_duplicate_skip] = true
    end

    def detect_features_sqlite
      @features[:supports_insert_on_duplicate_skip] = true
    end
  end
  ActiveSupport.run_load_hooks(:active_record_adbcadapter, Adapter)
end
