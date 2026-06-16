require "tempfile"

require "activerecord-adbc-adapter"
require "test-unit"

require_relative "helper/user"

if ENV["ACTIVERECORD_ADBC_ADAPTER_DEBUG"] == "yes"
  ActiveRecord::Base.logger = ActiveSupport::Logger.new(STDERR)
end

module Helper
  module Sandbox
    class << self
      def included(base)
        base.module_eval do
          setup :setup_connection
        end
      end
    end

    def backend
      ENV["ACTIVERECORD_ADBC_ADAPTER_BACKEND"]
    end

    def bigquery?
      backend == "bigquery"
    end

    def duckdb?
      backend == "duckdb"
    end

    def postgresql?
      backend == "postgresql"
    end

    def sqlite?
      return false if bigquery?
      return false if duckdb?
      return false if postgresql?
      true
    end

    def setup_connection
      suffix = nil
      database = "ar_adbc_test"
      ar_adapter_name = nil
      case backend
      when "bigquery"
        options = {
          driver: "adbc_driver_bigquery",
          "adbc.bigquery.sql.project_id":
            ENV.fetch("GOOGLE_CLOUD_PROJECT"),
          "adbc.bigquery.sql.dataset_id":
            ENV.fetch("BIGQUERY_DATASET", "adbc_test"),
        }
      when "duckdb"
        suffix = ".duckdb"
        path_key = :path
        options = {
          driver: "duckdb",
          entrypoint: "duckdb_adbc_init",
        }
      when "postgresql"
        options = {
          driver: "adbc_driver_postgresql",
          uri: "postgresql:///#{database}",
        }
        ar_adapter_name = "postgresql"
        ar_adapter_options = {
          database: database,
        }
        ar_adapter_create_database_options = {
          database: "postgres",
        }
        ar_create_database_options = {
          template: "template0",
        }
      else
        suffix = ".sqlite3"
        path_key = :uri
        options = {
          driver: "adbc_driver_sqlite",
        }
        ar_adapter_name = "sqlite3"
        ar_adapter_options = {
          database: nil,
        }
      end
      setup = lambda do
        ActiveRecord::Base.establish_connection(adapter: "adbc", **options)
        if ar_adapter_name
          ar_adapter_options[:database] ||= @db_path
          RawUser.establish_connection(adapter: ar_adapter_name,
                                       **ar_adapter_options)
        end
        begin
          yield
        ensure
          User.reset_column_information
          RawUser.reset_column_information
          RawUser.connection_handler.clear_all_connections!
          ActiveRecord::Base.connection_handler.clear_all_connections!
        end
      end
      if suffix
        Tempfile.create(["activerecord-adbc-adapter", suffix]) do |db_file|
          @db_path = db_file.path
          FileUtils.rm_f(@db_path)
          options[path_key] = @db_path
          setup.call
        end
      elsif bigquery?
        # Ideally we'd create/drop a temporary dataset per test run
        # (like PostgreSQL creates/drops a database), but the ADBC
        # BigQuery driver requires dataset_id for all SQL execution
        # — even CREATE SCHEMA fails without it. So we reuse an
        # existing dataset and drop tables in teardown instead.
        setup.call
      else
        adapter_class = ActiveRecord::ConnectionAdapters.resolve(ar_adapter_name)
        adapter = adapter_class.new(ar_adapter_create_database_options)
        adapter.drop_database(database)
        adapter.create_database(database, **ar_create_database_options)
        setup.call
      end
    end
  end
end
