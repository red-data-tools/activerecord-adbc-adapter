require "tempfile"

require "activerecord-adbc-adapter"
require "test-unit"

require_relative "helper/user"

if ENV["ACTIVERECORD_ADBC_ADAPTER_GC"] == "disable"
  GC.disable
end

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

    def duckdb?
      backend == "duckdb"
    end

    def postgresql?
      backend == "postgresql"
    end

    def sqlite?
      return false if duckdb?
      return false if postgresql?
      true
    end

    def setup_connection
      suffix = nil
      database = "ar_adbc_test"
      case backend
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
      end
      setup = lambda do
        ActiveRecord::Base.establish_connection(adapter: "adbc", **options)
        begin
          yield
        ensure
          User.reset_column_information
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
      else
        adapter_class = ActiveRecord::ConnectionAdapters.resolve(ar_adapter_name)
        adapter = adapter_class.new(ar_adapter_options)
        adapter.drop_database(database)
        adapter.create_database(database, **ar_create_database_options)
        setup.call
      end
    end
  end
end
