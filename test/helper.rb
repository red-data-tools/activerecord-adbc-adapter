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

    def run_sql(sql)
      Tempfile.create("activerecord-adbc-adapter-sqlite3-log") do |log_file|
        pid = spawn("sqlite3", @db_path, sql, out: log_file, err: log_file)
        _, status = Process.waitpid2(pid)
        if status.success?
          log_file.read
        else
          message = "Failed to execute a SQL: <#{sql}>\n"
          message << ("-" * 40) + "\n"
          message << log_file.read
          message << ("-" * 40) + "\n"
          raise message
        end
      end
    end

    def setup_connection
      case ENV["ACTIVERECORD_ADBC_ADAPTER_BACKEND"]
      when "duckdb"
        suffix = ".duckdb"
        path_key = :path
        options = {
          driver: "duckdb",
          entrypoint: "duckdb_adbc_init",
        }
      else
        suffix = ".sqlite3"
        path_key = :uri
        options = {
          driver: "adbc_driver_sqlite",
        }
      end
      Tempfile.create(["activerecord-adbc-adapter", suffix]) do |db_file|
        @db_path = db_file.path
        FileUtils.rm_f(@db_path)
        options[path_key] = @db_path
        ActiveRecord::Base.establish_connection(adapter: "adbc", **options)
        begin
          yield
        ensure
          User.reset_column_information
          ActiveRecord::Base.connection_handler.clear_all_connections!
        end
      end
    end
  end
end
