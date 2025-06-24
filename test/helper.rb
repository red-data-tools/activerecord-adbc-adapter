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
        unless status.success?
          message = "Failed to execute a SQL: <#{sql}>\n"
          message << ("-" * 40) + "\n"
          message << log_file.read
          message << ("-" * 40) + "\n"
          raise message
        end
      end
    end

    def setup_connection
      Tempfile.create(["activerecord-adbc-adapter", ".sqlite3"]) do |db_file|
        @db_path = db_file.path
        ActiveRecord::Base.establish_connection(adapter: "adbc",
                                                driver: "adbc_driver_sqlite",
                                                uri: @db_path)
        begin
          yield
        ensure
          ActiveRecord::Base.connection_handler.clear_all_connections!
        end
      end
    end
  end
end
