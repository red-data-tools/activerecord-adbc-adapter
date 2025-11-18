require 'active_record/connection_adapters/sqlite3/quoting'

module ActiveRecordADBCAdapter
  module Quoting
    class Sqlite3
      include ActiveRecord::ConnectionAdapters::Quoting
      include ActiveRecord::ConnectionAdapters::SQLite3::Quoting

      def initialize(adapter)
        @adapter = adapter
      end

      delegate :default_timezone, to: :@adapter
    end
  end
end
