require_relative "quoting/sqlite3"

module ActiveRecordADBCAdapter
  module Quoting
    extend ActiveSupport::Concern

    module ClassMethods
      def quote_column_name(column_name)
        "\"#{column_name.gsub("\"", "\"\"")}\""
      end
    end

    def quoted_date(value)
      value
    end

    def quoted_time(value)
      case backend
      when "sqlite"
        quoting_sqlite3.quoted_time(value)
      else
        value
      end
    end

    private

    def quoting_sqlite3
      @quoting_sqlite3 ||= Quoting::Sqlite3.new(self)
    end
  end
end
