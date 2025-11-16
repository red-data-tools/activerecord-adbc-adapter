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
        sqlite3_quoting_proxy.quoted_time(value)
      else
        value
      end
    end

    private

    def sqlite3_quoting_proxy
      @_sqlite3_quoting_proxy ||= begin
        require_relative "quoting/sqlite3"

        Quoting::Sqlite3.new(self)
      end
    end
  end
end
