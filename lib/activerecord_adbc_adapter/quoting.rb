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
      value
    end
  end
end
