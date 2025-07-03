module ActiveRecordADBCAdapter
  module Quoting
    extend ActiveSupport::Concern

    module ClassMethods
      def quote_column_name(column_name)
        "\"#{column_name.gsub("\"", "\"\"")}\""
      end
    end
  end
end
