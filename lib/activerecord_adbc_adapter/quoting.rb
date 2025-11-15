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
        require 'active_record/connection_adapters/sqlite3/quoting'

        # Dynamically create proxy class to avoid loading SQLite3 module at file load time
        proxy_class = Class.new do
          include ActiveRecord::ConnectionAdapters::Quoting
          include ActiveRecord::ConnectionAdapters::SQLite3::Quoting

          def initialize(adapter)
            @adapter = adapter
          end

          # Delegate methods to the ADBC adapter (for default_timezone, etc.)
          def method_missing(method, *args, &block)
            if adapter.respond_to?(method)
              adapter.public_send(method, *args, &block)
            else
              super
            end
          end

          def respond_to_missing?(method, include_private = false)
            adapter.respond_to?(method, include_private) || super
          end

          private

          attr_reader :adapter
        end

        proxy_class.new(self)
      end
    end
  end
end
