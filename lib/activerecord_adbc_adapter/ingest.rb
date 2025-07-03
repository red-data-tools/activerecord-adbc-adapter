module ActiveRecordADBCAdapter
  module Ingest
    extend ActiveSupport::Concern

    module ClassMethods
      def ingest(attributes)
        self.with_connection do |connection|
          connection.ingest(table_name, attributes, name: "#{self} Ingest")
        end
      end
    end
  end
end
