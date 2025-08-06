module ActiveRecordADBCAdapter
  class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition
    private
    def aliased_types(name, fallback)
      fallback
    end
  end
end
