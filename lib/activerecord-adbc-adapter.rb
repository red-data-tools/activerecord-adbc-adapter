require "active_record"

module ActiveRecord
  module ConnectionAdapters
    register("adbc",
             "ActiveRecord::ConnectionAdapters::ADBCAdapter",
             "active_record/connection_adapters/adbc_adapter")
  end
end
