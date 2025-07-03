require "active_record"

require_relative "activerecord_adbc_adapter/ingest"
require_relative "activerecord_adbc_adapter/version"

module ActiveRecord::ConnectionAdapters
  register("adbc",
           "ActiveRecordADBCAdapter::Adapter",
           "activerecord_adbc_adapter/adapter")
end
