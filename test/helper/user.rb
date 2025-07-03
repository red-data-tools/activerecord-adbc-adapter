class User < ActiveRecord::Base
  include ActiveRecordADBCAdapter::Ingest
end
