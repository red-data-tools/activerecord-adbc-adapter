class User < ActiveRecord::Base
  include ActiveRecordADBCAdapter::Ingest
end

class RawUser < ActiveRecord::Base
  self.table_name = :users
end
