module ActiveRecordADBCAdapter
  module RelationArrowable
    def to_arrow
      exec_main_query.to_arrow
    end

    def each_record_batch(&block)
      exec_main_query.each_record_batch(&block)
    end
  end

  ActiveRecord::Relation.include(RelationArrowable)
  ActiveRecord::Querying.delegate(:to_arrow, :each_record_batch, to: :all)
end
