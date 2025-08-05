module ActiveRecordADBCAdapter
  module RelationArrowable
    def to_arrow
      result = exec_main_query
      result.attach_model(model)
      result.to_arrow
    end

    def each_record_batch(&block)
      result = exec_main_query
      result.attach_model(model)
      result.each_record_batch(&block)
    end
  end

  ActiveRecord::Relation.include(RelationArrowable)
  ActiveRecord::Querying.delegate(:to_arrow, :each_record_batch, to: :all)
end
