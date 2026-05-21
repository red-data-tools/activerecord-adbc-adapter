require "arrow-activerecord"

module ActiveRecordADBCAdapter
  module RelationArrowable
    def to_arrow(...)
      if model.adapter_class == Adapter
        result = exec_main_query
        result.attach_model(model)
        result.to_arrow
      else
        super
      end
    end

    def each_record_batch(*, **, &block)
      if model.adapter_class == Adapter
        result = exec_main_query
        result.attach_model(model)
        result.each_record_batch(&block)
      else
        super
      end
    end
  end

  ActiveRecord::Relation.include(RelationArrowable)
  ActiveRecord::Querying.delegate(:to_arrow, :each_record_batch, to: :all)
end
