module ActiveRecordADBCAdapter
  module RelationArrowable
    def to_arrow(...)
      if adbc_connection?
        result = exec_main_query
        result.attach_model(model)
        result.to_arrow
      elsif defined?(super)
        super(...)
      else
        raise NoMethodError,
              "#{model}#to_arrow is only available on ADBC-backed connections " \
              "(install red-arrow-activerecord for non-ADBC support)"
      end
    end

    def each_record_batch(&block)
      if adbc_connection?
        result = exec_main_query
        result.attach_model(model)
        result.each_record_batch(&block)
      elsif defined?(super)
        super(&block)
      else
        raise NoMethodError,
              "#{model}#each_record_batch is only available on ADBC-backed connections"
      end
    end

    private
    def adbc_connection?
      model.with_connection do |connection|
        connection.is_a?(ActiveRecordADBCAdapter::Adapter)
      end
    end
  end

  ActiveRecord::Relation.prepend(RelationArrowable)
  ActiveRecord::Querying.delegate(:to_arrow, :each_record_batch, to: :all)
end
