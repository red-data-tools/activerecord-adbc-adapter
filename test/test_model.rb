class TestModel < Test::Unit::TestCase
  include Helper::Sandbox

  setup do
    ActiveRecord::Base.connection.create_table("users")
    User.insert_all([
                      {id: 1},
                      {id: 2},
                      {id: 3},
                    ])
  end

  def test_first
    assert_equal(User.new(id: 1), User.select(:id).first)
  end

  def test_all
    assert_equal([
                   User.new(id: 1),
                   User.new(id: 2),
                   User.new(id: 3),
                 ],
                 User.select(:id).all.order(:id))
  end

  def test_count
    assert_equal(3, User.count)
  end

  sub_test_case(".ingest") do
    def id_array
      Arrow::Int64Array.new([4, 5, 6])
    end

    def test_record_batch
      record_batch = Arrow::RecordBatch.new(id: id_array)
      User.ingest(record_batch)
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all.order(:id))
    end

    def test_table
      table = Arrow::Table.new(id: id_array)
      User.ingest(table)
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all.order(:id))
    end

    def test_record_batch_reader
      record_batch = Arrow::RecordBatch.new(id: id_array)
      User.ingest(Arrow::RecordBatchReader.new([record_batch]))
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all.order(:id))
    end
  end

  sub_test_case("#to_arrow") do
    def sort_table(table)
      table.take(table.sort_indices(:id))
    end

    def test_model
      assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1, 2, 3])),
                   sort_table(User.to_arrow))
    end

    def test_relation
      assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1, 2, 3])),
                   User.all.order(:id).to_arrow)
    end

    def test_red_arrow_activerecord
      omit("Red Arrow Active Record isn't enabled for DuckDB") if duckdb?
      table = RawUser.all.order(:id).to_arrow(batch_size: 2)
      assert_equal([
                     Arrow::RecordBatch.new(id: Arrow::Int64Array.new([1, 2])),
                     Arrow::RecordBatch.new(id: Arrow::Int64Array.new([3])),
                   ],
                   table.each_record_batch.to_a)
    end
  end

  sub_test_case("#each_record_batch") do
    def sort_record_batches(record_batches)
      record_batches.collect do |record_batch|
        record_batch.take(record_batch.sort_indices(:id))
      end
    end

    def test_model
      ids = Arrow::Int64Array.new([1, 2, 3])
      assert_equal([Arrow::RecordBatch.new(id: ids)],
                   sort_record_batches(User.each_record_batch))
    end

    def test_relation
      ids = Arrow::Int64Array.new([1, 2, 3])
      assert_equal([Arrow::RecordBatch.new(id: ids)],
                   User.all.order(:id).each_record_batch.to_a)
    end

    def test_red_arrow_activerecord
      omit("Red Arrow Active Record isn't enabled for DuckDB") if duckdb?
      query = RawUser.all.order(:id)
      assert_equal([
                     Arrow::RecordBatch.new(id: Arrow::Int64Array.new([1, 2])),
                     Arrow::RecordBatch.new(id: Arrow::Int64Array.new([3]))
                   ],
                   query.each_record_batch(batch_size: 2).to_a)
    end
  end
end
