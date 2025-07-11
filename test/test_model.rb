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
                 User.select(:id).all)
  end

  sub_test_case(".ingest") do
    def id_array
      Arrow::Int64Array.new([4, 5, 6])
    end

    def test_record_batch
      record_batch = Arrow::RecordBatch.new(id: id_array)
      User.ingest(record_batch)
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all)
    end

    def test_table
      table = Arrow::Table.new(id: id_array)
      User.ingest(table)
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all)
    end

    def test_record_batch_reader
      record_batch = Arrow::RecordBatch.new(id: id_array)
      User.ingest(Arrow::RecordBatchReader.new([record_batch]))
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all)
    end
  end

  sub_test_case("#to_arrow") do
    def test_model
      assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1, 2, 3])),
                   User.to_arrow)
    end

    def test_relation
      assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1, 2, 3])),
                   User.all.to_arrow)
    end
  end

  sub_test_case("#each_record_batch") do
    def test_model
      record_batch = Arrow::RecordBatch.new(id: Arrow::Int64Array.new([1, 2, 3]))
      assert_equal([record_batch],
                   User.each_record_batch.to_a)
    end

    def test_relation
      record_batch = Arrow::RecordBatch.new(id: Arrow::Int64Array.new([1, 2, 3]))
      assert_equal([record_batch],
                   User.all.each_record_batch.to_a)
    end
  end
end
