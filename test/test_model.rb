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
    def test_record_batch
      record_batch = Arrow::RecordBatch.new(id: [4, 5, 6])
      User.ingest(record_batch)
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all)
    end

    def test_table
      table = Arrow::Table.new(id: [4, 5, 6])
      User.ingest(table)
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all)
    end

    def test_record_batch_reader
      record_batch = Arrow::RecordBatch.new(id: [4, 5, 6])
      User.ingest(Arrow::RecordBatchReader.new([record_batch]))
      assert_equal((1..6).collect {|id| User.new(id: id)},
                   User.select(:id).all)
    end
  end
end
