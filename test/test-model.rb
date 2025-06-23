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
end
