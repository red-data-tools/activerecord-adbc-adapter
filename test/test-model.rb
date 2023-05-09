class TestModel < Test::Unit::TestCase
  include Helper::Sandbox

  setup do
    run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
    run_sql("INSERT INTO users VALUES (1), (2), (3)")
  end

  def test_first
    assert_equal(User.new(id: 1), User.first)
  end
end
