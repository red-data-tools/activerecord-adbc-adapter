class TestType < Test::Unit::TestCase
  include Helper::Sandbox

  def test_integer
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.column :integer, :integer
    end
    User.create!(id: 1, integer: 1)
    assert_equal(User.new(id: 1, integer: 1),
                 User.first)
  end

  def test_string
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.column :string, :string
    end
    User.create!(string: "Hello")
    assert_equal(User.new(id: 1, string: "Hello"),
                 User.first)
  end
end
