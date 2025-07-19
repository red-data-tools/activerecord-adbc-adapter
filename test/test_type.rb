class TestType < Test::Unit::TestCase
  include Helper::Sandbox

  def test_integer
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.column :integer, :integer
    end
    User.create!(integer: 1)
    assert_equal(User.new(id: 1, integer: 1),
                 User.first)
  end

  def test_float
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.column :float, :float
    end
    User.create!(float: 2.9)
    assert_equal(User.new(id: 1, float: 2.9),
                 User.first)
  end

  def test_double
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.column :double, :double
    end
    User.create!(double: 2.9)
    assert_equal(User.new(id: 1, double: 2.9),
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

  def test_text
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.column :text, :text
    end
    User.create!(text: "Hello")
    assert_equal(User.new(id: 1, text: "Hello"),
                 User.first)
  end
end
