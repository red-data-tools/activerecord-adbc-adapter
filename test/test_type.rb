class TestType < Test::Unit::TestCase
  include Helper::Sandbox

  def test_bigint
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.bigint :bigint
    end
    User.create!(bigint: 2 ** 32)
    assert_equal(User.new(id: 1, bigint: 2 ** 32),
                 User.first)
  end

  def test_integer
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.integer :integer
    end
    User.create!(integer: 1)
    assert_equal(User.new(id: 1, integer: 1),
                 User.first)
  end

  def test_float
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.float :float
    end
    User.create!(float: 2.9)
    assert_equal(User.new(id: 1, float: 2.9),
                 User.first)
  end

  def test_binary
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.binary :binary
    end
    User.create!(binary: "Hello".b)
    assert_equal(User.new(id: 1, binary: "Hello".b),
                 User.first)
  end

  def test_string
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.string :string
    end
    User.create!(string: "Hello")
    assert_equal(User.new(id: 1, string: "Hello"),
                 User.first)
  end

  def test_text
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.text :text
    end
    User.create!(text: "Hello")
    assert_equal(User.new(id: 1, text: "Hello"),
                 User.first)
  end
end
