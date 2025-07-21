class TestType < Test::Unit::TestCase
  include Helper::Sandbox

  def test_bigint_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.bigint :bigint
    end
    User.create!(bigint: 2 ** 32)
    assert_equal(User.new(id: 1, bigint: 2 ** 32),
                 User.first)
  end

  def test_bigint_arrow
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.bigint :bigint
    end
    User.create!(bigint: 2 ** 32)
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  bigint: Arrow::Int64Array.new([2 ** 32])),
                 User.to_arrow)
  end

  def test_integer_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.integer :integer
    end
    User.create!(integer: 1)
    assert_equal(User.new(id: 1, integer: 1),
                 User.first)
  end

  def test_integer_arrow
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.integer :integer
    end
    User.create!(integer: 1)
    if sqlite?
      array = Arrow::Int64Array.new([1])
    else
      array = Arrow::Int32Array.new([1])
    end
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  integer: array),
                 User.to_arrow)
  end

  def test_float_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.float :float
    end
    User.create!(float: 2.9)
    assert_equal(User.new(id: 1, float: 2.9),
                 User.first)
  end

  def test_float_arrow
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.float :float
    end
    User.create!(float: 2.9)
    if duckdb?
      array = Arrow::FloatArray.new([2.9])
    else
      array = Arrow::DoubleArray.new([2.9])
    end
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  float: array),
                 User.to_arrow)
  end

  def test_binary_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.binary :binary
    end
    User.create!(binary: "Hello".b)
    assert_equal(User.new(id: 1, binary: "Hello".b),
                 User.first)
  end

  def test_binary_arrow
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.binary :binary
    end
    User.create!(binary: "Hello".b)
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  binary: Arrow::BinaryArray.new(["Hello".b])),
                 User.to_arrow)
  end

  def test_string_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.string :string
    end
    User.create!(string: "Hello")
    assert_equal(User.new(id: 1, string: "Hello"),
                 User.first)
  end

  def test_text_arrow
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.text :text
    end
    User.create!(text: "Hello")
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  text: Arrow::StringArray.new(["Hello"])),
                 User.to_arrow)
  end

  def test_date_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.date :date
    end
    date = Date.new(2025, 7, 20)
    User.create!(date: date)
    assert_equal(User.new(id: 1, date: date),
                 User.first)
  end

  def test_date_arrow
    omit("TODO") if sqlite?
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.date :date
    end
    date = Date.new(2025, 7, 20)
    User.create!(date: date)
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  date: Arrow::Date32Array.new([date])),
                 User.to_arrow)
  end

  def test_datetime_active_record
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.datetime :datetime
    end
    datetime = DateTime.new(2025, 7, 20, 20, 40, 23)
    User.create!(datetime: datetime)
    assert_equal(User.new(id: 1, datetime: datetime),
                 User.first)
  end

  def test_datetime
    omit("TODO") if sqlite?
    ActiveRecord::Base.connection.create_table("users") do |table|
      table.datetime :datetime
    end
    datetime = DateTime.new(2025, 7, 20, 20, 40, 23)
    User.create!(datetime: datetime)
    assert_equal(Arrow::Table.new(id: Arrow::Int64Array.new([1]),
                                  datetime: Arrow::TimestampArray.new(:micro, [datetime])),
                 User.to_arrow)
  end
end
