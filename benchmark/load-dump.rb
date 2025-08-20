#!/usr/bin/env ruby

require "benchmark"
require "grm"
require "pg"
require_relative "../lib/activerecord-adbc-adapter"

if ENV["DEBUG"]
  ActiveRecord::Base.logger = ActiveSupport::Logger.new(STDERR)
end

n_rows = 10000
n_columns = 100

host = ENV["PGHOST"]
port = ENV["PGPORT"]
user = ENV["PGUSER"]
password = ENV["PGPASSWORD"]
database = ENV["PGDATABASE"] || "ar_adbc_benchmark"
uri = "postgresql:///#{database}"

class SqlLog < ActiveRecord::Base
end
SqlLog.establish_connection(adapter: "postgresql",
                            host: host,
                            port: port&.to_i,
                            username: user,
                            password: password,
                            database: "postgres")
SqlLog.connection.drop_database(database)
SqlLog.connection.create_database(database,
                                  template: "template0")
SqlLog.establish_connection(adapter: "postgresql",
                            host: host,
                            port: port&.to_i,
                            username: user,
                            password: password,
                            database: database)
SqlLog.connection.create_table(SqlLog.table_name, force: true) do |table|
  n_columns.times do |i|
    table.column "column#{i}", :integer
  end
end

class ActiveRecordLog < ActiveRecord::Base
end
ActiveRecordLog.establish_connection(adapter: "postgresql",
                                     host: host,
                                     port: port&.to_i,
                                     username: user,
                                     password: password,
                                     database: database)
ActiveRecordLog.connection.create_table(ActiveRecordLog.table_name,
                                        force: true) do |table|
  n_columns.times do |i|
    table.column "column#{i}", :integer
  end
end

class AdbcLog < ActiveRecord::Base
  include ActiveRecordADBCAdapter::Ingest
end
AdbcLog.establish_connection(adapter: "adbc",
                             driver: "adbc_driver_postgresql",
                             uri: uri)
AdbcLog.connection.create_table(AdbcLog.table_name, force: true) do |table|
  n_columns.times do |i|
    table.column "column#{i}", :integer
  end
end

sql_load = +"INSERT INTO #{SqlLog.table_name} ("
n_columns.times do |column|
  sql_load << ", " unless column.zero?
  sql_load << "column#{column}"
end
sql_load << ") VALUES "
n_rows.times do |row|
  sql_load << ", " unless row.zero?
  sql_load << "("
  n_columns.times do |column|
    sql_load << ", " unless column.zero?
    sql_load << column.to_s
  end
  sql_load << ")"
end
sql_load << ";"

sql_dump = +"SELECT * FROM #{SqlLog.table_name};"

raw_records = n_rows.times.collect do |row|
  record = {}
  n_columns.times do |column|
    record["column#{column}"] = column
  end
  record
end

arrow_columns = {}
n_columns.times do |column|
  arrow_columns["column#{column}"] = Arrow::Int32Array.new([column] * n_rows)
end
arrow_table = Arrow::Table.new(arrow_columns)

puts("Load:")
load_results = Benchmark.bm do |benchmark|
  benchmark.report("SQL") do
    SqlLog.connection.execute(sql_load)
  end

  benchmark.report("Active Record") do
    ActiveRecordLog.insert_all(raw_records)
  end

  benchmark.report("ADBC") do
    AdbcLog.ingest(arrow_table)
  end
end

File.open("load.csv", "w") do |csv|
  csv.puts("approach,elapsed_time")
  load_results.each do |result|
    csv.puts("#{result.label},#{result.real}")
  end
end

puts
puts("Dump:")
dump_results = Benchmark.bm do |benchmark|
  benchmark.report("SQL") do
    SqlLog.connection.execute(sql_dump)
  end

  benchmark.report("Active Record") do
    ActiveRecordLog.pluck
  end

  benchmark.report("ADBC") do
    AdbcLog.all.to_arrow
  end
end

File.open("dump.csv", "w") do |csv|
  csv.puts("approach,elapsed_time")
  dump_results.each do |result|
    csv.puts("#{result.label},#{result.real}")
  end
end


results = [
  {title: "Load", results: load_results},
  {title: "Dump", results: dump_results},
]
n_results = results.size
x_margin = 0.0
x_width = (1 / n_results.to_f) - (x_margin * 2)
y_min = 0.05
y_max = 0.95
all_reals = results.collect do |results|
  results[:results].collect do |result|
    result[:real]
  end
end
y_range = [0.0, all_reals.flatten.max]
subplots = results.each_with_index.collect do |data, i|
  x_index = (i - 1) % n_results
  x_margin_right = x_margin + (2 * x_margin * i)
  position = [
    x_width * i + x_margin_right,
    x_width * (i + 1) + x_margin_right,
    y_min,
    y_max,
  ]
  {
    kind: "barplot",
    title: data[:title],
    c: n_results.times.collect {|j| j + 2},
    x_label: "Approach",
    y: data[:results].collect {|result| result[:real]},
    y_label: "Elapsed time (s)",
    y_labels: data[:results].collect {|result| result[:label]},
    y_range: y_range,
    subplot: position,
  }
end
GRM.merge(subplots: subplots, size: [1200, 600])
GRM.export("load-dump.png")
GRM.export("load-dump.svg")
