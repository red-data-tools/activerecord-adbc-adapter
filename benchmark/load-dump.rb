#!/usr/bin/env ruby

require "benchmark"
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
uri = +"postgresql://"
if user
  uri << user
  uri << ":#{password}" if password
  uri << "@"
end
uri << "#{host}:#{port || 5432}" if host
uri << "/#{database}"

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

Benchmark.bm do |benchmark|
  benchmark.report("SQL: Load") do
    SqlLog.connection.execute(sql_load)
  end

  benchmark.report("SQL: Dump") do
    SqlLog.connection.execute(sql_dump)
  end

  benchmark.report("Active Record: Load") do
    ActiveRecordLog.insert_all(raw_records)
  end

  benchmark.report("Active Record: Dump") do
    ActiveRecordLog.pluck
  end

  benchmark.report("ADBC: Load") do
    AdbcLog.ingest(arrow_table)
  end

  benchmark.report("ADBC: Dump") do
    AdbcLog.all.to_arrow
  end
end
