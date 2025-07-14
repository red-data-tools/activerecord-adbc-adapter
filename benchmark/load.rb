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

sql = +"INSERT INTO #{SqlLog.table_name} VALUES "
n_rows.times do |row|
  sql << ", " unless row.zero?
  sql << "(#{row}" # id
  n_columns.times do |column|
    sql << ", "
    sql << column.to_s
  end
  sql << ")"
end
sql << ";"

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
  benchmark.report("SQL") do
    SqlLog.connection.execute(sql)
  end

  benchmark.report("Active Record") do
    ActiveRecordLog.insert_all(raw_records)
  end

  benchmark.report("ADBC") do
    AdbcLog.ingest(arrow_table)
  end
end
