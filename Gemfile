# -*- ruby -*-

source "https://rubygems.org/"

plugin "rubygems-requirements-system"

gemspec

gem "bundler"
gem "rake"

group :development, :test do
  gem "irb"
  gem "repl_type_completor"
end

group :benchmark do
  gem "benchmark"
  gem "pg"
end

group :test do
  gem "test-unit"
end

local_rails = File.expand_path(File.join(__dir__, "..", "rails"))
if File.exist?(local_rails)
  path local_rails do
    gem "activerecord"
  end
else
  gem "activerecord"
end

local_adbc = File.expand_path(File.join(__dir__, "..", "arrow-adbc"))
if File.exist?(local_adbc)
  path "#{local_adbc}/ruby" do
    gem "red-adbc"
  end
else
  gem "red-adbc"
end
