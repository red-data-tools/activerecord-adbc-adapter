# -*- ruby -*-

source "https://rubygems.org/"

plugin "rubygems-requirements-system"

gemspec

gem "bundler"
gem "rake"
gem "test-unit"

local_rails = File.expand_path(File.join(__dir__, "..", "rails"))
if File.exist?(local_rails)
  path local_rails do
    gem "activerecord"
  end
else
  git "https://github.com/rails/rails.git" do
    gem "activerecord"
  end
end

local_adbc = File.expand_path(File.join(__dir__, "..", "arrow-adbc"))
if File.exist?(local_adbc)
  path "#{local_adbc}/ruby" do
    gem "red-adbc"
  end
else
  git "https://github.com/apache/arrow-adbc.git" do
    gem "red-adbc"
  end
end
