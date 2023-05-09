# -*- ruby -*-

require "rubygems"
require "bundler/gem_helper"

base_dir = File.join(__dir__)

helper = Bundler::GemHelper.new(base_dir)
helper.install

release_task = Rake::Task["release"]
release_task.prerequisites.replace(["build", "release:rubygem_push"])

desc "Run tests"
task :test do
  cd(base_dir) do
    ruby("test/run.rb")
  end
end

task default: :test
