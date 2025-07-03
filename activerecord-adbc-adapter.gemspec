# -*- ruby -*-

require_relative "lib/activerecord_adbc_adapter/version"

Gem::Specification.new do |spec|
  spec.name = "activerecord-adbc-adapter"
  spec.version = ActiveRecordADBCAdapter::VERSION
  spec.homepage = "https://github.com/red-data-tools/activerecord-adbc-adapter"
  spec.authors = ["Sutou Kouhei"]
  spec.email = ["kou@clear-code.com"]

  spec.summary = "Active Record's ADBC adapter"
  spec.description =
    "This gem provides an ADBC adapter for Active Record. " +
    "This adapter is optimized for extracting and loading large data " +
    "from/to DBs. The optimization is powered by Apache Arrow."
  spec.license = "MIT"
  spec.files = [
    "LICENSE.txt",
    "README.md",
  ]
  spec.files += Dir.glob("lib/**/*.rb")

  spec.add_runtime_dependency("activerecord")
  spec.add_runtime_dependency("red-adbc")
end
