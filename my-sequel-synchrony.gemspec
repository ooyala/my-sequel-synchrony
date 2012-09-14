# -*- encoding: utf-8 -*-
require File.expand_path('../lib/my-sequel-synchrony/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = [ "Ilya Maykov" ]
  gem.email         = [ "ivmaykov@gmail.com" ]
  gem.description   = "A fiber-aware MySQL adapter for Sequel that works with em-synchrony"
  gem.summary       = "A fiber-aware MySQL adapter for Sequel that works with em-synchrony"
  gem.homepage      = "https://github.com/ivmaykov/my-sequel-synchrony"

  gem.files         = `git ls-files`.split($\)
  # gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  # gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "my-sequel-synchrony"
  gem.require_paths = [ "lib" ]
  gem.version       = Sequel::Mysql2::Synchrony::VERSION
  gem.required_ruby_version = " >= 1.9.2"

  gem.add_dependency("sequel", "~> 3.39.0")
  gem.add_dependency("mysql2", "~> 0.3.11")
  gem.add_dependency("em-synchrony", "~> 1.0.0")
  gem.add_dependency("rake", "~> 0.8.7")
end
