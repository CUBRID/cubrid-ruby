spec = Gem::Specification.new do |s|
  s.name = "cubrid"
  s.description = "This extension is a Ruby connector for CUBRID Database."
  s.version = "11.1.0"
  s.authors = "NHN"
  s.email = "cubrid_ruby@nhncorp.com"
  s.homepage ="http://www.cubrid.org/cubrid_ruby_programming"
  s.summary = "CUBRID Database API Module for Ruby"
  s.rubyforge_project = "cubrid"
  s.licenses = ['GPL-2.0']
  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = Gem::Version::Requirement.new(">= 1.9")
  s.files = ["README.rdoc", "ext/extconf.rb", "ext/cubrid.c", "ext/cubrid.h", "ext/conn.c", "ext/stmt.c", "ext/error.c"]
  s.require_path = "."
  s.extensions = ["ext/extconf.rb"]
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.rdoc"]
  s.rdoc_options = ["--title", "cubrid-ruby documentation", "--line-numbers", "--main", "README"]
end
