spec = Gem::Specification.new do |s|
  s.name = "cubrid"
  s.description = "This extension is a Ruby connector for CUBRID Database."
  s.licenses = 'BSD'
  s.version = "10.0.0"
  s.authors = "NHN"
  s.email = "cubrid_ruby@nhncorp.com"
  s.homepage ="http://www.cubrid.org/cubrid_ruby_programming"
  s.summary = "CUBRID Database API Module for Ruby"
  s.rubyforge_project = "cubrid"
  s.platform = Gem::Platform::RUBY
  s.files = ["README.rdoc"] + Dir["ext/**/*"]
  s.require_path = "."
  s.extensions = ["ext/extconf.rb"]
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.rdoc"]
  s.rdoc_options = ["--title", "cubrid-ruby documentation", "--line-numbers", "--main","README.rdoc"]
end
