require File.expand_path('../lib/version', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'ruby-manta'
  s.version     = MantaClient::LIB_VERSION
  s.date        = '2013-01-15'
  s.summary     = "Interface for Joyent's Manta service."
  s.description = "A simple low-abstraction layer which communicates with Joyent's Manta service." 
  s.authors     = ['Joyent']
  s.email       = 'marsell@joyent.com'
  s.homepage    = 'http://github.com/joyent/ruby-manta/'

  s.add_dependency('httpclient', '>= 2.3.0.1')
  s.add_dependency('net-ssh', '>= 2.6.0')
  s.add_development_dependency('minitest')

  s.files       = ['LICENSE',
                   'README.md',
                   'ruby-manta.gemspec',
                   'example.rb',
                   'lib/version.rb',
                   'lib/ruby-manta.rb',
                   'tests/test_ruby-manta.rb']

  s.test_files  = s.files.grep(%r{^test})
  s.require_paths = %w{lib}
end
