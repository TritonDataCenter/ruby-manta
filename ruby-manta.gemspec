require File.expand_path('../lib/ruby-manta/version', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'ruby-manta'
  s.version     = RubyManta::VERSION
  s.date        = '2015-03-15'
  s.summary     = "Interface for Joyent's Manta service."
  s.description = "A simple low-abstraction layer which communicates with Joyent's Manta service."
  s.authors     = ['Joyent']
  s.email       = 'marsell@joyent.com'
  s.homepage    = 'http://github.com/joyent/ruby-manta/'
  s.license     = 'MIT'

  s.add_dependency             'net-ssh',    '>= 2.6.0'
  s.add_dependency             'httpclient', '>= 2.6.0.1'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'minitest',   '~> 5.5.1'

  s.files       = ['LICENSE',
                   'README.md',
                   'ruby-manta.gemspec',
                   'example.rb',
                   'lib/ruby-manta.rb',
                   'lib/ruby-manta/version.rb',
                   'lib/ruby-manta/manta_client.rb',
                   'test/unit/manta_client_test.rb']

  s.test_files  = s.files.grep(%r{^test})
  s.require_paths = %w{lib}
end
