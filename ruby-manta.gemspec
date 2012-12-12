require File.expand_path('../lib/ruby-manta', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'ruby-manta'
  s.version     = MantaClient::LIB_VERSION
  s.date        = '2012-12-10'
  s.summary     = "Interface for Joyent's Manta service."
  s.description = "A simple low-abstraction layer which communicates with Joyent's Manta service." 
  s.authors     = ['Joyent']
  s.email       = 'marsell@joyent.com'
  s.homepage    = 'http://github.com/joyent/ruby-manta/'

  s.add_dependency('httpclient', '>= 2.3.0.1')

  s.files       = ['LICENSE',
                   'lib/ruby-manta.rb']
end
