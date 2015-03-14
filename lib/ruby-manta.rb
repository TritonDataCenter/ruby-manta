module RubyManta
end

require_relative 'ruby-manta/manta_client'

# Added API compatibility with 1.xx versions. This may be removed in the future
MantaClient = RubyManta::MantaClient
