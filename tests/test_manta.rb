require 'minitest/autorun'
require File.expand_path('../../lib/manta', __FILE__)

class TestManta < MiniTest::Unit::TestCase
  def setup
    host = ENV['HOST']
    user = ENV['USER']
    key  = ENV['KEY' ]

    unless host && user && key
      puts 'Require HOST, USER and KEY to run tests.'
      exit
    end

    priv_key_data = File.read(key)
    http_client, fingerprint, priv_key  = Manta.prepare(priv_key_data, :disable_ssl_verification => true)

    @client = Manta.new(http_client, host, user, fingerprint, priv_key)
  end



  def test_object_creation
    @client.put_object('/marsell/stor/foo', 'asdasd')
    @client.put_link('/marsell/stor/foo', '/marsell/stor/falafel')
    @client.get_object('/marsell/stor/foo')
    @client.delete_object('/marsell/stor/foo')
    @client.put_directory('/marsell/stor/quux')
    @client.list_directory('/marsell/stor')
    @client.delete_directory('/marsell/stor/quux')

    path, _  = @client.create_job({ phases: [{ exec: 'grep foo' }] })
    @client.get_job(path)
    @client.list_jobs(:all)
    @client.add_job_keys(path, ['/marsell/stor/foo', '/marsell/stor/falafel'])
    sleep(5)
    @client.get_job_input(path)
    @client.get_job_output(path)
    @client.get_job_failures(path)
    @client.get_job_errors(path)
    @client.cancel_job(path)

    @client.gen_signed_url(Time.now + 500000, :get, '/marsell/stor/foo', [[ 'bar', 'beep' ]])
  end
end
