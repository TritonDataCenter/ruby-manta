require 'minitest/autorun'
require_relative '../../lib/ruby-manta'



##
# Test class that verifies that the MantaClient is still compatible with the
# 1.x.x API.
class TestDeprecatedMantaClientApi < Minitest::Test
  @@client = nil
  @@user   = nil

  def setup
    if ! @@client
      host   = ENV['MANTA_URL']
      key    = ENV['MANTA_KEY' ]
      @@user = ENV['MANTA_USER']

      unless host && key && @@user
        $stderr.puts 'Require HOST, USER and KEY env variables to run tests.'
        $stderr.puts 'E.g. MANTA_USER=john MANTA_KEY=~/.ssh/john MANTA_URL=https://us-east.manta.joyent.com bundle exec rake test'
        exit
      end

      priv_key_data = File.read(key)

      opts = {
          disable_ssl_verification: true
      }

      if ENV.key?('MANTA_SUBUSER')
        opts[:subuser] = ENV['MANTA_SUBUSER']
      end

      @@client = MantaClient.new(host, @@user, priv_key_data, opts)

      @@test_dir_path = '/%s/stor/ruby-manta-test' % @@user
    end

    teardown()

    @@client.put_directory(@@test_dir_path)
  end

  def test_put_object
    assert @@client.respond_to? :put_object
    assert -3, @@client.method(:put_object).arity
  end

  def test_get_object
    assert @@client.respond_to? :get_object
    assert -2, @@client.method(:get_object).arity
  end

  def test_delete_object
    assert @@client.respond_to? :delete_object
    assert -2, @@client.method(:delete_object).arity
  end

  def test_put_directory
    assert @@client.respond_to? :put_directory
    assert -2, @@client.method(:put_directory).arity
  end

  def test_list_directory
    assert @@client.respond_to? :list_directory
    assert -2, @@client.method(:list_directory).arity
  end

  def test_delete_directory
    assert @@client.respond_to? :delete_directory
    assert -2, @@client.method(:delete_directory).arity
  end

  def test_put_snaplink
    assert @@client.respond_to? :put_snaplink
    assert -3, @@client.method(:put_snaplink).arity
  end

  def test_create_job
    assert @@client.respond_to? :create_job
    assert -2, @@client.method(:create_job).arity
  end

  def test_get_job
    assert @@client.respond_to? :get_job
    assert -2, @@client.method(:get_job).arity
  end

  def test_get_job_errors
    assert @@client.respond_to? :get_job_errors
    assert -2, @@client.method(:get_job_errors).arity
  end

  def test_cancel_job
    assert @@client.respond_to? :cancel_job
    assert -2, @@client.method(:cancel_job).arity
  end

  def test_add_job_keys
    assert @@client.respond_to? :add_job_keys
    assert -3, @@client.method(:add_job_keys).arity
  end

  def test_end_job_input
    assert @@client.respond_to? :end_job_input
    assert -2, @@client.method(:end_job_input).arity
  end

  def test_get_job_input
    assert @@client.respond_to? :get_job_input
    assert -2, @@client.method(:get_job_input).arity
  end

  def test_get_job_output
    assert @@client.respond_to? :get_job_output
    assert -2, @@client.method(:get_job_output).arity
  end

  def test_get_job_failures
    assert @@client.respond_to? :get_job_failures
    assert -2, @@client.method(:get_job_failures).arity
  end

  def test_list_jobs
    assert @@client.respond_to? :list_jobs
    assert -2, @@client.method(:list_jobs).arity
  end

  def test_gen_signed_url
    assert @@client.respond_to? :gen_signed_url
    assert -2, @@client.method(:gen_signed_url).arity
  end
end
