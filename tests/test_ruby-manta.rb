require 'rubygems'  # for 1.8 compat
require 'minitest/autorun'
require 'httpclient'
require File.expand_path('../../lib/ruby-manta', __FILE__)



class TestMantaClient < MiniTest::Unit::TestCase
  @@client = nil
  @@user   = nil

  def setup
    if ! @@client
      host   = ENV['HOST']
      key    = ENV['KEY' ]
      @@user = ENV['USER']

      unless host && key && @@user
        $stderr.puts 'Require HOST, USER and KEY env variables to run tests.'
        $stderr.puts 'E.g. USER=john KEY=~/.ssh/john HOST=https://manta.joyent.com ruby tests/test_manta.rb'
        exit
      end

      priv_key_data = File.read(key)
      @@client = MantaClient.new(host, @@user, priv_key_data,
                                 :disable_ssl_verification => true)

      @@test_dir_path = '/%s/stor/ruby-manta-test' % @@user
    end

    teardown() 

    @@client.put_directory(@@test_dir_path)
  end



  def teardown
    listing, _ = @@client.list_directory(@@test_dir_path)
    listing.each do |entry, _|
      path = @@test_dir_path + '/' + entry['name']
      if entry['type'] == 'directory'
        @@client.delete_directory(path)
      else
        @@client.delete_object(path)
      end
    end

    @@client.delete_directory(@@test_dir_path)
  rescue MantaClient::ResourceNotFound
  end



  def test_paths
    def check(&blk)
      begin
        yield blk
        assert false
      rescue ArgumentError
      end
    end

    good_obj_path = "/#{@@user}/stor/ruby-manta-test"
    bad_obj_path  = "/#{@@user}/stora/ruby-manta-test"

    check { @@client.put_directory(bad_obj_path)            }
    check { @@client.put_object(bad_obj_path, 'asd')        }
    check { @@client.get_object(bad_obj_path)               }
    check { @@client.delete_object(bad_obj_path)            }
    check { @@client.put_directory(bad_obj_path)            }
    check { @@client.list_directory(bad_obj_path)           }
    check { @@client.delete_directory(bad_obj_path)         }
    check { @@client.put_link(good_obj_path, bad_obj_path)  }
    check { @@client.put_link(bad_obj_path,  good_obj_path) }

    good_job_path = "/#{@@user}/job/ruby-manta-test"
    bad_job_path  = "/#{@@user}/joba/ruby-manta-test"

    check { @@client.get_job(bad_job_path)                  }
    check { @@client.get_job_errors(bad_job_path)           }
    check { @@client.cancel_job(bad_job_path)               }
    check { @@client.add_job_keys(bad_job_path,  [good_obj_path]) }
    check { @@client.add_job_keys(good_job_path, [bad_obj_path])  }
    check { @@client.end_job_input(bad_job_path)            }
    check { @@client.get_job_input(bad_job_path)            }
    check { @@client.get_job_output(bad_job_path)           }
    check { @@client.get_job_failures(bad_job_path)         }
    check { @@client.gen_signed_url(Time.now, :get, bad_obj_path) }
  end



  def test_directories
    result, headers = @@client.put_directory(@@test_dir_path)
    assert_equal result, true
    assert headers.is_a? Hash

    result, headers = @@client.put_directory(@@test_dir_path + '/dir1')
    assert_equal result, true
    assert headers.is_a? Hash

    # since idempotent
    result, headers = @@client.put_directory(@@test_dir_path + '/dir1')
    assert_equal result, true
    assert headers.is_a? Hash

    result, headers = @@client.put_object(@@test_dir_path + '/obj1', 'obj1-data')
    assert_equal result, true
    assert headers.is_a? Hash

    result, headers = @@client.put_object(@@test_dir_path + '/obj2', 'obj2-data')
    assert_equal result, true
    assert headers.is_a? Hash

    result, headers = @@client.list_directory(@@test_dir_path)
    assert headers.is_a? Hash
    assert_equal result.size, 3

    assert_equal result[0]['name'], 'dir1'
    assert_equal result[0]['type'], 'directory'
    assert result[0]['mtime'].match(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/)

    assert_equal result[1]['name'], 'obj1'
    assert_equal result[1]['type'], 'object'
    assert_equal result[1]['size'], 9
    assert result[1]['mtime'].match(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/)

    assert_equal result[2]['name'], 'obj2'
    assert_equal result[2]['type'], 'object'
    assert_equal result[2]['size'], 9
    assert result[2]['mtime'].match(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/)

    result, _ = @@client.list_directory(@@test_dir_path, :limit => 2)
    assert_equal result.size, 2
    assert_equal result[0]['name'], 'dir1'
    assert_equal result[1]['name'], 'obj1'

    result, _ = @@client.list_directory(@@test_dir_path, :limit => 1)
    assert_equal result.size, 1
    assert_equal result[0]['name'], 'dir1'

    result, _ = @@client.list_directory(@@test_dir_path, :limit  => 2,
                                                       :marker => 'obj1')
    assert_equal result.size, 2
    assert_equal result[0]['name'], 'obj1'
    assert_equal result[1]['name'], 'obj2'

    result, headers = @@client.list_directory(@@test_dir_path, :head => true)
    assert_equal result, true
    assert_equal headers['Result-Set-Size'], '3'

    begin
      @@client.delete_directory(@@test_dir_path)
      assert false
    rescue MantaClient::DirectoryNotEmpty
    end

    @@client.delete_directory(@@test_dir_path + '/dir1')
    @@client.delete_object(@@test_dir_path + '/obj1')
    @@client.delete_object(@@test_dir_path + '/obj2')

    result, headers = @@client.delete_directory(@@test_dir_path)
    assert_equal result, true
    assert headers.is_a? Hash

    begin
      @@client.list_directory(@@test_dir_path + '/does-not-exist')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    begin
      @@client.put_directory(@@test_dir_path + '/dir1')
      assert false
    rescue MantaClient::DirectoryDoesNotExist
    end
  end



  def test_objects
    result, headers = @@client.put_object(@@test_dir_path + '/obj1', 'foo-data')
    assert_equal result, true
    assert headers.is_a? Hash

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'foo-data'
    assert_equal headers['Content-Type'], 'application/x-www-form-urlencoded'

    @@client.put_object(@@test_dir_path + '/obj1', 'bar-data',
                        :content_type     => 'application/wacky',
                        :durability_level => 3)

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'bar-data'
    assert_equal headers['Content-Type'], 'application/wacky'

    result, headers = @@client.get_object(@@test_dir_path + '/obj1', :head => true)
    assert_equal result, true
    assert_equal headers['Content-Type'], 'application/wacky'

    begin
      @@client.put_object(@@test_dir_path + '/obj1', 'bar-data',
                          :durability_level => 999)
      assert false
    rescue MantaClient::InvalidDurabilityLevel
    end

    begin
      @@client.get_object(@@test_dir_path + '/does-not-exist')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    begin
      @@client.delete_object(@@test_dir_path + '/does-not-exist')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    result, headers = @@client.delete_object(@@test_dir_path + '/obj1')
    assert_equal result, true
    assert headers.is_a? Hash
  end



  def test_public
    host = ENV['HOST'].gsub('https', 'http')
    test_pub_dir_path  = '/%s/public/ruby-manta-test' % @@user

    @@client.put_directory(test_pub_dir_path)
    @@client.put_object(test_pub_dir_path + '/obj1', 'foo-data')

    client = HTTPClient.new
    client.ssl_config.verify_mode = nil  # temp hack
    result = client.get(host + test_pub_dir_path + '/obj1')
    assert_equal result.body, 'foo-data'

    @@client.delete_object(test_pub_dir_path + '/obj1')
    @@client.delete_directory(test_pub_dir_path)
  end



  def test_cors
    cors_args = {
      :access_control_allow_credentials => true,
      :access_control_allow_headers     => 'X-Random, X-Bar',
      :access_control_allow_methods     => 'GET, POST, DELETE',
      :access_control_allow_origin      => 'https://example.com:1234 http://127.0.0.1',
      :access_control_expose_headers    => 'X-Last-Read, X-Foo',
      :access_control_max_age           => 30
    }

    @@client.put_object(@@test_dir_path + '/obj1', 'foo-data', cors_args)

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'foo-data'

    for name, value in [[ 'access-control-allow-methods',     'GET, POST, DELETE'  ],
                        [ 'access-control-allow-origin',      'https://example.com:1234 http://127.0.0.1' ],
                        [ 'access-control-expose-headers',    'x-foo, x-last-read' ],
                        [ 'access-control-max-age',           '30'                 ] ]
      assert_equal headers[name], value
    end

    result, headers = @@client.get_object(@@test_dir_path + '/obj1',
                                          :origin => 'https://example.com:1234')

    assert_equal result, 'foo-data'

    for name, value in [[ 'access-control-allow-methods',     'GET, POST, DELETE'  ],
                        [ 'access-control-allow-origin',      nil                  ],
                        [ 'access-control-expose-headers',    'x-foo, x-last-read' ],
                        [ 'access-control-max-age',           nil                  ]]
      assert_equal headers[name], value
    end

    @@client.put_directory(@@test_dir_path + '/dir', cors_args)

    result, headers = @@client.list_directory(@@test_dir_path + '/dir')

    for name, value in [[ 'access-control-allow-methods',     'GET, POST, DELETE'  ],
                        [ 'access-control-allow-origin',      'https://example.com:1234 http://127.0.0.1' ],
                        [ 'access-control-expose-headers',    'x-foo, x-last-read' ],
                        [ 'access-control-max-age',           '30'                 ] ]
      assert_equal headers[name], value
    end
  end



  def test_signed_urls
    @@client.put_object(@@test_dir_path + '/obj1', 'foo-data')

    url = @@client.gen_signed_url(Time.now + 500000, :get,
                                  @@test_dir_path + '/obj1')

    client = HTTPClient.new
    result = client.get('http://' + url)
    assert_equal result.body, 'foo-data'
  end



  def test_links
    begin
      @@client.put_link(@@test_dir_path + '/obj1', @@test_dir_path + '/obj2')
      assert false
    rescue MantaClient::SourceObjectNotFound
    end

    @@client.put_object(@@test_dir_path + '/obj1', 'foo-data')

    result, headers = @@client.put_link(@@test_dir_path + '/obj1',
                                        @@test_dir_path + '/obj2')
    assert_equal result, true
    assert headers.is_a? Hash

    @@client.put_object(@@test_dir_path + '/obj1', 'bar-data')
   
    result, _ = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'bar-data'
   
    result, _ = @@client.get_object(@@test_dir_path + '/obj2')
    assert_equal result, 'foo-data'
  end



  def test_conditionals_on_objects
    result, headers = @@client.put_object(@@test_dir_path + '/obj1', 'foo-data',
                                          :if_modified_since => Time.now)
    assert_equal result, true

    modified = headers['Last-Modified']
    assert modified

    sleep 2

    @@client.put_object(@@test_dir_path + '/obj1', 'bar-data',
                        :if_modified_since => modified)

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'foo-data'
    assert_equal headers['Last-Modified'], modified

    @@client.put_object(@@test_dir_path + '/obj1', 'bar-data',
                        :if_unmodified_since => modified)

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'bar-data'
    assert headers['Last-Modified'] != modified

    etag = headers['Etag']

    begin
      @@client.put_object(@@test_dir_path + '/obj1', 'foo-data',
                          :if_none_match => etag)
      assert false
    rescue MantaClient::PreconditionFailed
    end

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'bar-data'
    assert_equal headers['Etag'], etag

    @@client.put_object(@@test_dir_path + '/obj1', 'foo-data',
                        :if_match => etag)

    result, headers = @@client.get_object(@@test_dir_path + '/obj1')
    assert_equal result, 'foo-data'
    assert headers['Etag'] != etag

    begin
      @@client.get_object(@@test_dir_path + '/obj1', :if_match => etag)
      assert false
    rescue MantaClient::PreconditionFailed
    end

    etag     = headers['Etag']
    modified = headers['Last-Modified']

    result, headers = @@client.get_object(@@test_dir_path + '/obj1',
                                          :if_match => etag)
    assert_equal result, 'foo-data'
    assert_equal headers['Etag'], etag

    result, headers = @@client.get_object(@@test_dir_path + '/obj1',
                                          :if_none_match => etag)
    assert_equal result, nil
    assert_equal headers['Etag'], etag

    result, headers = @@client.get_object(@@test_dir_path + '/obj1',
                                          :if_none_match => 'blahblah')
    assert_equal result, 'foo-data'
    assert_equal headers['Etag'], etag

    begin
      @@client.put_link(@@test_dir_path + '/obj1', @@test_dir_path + '/obj2',
                        :if_none_match => etag)
      assert false
    rescue MantaClient::PreconditionFailed
    end

    result, headers = @@client.put_link(@@test_dir_path + '/obj1',
                                        @@test_dir_path + '/obj2',
                                        :if_match => etag)
    assert true
    assert_equal headers['Etag'], etag

# XXX Manta has odd semantics here. Omitting until fixed.
#    begin
#      @@client.put_link(@@test_dir_path + '/obj1', @@test_dir_path + '/obj3',
#                        :if_modified_since => modified)
#      assert false
#    rescue MantaClient::PreconditionFailed
#    end
# Placeholder for now:
    @@client.put_link(@@test_dir_path + '/obj1', @@test_dir_path + '/obj3')
#
#

    result, headers = @@client.put_link(@@test_dir_path + '/obj1',
                                        @@test_dir_path + '/obj4',
                                        :if_unmodified_since => modified)
    assert true

    modified = headers['Last Modified']

    begin
      @@client.delete_object(@@test_dir_path + '/obj1', :if_none_match => etag)
      assert false
    rescue MantaClient::PreconditionFailed
    end

    result, _ = @@client.delete_object(@@test_dir_path + '/obj1', :if_match => etag)
    assert_equal result, true

    sleep 1

    begin
      @@client.delete_object(@@test_dir_path + '/obj3', :if_unmodified_since => Time.now - 10000)
      assert false
    rescue MantaClient::PreconditionFailed
    end

# XXX Manta has odd semantics here. Omitting until fixed.
#    begin
#      @@client.delete_object(@@test_dir_path + '/obj3', :if_modified_since => Time.now)
#      assert false
#    rescue MantaClient::PreconditionFailed
#    end

    @@client.delete_object(@@test_dir_path + '/obj3', :if_unmodified_since => Time.now)
    @@client.delete_object(@@test_dir_path + '/obj4', :if_modified_since=> Time.now - 10000)


    for obj_name in ['/obj1', '/obj3', '/obj4']
      begin
        @@client.get_object(@@test_dir_path + obj_name)
        assert false
      rescue MantaClient::ResourceNotFound
      end
    end
  end



  # This test is definitely not pretty, but splitting it up will make it
  # take much longer due to the redundant creation of jobs. Perhaps that's
  # the wrong choice...
  def test_jobs
    result, headers = @@client.list_jobs(:running)
    assert headers.is_a? Hash

    result.each do |entry|
      path = '/%s/jobs/%s' % [ @@user, entry['id'] ]
      @@client.cancel_job(path)
    end

    begin
      @@client.create_job({})
      assert false
    rescue ArgumentError
    end

    result, headers = @@client.list_jobs(:running)
    assert_equal result, []
    assert headers.is_a? Hash

    path, headers  = @@client.create_job({ :phases => [{ :exec => 'grep foo' }] })
    assert path =~ Regexp.new('^/' + @@user + '/jobs/.+')
    assert headers.is_a? Hash

    result, headers  = @@client.cancel_job(path)
    assert_equal result, true
    assert headers.is_a? Hash

    path, _ = @@client.create_job({ :phases => [{ :exec => 'grep foo' }] })

    result, _ = @@client.list_jobs(:all)
    result.each do |job|
      assert ['done', 'running', 'queued'].include? job['state']
      assert job['id'] =~ /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
      assert job['name'       ].is_a? String
      assert job['phases'     ].is_a? Array
      assert job['cancelled'  ].is_a?(TrueClass) ||
             job['cancelled'  ].is_a?(FalseClass)
      assert job['timeCreated'].match(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\dZ$/)

      if job['timeDone']
        assert job['timeDone'].match(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\dZ$/)
      end
    end

    assert result.select { |r| r['status'] == 'running' }.size, 1

    begin
      @@client.list_jobs(:some)
      assert false
    rescue ArgumentError
    end

    jobs, _ = @@client.list_jobs(:running)
    assert_equal jobs.size, 1
    assert_equal jobs.first['state'], 'running'
    assert_equal jobs.first['id'], path.split('/').last

# Commented out until HEAD here by Manta
#    jobs, headers = @@client.list_jobs(:running, :head => true)
#    assert_equal jobs, true
#    assert_equal headers['Result-Set-Size'], 1

    job, headers = @@client.get_job(path)
    assert headers.is_a? Hash
    assert job['name'       ].is_a? String
    assert job['phases'     ].is_a? Array
    assert job['timeCreated'].match(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\dZ$/)
    assert_equal jobs.first['id'], path.split('/').last
    assert_equal job['state'    ], 'running'
    assert_equal job['cancelled'], false
    assert_equal job['timeDone' ], nil

    @@client.put_object(@@test_dir_path + '/obj1', 'foo-data')
    @@client.put_object(@@test_dir_path + '/obj2', 'bar-data')

    obj_key_paths = [@@test_dir_path + '/obj1',
                     @@test_dir_path + '/obj2',
                     @@test_dir_path + '/obj3']

    result, headers = @@client.add_job_keys(path, obj_key_paths)
    assert_equal result, true
    assert headers.is_a? Hash

    result, headers = @@client.get_job_input(path)
    assert_equal result, obj_key_paths
    assert headers.is_a? Hash

    begin
      @@client.get_job_input(path + 'a')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    begin
      @@client.get_job_output(path + 'a')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    begin
      @@client.get_job_failures(path + 'a')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    begin
      @@client.get_job_errors(path + 'a')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    begin
      @@client.end_job_input(path + 'a')
      assert false
    rescue MantaClient::ResourceNotFound
    end

    result, headers = @@client.end_job_input(path)
    assert_equal result, true
    assert headers.is_a? Hash

    for i in (1...10)
      job, _ = @@client.get_job(path)
      break if job['state'] == 'done'
      sleep 1
    end

    result, headers = @@client.get_job_output(path)
    assert headers.is_a? Hash

    result, _ = @@client.get_object(result.first)
    assert_equal result, "foo-data\n"

    result, headers = @@client.get_job_failures(path)
    assert_equal result, obj_key_paths.slice(1, 2)
    assert headers.is_a? Hash

    result, headers = @@client.get_job_errors(path)
    assert headers.is_a? Hash

    obj2_result = result[0]
    obj3_result = result[1]

    assert obj2_result['id']
    assert obj2_result['what']
    assert_equal obj2_result['code'   ], 'EJ_USER'
    assert_equal obj2_result['message'], 'user command exited with status 1'
    assert_equal obj2_result['key'    ], obj_key_paths[1]
    assert_equal obj2_result['phase'  ], 0

    assert obj3_result['id']
    assert obj3_result['what']
    assert_equal obj3_result['code'   ], 'EJ_NOENT'
    assert obj3_result['message'] =~ /^no such object/
    assert_equal obj3_result['key'    ], obj_key_paths[2]
    assert_equal obj3_result['phase'  ], 0

    begin
      @@client.cancel_job(path)
      assert fail
    rescue MantaClient::InvalidJobState
    end
  end
end
