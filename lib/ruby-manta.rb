# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#
# ruby-manta is a simple low-abstraction layer which communicates with Joyent's
# Manta service.
#
# Manta is an HTTP-accessible object store supporting UNIX-based map-reduce
# jobs. Through ruby-manta a programmer can save/overwrite/delete objects
# stored on a Manta service, or run map-reduce jobs over those objects.
#
# ruby-manta should be thread-safe, and supports pooling of keep-alive
# connections to the same server (through HTTPClient). It only relies on the
# standard library and two pure Ruby libraries, so it should work anywhere.
#
# For more information about Manta and general ruby-manta usage, please see
# README.md.



require 'openssl'
require 'net/ssh'
require 'httpclient'
require 'base64'
require 'time'
require 'json'
require 'cgi'

require File.expand_path('../version', __FILE__)



class MantaClient
  DEFAULT_ATTEMPTS        = 3
  DEFAULT_CONNECT_TIMEOUT = 5
  DEFAULT_SEND_TIMEOUT    = 60
  DEFAULT_RECEIVE_TIMEOUT = 60
  MAX_LIMIT        = 1000
  HTTP_AGENT       = "ruby-manta/#{LIB_VERSION} (#{RUBY_PLATFORM}; #{OpenSSL::OPENSSL_VERSION}) ruby/#{RUBY_VERSION}-p#{RUBY_PATCHLEVEL}"
  HTTP_SIGNATURE   = 'Signature keyId="/%s/keys/%s",algorithm="%s",signature="%s"'
  OBJ_PATH_REGEX   = Regexp.new('^/.+/(?:stor|public|reports)(?:/|$)')
  JOB_PATH_REGEX   = Regexp.new('^/.+?/jobs/.+?(?:/live|$)')

  # match one or more protocol and hostnames, with optional port numbers.
  # E.g. "http://example.com https://example.com:8443"
  CORS_ORIGIN_REGEX  = Regexp.new('^\w+://[^\s\:]+(?:\:\d+)?' +
                                  '(?:\s\w+://[^\s\:]+(?:\:\d+)?)*$')
  CORS_HEADERS_REGEX = Regexp.new('^[\w-]+(?:, [\w-]+)*$')
  CORS_METHODS       = [ 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS' ]

  ERROR_CLASSES    = [ 'AuthorizationFailed', 'AuthSchemeNotAllowed',
                       'BadRequest', 'Checksum', 'ConcurrentRequest',
                       'ContentLength', 'ContentMD5Mismatch',
                       'DirectoryDoesNotExist', 'DirectoryExists',
                       'DirectoryNotEmpty', 'DirectoryOperation',
                       'EntityExists', 'Internal', 'InvalidArgument',
                       'InvalidAuthToken', 'InvalidCredentials',
                       'InvalidDurabilityLevel', 'InvalidJob', 'InvalidKeyId',
                       'InvalidLink', 'InvalidSignature', 'InvalidJobState',
                       'JobNotFound', 'JobState', 'KeyDoesNotExist',
                       'LinkNotFound', 'LinkNotObject', 'LinkRequired',
                       'NotAcceptable', 'NotEnoughSpace', 'ParentNotDirectory',
                       'PreconditionFailed', 'PreSignedRequest',
                       'RequestEntityTooLarge', 'ResourceNotFound',
                       'RootDirectory', 'ServiceUnavailable',
                       'SourceObjectNotFound', 'SSLRequired', 'TaskInit',
                       'UploadTimeout', 'UserDoesNotExist', 'UserTaskError',
                       # and errors that are specific to this class:
                       'CorruptResult', 'UnknownError',
                       'UnsupportedKey' ]



  # Initialize a MantaClient instance.
  #
  # priv_key_data is data read directly from an SSH private key (i.e. RFC 4716
  # format). The method can also accept several optional args: :connect_timeout,
  # :send_timeout, :receive_timeout, :disable_ssl_verification and :attempts.
  # The timeouts are in seconds, and :attempts determines the default number of
  # attempts each method will make upon receiving recoverable errors.
  #
  # Will throw an exception if given a key whose format it doesn't understand.
  def initialize(host, user, priv_key_data, opts = {})
    raise ArgumentError unless host =~ /^https{0,1}:\/\/.*[^\/]/
    raise ArgumentError unless user.is_a?(String) && user.size > 0

    @host        = host
    @user        = user

    @attempts = opts[:attempts] || DEFAULT_ATTEMPTS
    raise ArgumentError unless @attempts > 0

    if priv_key_data =~ /BEGIN RSA/
      @digest      = OpenSSL::Digest::SHA1.new
      @digest_name = 'rsa-sha1'
      algorithm    = OpenSSL::PKey::RSA
    elsif priv_key_data =~ /BEGIN DSA/
      @digest      = OpenSSL::Digest::DSS1.new
      @digest_name = 'dsa-sha1'
      algorithm    = OpenSSL::PKey::DSA
    else
      raise UnsupportedKeyError
    end

    @priv_key    = algorithm.new(priv_key_data)
    @fingerprint = OpenSSL::Digest::MD5.hexdigest(@priv_key.to_blob).
                                        scan(/../).join(':')

    @client = HTTPClient.new
    @client.connect_timeout = opts[:connect_timeout] || DEFAULT_CONNECT_TIMEOUT
    @client.send_timeout    = opts[:send_timeout   ] || DEFAULT_SEND_TIMEOUT
    @client.receive_timeout = opts[:receive_timeout] || DEFAULT_RECEIVE_TIMEOUT
    @client.ssl_config.verify_mode = nil if opts[:disable_ssl_verification]

    @job_base  = '/' + user + '/jobs'
  end



  # Uploads object data to Manta to the given path, along with a computed MD5
  # hash.
  #
  # The path must start with /<user>/stor or /<user/public. Data can be any
  # sequence of octets. The HTTP Content-Type stored on Manta can be set
  # with an optional :content_type argument; the default is
  # application/octet-stream. The number of distributed replicates of an object
  # stored in Manta can be set with an optional :durability_level; the default
  # is 2.
  #
  # Returns true along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def put_object(obj_path, data, opts = {})
    url = obj_url(obj_path)

    opts[:data] = data
    headers = gen_headers(opts)

    cors_headers = gen_cors_headers(opts)
    headers = headers.concat(cors_headers)

    durability_level = opts[:durability_level]
    if durability_level
      raise ArgumentError unless durability_level > 0
      headers.push([ 'Durability-Level', durability_level ])
    end

    content_type = opts[:content_type]
    if content_type
      raise ArgumentError unless content_type.is_a? String
      headers.push([ 'Content-Type', content_type ])
    end

    attempt(opts[:attempts]) do
      result = @client.put(url, data, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if [204, 304].include? result.status
      raise_error(result)
    end
  end



  # Get an object from Manta at a given path, and checks it's uncorrupted.
  #
  # The path must start with /<user>/stor or /<user/public and point at an
  # actual object, as well as output objects for jobs. :head => true can
  # optionally be passed in to do a HEAD instead of a GET.
  #
  # Returns the retrieved data along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_object(obj_path, opts = {})
    url     = obj_url(obj_path)
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      method = opts[:head] ? :head : :get
      result = @client.send(method, url, nil, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 200
        return true, result.headers if method == :head

        sent_md5     = result.headers['Content-MD5']
        received_md5 = base64digest(result.body)
        raise CorruptResult if sent_md5 != received_md5

        return result.body, result.headers
      elsif result.status == 304
        return nil, result.headers
      end

      raise_error(result)
    end
  end



  # Deletes an object off Manta at a given path.
  #
  # The path must start with /<user>/stor or /<user/public and point at an
  # actual object.
  #
  # Returns true along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def delete_object(obj_path, opts = {})
    url     = obj_url(obj_path)
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      result = @client.delete(url, nil, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 204
      raise_error(result)
    end
  end



  # Creates a directory on Manta at a given path.
  #
  # The path must start with /<user>/stor or /<user/public.
  #
  # Returns true along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def put_directory(dir_path, opts = {})
    url = obj_url(dir_path)
    headers = gen_headers(opts)
    headers.push([ 'Content-Type', 'application/json; type=directory' ])

    cors_headers = gen_cors_headers(opts)
    headers = headers.concat(cors_headers)

    attempt(opts[:attempts]) do
      result = @client.put(url, nil, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 204
      raise_error(result)
    end
  end



  # Gets a lexicographically sorted directory listing on Manta at a given path,
  #
  # The path must start with /<user>/stor or /<user/public and point at an
  # actual directory. :limit optionally changes the maximum number of entries;
  # the default is 1000. If given :marker, an object name in the directory,
  # returned directory entries will begin from that point. :head => true can
  # optionally be passed in to do a HEAD instead of a GET.
  #
  # Returns an array of hash objects, each object representing a directory
  # entry. Also returns the received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def list_directory(dir_path, opts = {})
    url     = obj_url(dir_path)
    headers = gen_headers(opts)
    query_parameters = {}

    limit = opts[:limit] || MAX_LIMIT
    raise ArgumentError unless 0 < limit && limit <= MAX_LIMIT
    query_parameters[:limit] = limit

    marker = opts[:marker]
    if marker
      raise ArgumentError unless marker.is_a? String
      query_parameters[:marker] = marker
    end

    attempt(opts[:attempts]) do
      method = opts[:head] ? :head : :get
      result = @client.send(method, url, query_parameters, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 200
        raise unless result.headers['Content-Type'] ==
                     'application/x-json-stream; type=directory'

        return true, result.headers if method == :head

        json_chunks = result.body.split("\n")
        sent_num_entries = result.headers['Result-Set-Size'].to_i

        if (json_chunks.size != sent_num_entries && json_chunks.size != limit) ||
           json_chunks.size > limit
          raise CorruptResult
        end

        dir_entries = json_chunks.map { |i| JSON.parse(i) }

        return dir_entries, result.headers
      end

      raise_error(result)
    end
  end



  # Removes a directory from Manta at a given path.
  #
  # The path must start with /<user>/stor or /<user/public and point at an
  # actual object.
  #
  # Returns true along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def delete_directory(dir_path, opts = {})
    url     = obj_url(dir_path)
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      result = @client.delete(url, nil, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 204
      raise_error(result)
    end
  end



  # Creates a snaplink from one object in Manta at a given path to a different
  # path.
  #
  # Both paths should start with /<user>/stor or /<user/public.
  #
  # Returns true along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def put_snaplink(orig_path, link_path, opts = {})
    headers = gen_headers(opts)
    headers.push([ 'Content-Type', 'application/json; type=link' ],
                 [ 'Location',     obj_url(orig_path)            ])

    attempt(opts[:attempts]) do
      result = @client.put(obj_url(link_path), nil, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 204
      raise_error(result)
    end
  end



  # Creates a job in Manta.
  #
  # The job must be a hash, containing at minimum a :phases key. See README.md
  # or the Manta docs to see the format and options for setting up a job on
  # Manta; this method effectively just converts the job hash to JSON and sends
  # to the Manta service.
  #
  # Returns the path for the new job, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def create_job(job, opts = {})
    raise ArgumentError unless job[:phases] || job['phases']

    headers = gen_headers(opts)
    headers.push([ 'Content-Type', 'application/json; type=job' ])
    data = job.to_json

    attempt(opts[:attempts]) do
      result = @client.post(job_url(), data, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 201
        location = result.headers['Location']
        raise unless location

        return location, result.headers
      end

      raise_error(result)
    end
  end



  # Gets various information about a job in Manta at a given path.
  #
  # The path must start with /<user>/jobs/<job UUID> and point at an actual job.
  # :head => true can optionally be passed in to do a HEAD instead of a GET.
  #
  # Returns a hash with job information, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_job(job_path, opts = {})
    url     = job_url(job_path, '/live/status')
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      method = opts[:head] ? :head : :get
      result = @client.send(method, url, nil, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 200
        raise unless result.headers['Content-Type'] == 'application/json'

        return true, result.headers if method == :head

        job = JSON.parse(result.body)
        return job, result.headers
      end

      raise_error(result)
    end
  end



  # Gets errors that occured during the execution of a job in Manta at a given
  # path.
  #
  # The path must start with /<user>/jobs/<job UUID> and point at an actual job.
  # :head => true can optionally be passed in to do a HEAD instead of a GET.
  #
  # Returns an array of hashes, each hash containing information about an
  # error; this information is best-effort by Manta, so it may not be complete.
  # Also returns received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_job_errors(job_path, opts = {})
    url     = job_url(job_path, '/live/err')
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      method = opts[:head] ? :head : :get
      result = @client.send(method, url, nil, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 200
        raise unless result.headers['Content-Type'] ==
                     'application/x-json-stream; type=job-error'

        return true, result.headers if method == :head

        json_chunks = result.body.split("\n")
        errors = json_chunks.map { |i| JSON.parse(i) }

        return errors, result.headers
      end

      raise_error(result)
    end
  end



  # Cancels a running job in Manta at a given path.
  #
  # The path must start with /<user>/jobs/<job UUID> and point at an actual job.
  #
  # Returns true, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def cancel_job(job_path, opts = {})
    url     = job_url(job_path, '/live/cancel')
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      result = @client.post(url, nil, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 202
      raise_error(result)
    end
  end



  # Adds objects for a running job in Manta to process.
  #
  # The job_path must start with /<user>/jobs/<job UUID> and point at an actual
  # running job. The obj_paths must be an array of paths, starting with
  # /<user>/stor or /<user>/public, pointing at actual objects.
  #
  # Returns true, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def add_job_keys(job_path, obj_paths, opts = {})
    url = job_url(job_path, '/live/in')
    headers = gen_headers(opts)
    headers.push([ 'Content-Type', 'text/plain' ])

    data = obj_paths.join("\n")

    attempt(opts[:attempts]) do
      result = @client.post(url, data, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 204
      raise_error(result)
    end
  end



  # Inform Manta that no more objects will be added for processing by a job,
  # and that the job should finish all phases and terminate.
  #
  # The job_path must start with /<user>/jobs/<job UUID> and point at an actual
  # running job.
  #
  # Returns true, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def end_job_input(job_path, opts = {})
    url     = job_url(job_path, '/live/in/end')
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      result = @client.post(url, nil, headers)
      raise unless result.is_a? HTTP::Message

      return true, result.headers if result.status == 202
      raise_error(result)
    end
  end



  # Get a list of objects that have been given to a Manta job for processing.
  #
  # The job_path must start with /<user>/jobs/<job UUID> and point at an actual
  # running job.
  #
  # Returns an array of object paths, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_job_input(job_path, opts = {})
    get_job_state_streams(:in, job_path, opts)
  end



  # Get a list of objects that contain the intermediate results of a running
  # Manta job.
  #
  # The job_path must start with /<user>/jobs/<job UUID> and point at an actual
  # running job.
  #
  # Returns an array of object paths, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_job_output(job_path, opts = {})
    get_job_state_streams(:out, job_path, opts)
  end



  # Get a list of objects that had failures during processing in a Manta job.
  #
  # The job_path must start with /<user>/jobs/<job UUID> and point at an actual
  # running job.
  #
  # Returns an array of object paths, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_job_failures(job_path, opts = {})
    get_job_state_streams(:fail, job_path, opts)
  end



  # Get lists of Manta jobs.
  #
  # The state indicates which kind of jobs to return. :running is for jobs
  # that are currently processing, :done and :all should be obvious. Be careful
  # of the latter two if you've run a lot of jobs -- the list could be quite
  # long.
  #
  # Returns an array of hashes, each hash containing some information about a
  # job. Also returns received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def list_jobs(state, opts = {})
    raise ArgumentError unless [:all, :running, :done].include? state
    state = nil if state == :all

    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
#      method = opts[:head] ? :head : :get
      method = :get # until added to Manta service
      result = @client.send(method, job_url(), { :state => state }, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 200
#        return true, result.headers if method == :head
        return [],   result.headers if result.body.size == 0

        raise unless result.headers['Content-Type'] ==
                     'application/x-json-stream; type=job'

        json_chunks = result.body.split("\n")
        job_entries = json_chunks.map { |i| JSON.parse(i) }

        return job_entries, result.headers
      end

      raise_error(result)
    end
  end



  # Generates a signed URL which can be used by unauthenticated users to
  # make a request to Manta at the given path. This is typically used to GET
  # an object.
  #
  # expires is a Time object or integer representing time after epoch; this
  # determines how long the signed URL will be valid for. The method is the HTTP
  # method (:get, :put, :post, :delete) the signed URL is allowed to be used
  # for. The path must start with /<user>/stor. Lastly, the optional args is an
  # array containing pairs of query args that will be appended at the end of
  # the URL.
  #
  # The returned URL is signed, and can be used either over HTTP or HTTPS until
  # it reaches the expiry date.
  def gen_signed_url(expires, method, path, args=[])
    raise ArgumentError unless [:get, :put, :post, :delete].include? method
    raise ArgumentError unless path =~ OBJ_PATH_REGEX

    key_id = '/%s/keys/%s' % [@user, @fingerprint]

    args.push([ 'expires',   expires.to_i ])
    args.push([ 'algorithm', @digest_name ])
    args.push([ 'keyId',     key_id       ])

    encoded_args = args.sort.map do |key, val|
      # to comply with RFC 3986
      CGI.escape(key.to_s) + '=' + CGI.escape(val.to_s)
    end.join('&')

    method = method.to_s.upcase
    host   = @host.split('/').last

    plaintext = "#{method}\n#{host}\n#{path}\n#{encoded_args}"
    signature = @priv_key.sign(@digest, plaintext)
    encoded_signature = CGI.escape(strict_encode64(signature))

    host + path + '?' + encoded_args + '&signature=' + encoded_signature
  end



  # Create some Manta error classes
  class MantaClientError < StandardError; end
  for class_name in ERROR_CLASSES
    MantaClient.const_set(class_name, Class.new(MantaClientError))
  end



  # ---------------------------------------------------------------------------
  protected



  # Fetch lists of objects that have a given status.
  #
  # type takes one of three values (:in, :out, fail), path must start with
  # /<user>/jobs/<job UUID> and point at an actual job.
  #
  # Returns an array of object paths, along with received HTTP headers.
  #
  # If there was an unrecoverable error, throws an exception. On connection or
  # corruption errors, more attempts will be made; the number of attempts can
  # be altered by passing in :attempts.
  def get_job_state_streams(type, path, opts)
    raise ArgumentError unless [:in, :out, :fail].include? type

    url     = job_url(path, '/live/' + type.to_s)
    headers = gen_headers(opts)

    attempt(opts[:attempts]) do
      #method = opts[:head] ? :head : :get
      method = :get # until added to Manta service
      result = @client.send(method, url, nil, headers)
      raise unless result.is_a? HTTP::Message

      if result.status == 200
        raise unless result.headers['Content-Type'] == 'text/plain'
        return true, result.headers if method == :head
        paths = result.body.split("\n")
        return paths, result.headers
      end

      raise_error(result)
    end
  end



  # Returns a full URL for a given path to an object.
  def obj_url(path)
    raise ArgumentError unless path =~ OBJ_PATH_REGEX

    @host + path
  end



  # Returns a full URL for a given path to a job.
  def job_url(*args)
    path = if args.size == 0
             @job_base
           else
             raise ArgumentError unless args.first =~ JOB_PATH_REGEX
             args.join('/')
           end

    @host + path
  end



  # Executes a block. If there is a connection- or corruption-related exception
  # the block will be reexecuted up to the `tries' argument. It will sleep
  # for an exponentially-increasing number of seconds between retries.
  def attempt(tries, &blk)
    if tries
      raise ArgumentError unless tries > 0
    else
      tries ||= @attempts
    end

    attempt = 1

    while true
      begin
        return yield blk
      rescue Errno::ECONNREFUSED, HTTPClient::TimeoutError,
             CorruptResult => e
        raise e if attempt == tries
        sleep 2 ** attempt
        attempt += 1
      end
    end
  end



  # Creates headers to be given to the HTTP client and sent to the Manta
  # service. The most important is the Authorization header, without which
  # none of this class would work.
  def gen_headers(opts)
    now = Time.now.httpdate
    sig = gen_signature('date: ' + now)

    headers = [[ 'Date',           now        ],
               [ 'Authorization',  sig        ],
               [ 'User-Agent',     HTTP_AGENT ],
               [ 'Accept-Version', '~1.0'     ]]


     # headers for conditional requests (dates)
     for arg, conditional in [[:if_modified_since,   'If-Modified-Since'  ],
                              [:if_unmodified_since, 'If-Unmodified-Since']]
      date = opts[arg]
      next unless date

      date = Time.parse(date.to_s) unless date.kind_of? Time
      headers.push([conditional, date])
    end

    # headers for conditional requests (etags)
    for arg, conditional in [[:if_match,      'If-Match'     ],
                             [:if_none_match, 'If-None-Match']]
      etag = opts[arg]
      next unless etag

      raise ArgumentError unless etag.kind_of? String
      headers.push([conditional, etag])
    end

    origin = opts[:origin]
    if origin
      raise ArgumentError unless origin == 'null' || origin =~ CORS_ORIGIN_REGEX
      headers.push([ 'Origin',  origin ])
    end

    # add md5 hash when sending data
    data = opts[:data]
    if data
      md5 = base64digest(data)
      headers.push([ 'Content-MD5', md5 ])
    end

    return headers
  end



  # Do some sanity checks and create CORS-related headers
  #
  # For more details, see http://www.w3.org/TR/cors/ and
  # https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS#Access-Control-Expose-Headers
  def gen_cors_headers(opts)
    headers = []

    allow_credentials = opts[:access_control_allow_credentials]
    if allow_credentials
      allow_credentials = allow_credentials.to_s
      raise ArgumentError unless allow_credentials == 'true' ||
                                 allow_credentials == 'false'
      headers.push([ 'Access-Control-Allow-Credentials', allow_credentials ])
    end

    allow_headers = opts[:access_control_allow_headers]
    if allow_headers
      raise ArgumentError unless allow_headers =~ CORS_HEADERS_REGEX
      allow_headers = allow_headers.split(', ').map(&:downcase).sort.join(', ')
      headers.push([ 'Access-Control-Allow-Headers', allow_headers ])
    end

    allow_methods = opts[:access_control_allow_methods]
    if allow_methods
      raise ArgumentError unless allow_methods.kind_of? String

      unknown_methods = allow_methods.split(', ').reject do |str|
                          CORS_METHODS.include? str
                        end
      raise ArgumentError unless unknown_methods.size == 0

      headers.push([ 'Access-Control-Allow-Methods', allow_methods ])
    end

    allow_origin = opts[:access_control_allow_origin]
    if allow_origin
      raise ArgumentError unless allow_origin.kind_of? String
      raise ArgumentError unless allow_origin == '*' ||
                                 allow_origin == 'null' ||
                                 allow_origin =~ CORS_ORIGIN_REGEX
      headers.push([ 'Access-Control-Allow-Origin', allow_origin ])
    end

    expose_headers = opts[:access_control_expose_headers]
    if expose_headers
      raise ArgumentError unless expose_headers =~ CORS_HEADERS_REGEX
      expose_headers = expose_headers.split(', ').map(&:downcase).sort.join(', ')
      headers.push([ 'Access-Control-Expose-Headers', expose_headers ])
    end

    max_age = opts[:access_control_max_age]
    if max_age
      raise ArgumentError unless max_age.kind_of?(Integer) && max_age >= 0
      headers.push([ 'Access-Control-Max-Age', max_age.to_s ])
    end

    headers
  end



  # Given a chunk of data, creates an HTTP signature which the Manta service
  # understands and uses for authentication.
  def gen_signature(data)
    raise ArgumentError unless data

    sig = @priv_key.sign(@digest, data)
    base64sig = strict_encode64(sig)

    return HTTP_SIGNATURE % [@user, @fingerprint, @digest_name, base64sig]
  end



  # Raises an appropriate exception given the HTTP response. If a 40* is
  # returned, attempts to look up an appropriate error class and raise,
  # otherwise raises an UnknownError.
  def raise_error(result)
    raise unless result.is_a? HTTP::Message

    err   = JSON.parse(result.body)
    klass = MantaClient.const_get err['code']
    raise klass, err['message']
  rescue NameError, TypeError, JSON::ParserError
    raise UnknownError, result.status.to_s + ': ' + result.body
  end



  # Ruby 1.8 is missing 1.9's strict_encode64, so we have this instead
  def strict_encode64(str)
    Base64.encode64(str).tr("\n",'')
  end



  # Ruby 1.8 is missing 1.9's base64digest, so we have this instead
  def base64digest(str)
    md5 = OpenSSL::Digest::MD5.digest(str)
    strict_encode64(md5)
  end
end

