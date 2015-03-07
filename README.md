ruby-manta
==========



What's ruby-manta?
------------------

ruby-manta is a client for communicating with Manta. Manta is a RESTful service,
so ruby-manta is effectively an HTTP(S) wrapper which handles required HTTP
headers and performs some sanity checks. ruby-manta seeks to expose all of
Manta's features in a thin low-abstraction client.



What's Manta?
-------------

Manta is a RESTful replicated object store with a directory structure,
emphasizing Consistency and Partition Tolerance (C & P) in Brewer's CAP,
which differs from the usual Dynamo-derivative choice of Availability and
Partition Tolerance (A & P). This makes reasoning about Manta simpler (reads
after writes will _always_ return the newest version), and it supports CAS
semantics, but it also means that HTTP 500s may temporarily be returned for
some objects on rare occasion.

Manta also provides a map-reduce service operating over objects stored in
Manta. The most notable feature here is that map and reduce phases operate
fully within a UNIX environment. Standard utilities, language environments
(e.g. node.js, Python, Ruby, Perl), precompiled binaries (e.g. LuxRender,
BLAST, Postgres, your own custom binaries) and anything you can run in a
standard UNIX environment can be used.



Streaming Large Objects
-----------------------

One important limitation of ruby-manta is that it is not designed to handle
large objects. Specifically, when used to upload or download a large object,
that entire object will be loaded into Ruby's heap. If you're trying to move
multi-gigabyte objects using a sub-gigabyte VPS or zone, that won't work.
This leads to the following observations:

* don't upload or download large objects using ruby-manta
* if you must move large objects, consider compressing them first
* if they're still large, consider using node-manta instead.

Unlike ruby-manta, node-manta (the Node.js API for Manta) streams, so object
size is not a limitation. If you intend to work with large objects, use
node-manta instead. An slight alternative is to use node-manta for uploading
and downloading objects, and ruby-manta for everything else.



Example
-------

If you're like the author, examples are worth reams of explanation. Here,
hurried friend, is an example demonstrating some of ruby-manta's usage:

````` ruby
    require 'ruby-manta'

    # You'll need to provide these four environment variables to run this
    # example. E.g.:
    # USER=john KEY=~/.ssh/john HOST=https://us-east.manta.joyent.com DIR=. \
    #   ruby example.rb
    host       = ENV['HOST']
    user       = ENV['USER']
    priv_key   = ENV['KEY' ]
    upload_dir = ENV['DIR' ]

    # Read in private key, create a MantaClient instance. MantaClient is
    # thread-safe and provides persistent connections with pooling, so you'll
    # only ever need a single instance of this in a program.
    priv_key_data = File.read(priv_key)
    client = MantaClient.new(host, user, priv_key_data,
                             :disable_ssl_verification => true)

    # Create an directory in Manta solely for this example run.
    dir_path = '/' + user + '/stor/ruby-manta-example'
    client.put_directory(dir_path)

    # Upload files in a local directory to the Manta directory.
    file_paths = Dir[upload_dir + '/*'].select { |p| File.file? p }
    file_paths.each do |file_path|
      file_name = File.basename(file_path)
      # Be careful about binary files and file encodings in Ruby 1.9. If you don't
      # use ASCII-8BIT (forced by 'rb' below), expect timeouts while PUTing an
      # object.
      file_data = File.open(file_path, 'rb') { |f| f.read }
      client.put_object(dir_path + '/' + file_name, file_data)
    end

    # This example job runs the wc UNIX command on every object for the
    # map phase, then uses awk during reduce to sum up the three numbers each wc
    # returned.
    job_details = {
      :name => 'total word count',
      :phases => [ {
        :exec => 'wc'
      }, {
        :type => 'reduce',
        :exec => "awk '{ l += $1; w += $2; c += $3 } END { print l, w, c }'"
      } ]
    }

    # Create the job, then add the objects the job should operate on.
    job_path, _ = client.create_job(job_details)

    entries, _ = client.list_directory(dir_path)
    obj_paths = entries.select { |e| e['type'] == 'object' }.
                        map { |e| dir_path + '/' + e['name'] }

    client.add_job_keys(job_path, obj_paths)

    # Tell Manta we're done adding objects to the job. Manta doesn't need this
    # to start running a job -- you can see map results without it, for
    # example -- but reduce phases in particular depend on all mapping
    # finishing.
    client.end_job_input(job_path)

    # Poll until Manta finishes the job.
    begin
      sleep 1
      job, _ = client.get_job(job_path)
    end while job['state'] != 'done'

    # We know in this case there will be only one result. Fetch it and
    # display it.
    results, _ = client.get_job_output(job_path)
    data, _ = client.get_object(results[0])
    puts data

    # Clean up; remove objects and directory.
    obj_paths.each do |obj_path|
      client.delete_object(obj_path)
    end

    client.delete_directory(dir_path)
`````


NB: there's no catching of exceptions above! Real production code should
be prepared for the exceptional -- see "a note on semantics" below.

If you see a PEM pass phrase request, that's because you're using an
encrypted private key. In production on a server, you'd presumably use an
unencrypted key.



Installation
------------

If you're one of the chaps using Ruby 1.9 or higher, life is easy:

    gem install ruby-manta

Done.

Ruby 1.8.7 was end-of-life'd on June, 2013. As a result, ruby-manta no longer
supports it either.



Public and Private spaces
-------------------------

ruby-manta operates with Manta paths. An example of a path is:

    /john/stor/image.png

This object, image.png, belongs to the user "john". It's in his private space,
"stor". But what if John wants to let the ~~unwashed hoi poll~~general public to
see his image.png? In this case there is also the "public" space:

    /john/public/image.png



Signed URLs
-----------

Objects put in the public space are accessible by everyone. Objects in the
private space are only accessible by individuals authenticated and authorized
by Manta. Manta also supports temporary signed URLs that allow unauthenticated
individuals to operate on private objects, until the link expires. See
gen_signed_url() below for the details.



Map/Reduce Jobs
---------------

Alas, this is beyond the scope of this document. Please refer to Manta's
documentation for details on how to construct a job. ruby-manta passes job
details directly to Manta, so what you see is what you'll get.

Short summary: create a job, add paths to objects the job should operate on,
then close the job. Poll until Manta tells you the job is finished, then peek
at the resulting objects.



The API
=======

A note on sematics
------------------

All methods throw exceptions upon failure. If a method doesn't throw, the
returned results are valid. The most common category of failure you'll see
inherit from the MantaClient::MantaClientError class. If you feed ruby-manta
an argument it doesn't like, it'll throw ArgumentError. You might also see
Errno::ECONNREFUSED and HTTPClient::TimeoutError exceptions from the underlying
HTTPClient class.

Most methods take paths to Manta objects or jobs. Object (or directory) paths
are typically of the forms:

    /<user>/stor/<directory>*/<object>
    /<user>/public/<directory>*/<object>
    /<user>/jobs/.../stor/...

The last one is a path for an intermediate or final object generated by a job.
Job paths are simpler:

    /<user>/jobs/<job UUID>

MantaClient methods perform some basic sanity checks to prevent you from
using malformed paths, or mixing object/directory paths and job paths.
ArgumentError exceptions are immediately thrown if a bad path is provided,
otherwise you'll likely receive a MantaClient::ResourceNotFound exception
(inherits from MantaClient::MantaClientError) after the call fails in Manta.

All method calls, except for get_signed_url(), can take an optional :attempts.
By default they use the :attempts from the constructor (which default to three
tries), but the number of times a Manta call is attempted upon certain failures
can be overridden on an individual basis.

Lastly, recall that due to Manta's semantics you may see 500 errors on occasion.
When that happens, try again after a minute or three.



Conditional requests
--------------------

Operations on objects support conditional requests. Pass in as an optional
argument :if_modified_since, :if_unmodified_since, :if_match, or :if_none_match
to the method. For example, to conditionally get an object with etag
"e346dce6-22f3-4ed5-8191-6b059b3684de":

````` ruby

    client.get_object(path, :if_match => 'e346dce6-22f3-4ed5-8191-6b059b3684de')
`````

You can get the Etag or Last-Modified from the headers returned by most
methods.

The methods follow the RFC2616 semantics in the following manner: where 304
would return instead of data, a nil returns instead. Where 412 would occur,
a MantaClient::PreconditionFailed is thrown.

Conditional requests allow many good things, so you're advised to use them
where applicable. You can conditionally download an object only if it has
changed, update an object with CAS semantics, create snaplinks correctly in the
face of concurrent updates, and so forth.



Cross-origin resource sharing
-----------------------------

Browsers do not allow cross-domain requests due to the same-origin policy.
Cross-Origin Resource Sharing (CORS) headers provide a mechanism by which a
browser can safely loosen this policy.

ruby-manta and Manta support all headers specified by the W3C working draft,
by passing in optional arguments to put_object() or put_directory():

:access_control_allow_credentials, :access_control_allow_headers,
:access_control_allow_methods, :access_control_allow_origin,
:access_control_expose_headers, :access_control_max_age

You can also pass in :origin to most object- and directory-related methods.



initialize(manta_host, user, priv_key, _options_)
-------------------------------------------------

Construct a new MantaClient instance.

priv_key_data is data read directly from an SSH private key (i.e. RFC 4716
format). The method can also accept several optional args: :connect_timeout,
:send_timeout, :receive_timeout, :disable_ssl_verification and :attempts.
The timeouts are in seconds, and :attempts determines the default number of
attempts each method will make upon receiving recoverable errors.

Will throw an exception if given a key whose format it doesn't understand.

MantaClient is thread-safe (in theory, anyway), and uses an HTTP client that
pools connections. You should only need to initialize a single MantaClient
object per process.

Example:

````` ruby

    priv_key_data = File.read('/home/john/.ssh/john')
    client = MantaClient.new('https://manta.joyentcloud.com', 'john',
                             priv_key_data, :disable_ssl_verification => true)
`````



put_object(object path, file data, _options_)
---------------------------------------------

Uploads object data to Manta to the given path, along with a computed MD5
hash.

The path must be a valid object path. Data can be any sequence of octets.
The HTTP Content-Type stored on Manta can be set with an optional
:content_type argument; the default is application/octet-stream. The
number of distributed replicates of an object stored in Manta can be set
with an optional :durability_level; the default is 2. Supports CORS optional
arguments.

Returns true along with received HTTP headers.

Examples:

````` ruby

    obj_path, headers = client.put_object('/john/stor/area51_map.png',
                                          binary_file_data,
                                          :content_type        => 'image/png',
                                          :durability_level    => 1,
                                          :if_unmodified_since => Time.now - 300,
                                          :access_control_allow_origin => 'http://example.com')

    obj_path, _ = client.put_object('/john/public/pass.txt', 'illuminati 4evah')
`````



get_object(object path, _options_)
----------------------------------

Get an object from Manta at a given path, and checks it's uncorrupted.

The object path must point at an actual Manta object. :head => true can
optionally be passed in to do a HEAD instead of a GET.

Returns the retrieved data along with received HTTP headers.

Examples:

````` ruby

    _, headers = client.get_object('/john/stor/area51_map.png',
                                   :head   => true,
                                   :origin => 'https://illuminati.org')

    file_data, headers = client.get_object('/john/stor/area51_map.png')
`````



delete_object(object path, _options_)
-------------------------------------

Deletes an object off Manta at a given path.

The object path must point at an actual object.

Returns true along with received HTTP headers.

Examples:

````` ruby

    client.delete_object('/john/stor/area51_map.png')

    _, headers = client.delete_object('/john/public/pass.txt')
`````



put_directory(dir path, _options_)
----------------------------------

Creates a directory on Manta at a given path. Supports CORS optional arguments.

Returns true along with received HTTP headers.

Example:

````` ruby

    client.put_directory('/john/stor/plans-world-domination')

    client.put_directory('/john/public/honeypot',
                        :access_control_allow_methods => 'GET, PUT, DELETE',
                        :access_control_allow_origin => '*')
`````



list_directory(dir_path, _options_)
-----------------------------------

Gets a lexicographically sorted directory listing on Manta at a given path,

The path must be a valid directory path and point at an actual directory.
:limit optionally changes the maximum number of entries; the default is 1000.
If given :marker, an object name in the directory, returned directory entries
will begin from that point. :head => true can optionally be passed in to do a
HEAD instead of a GET.

Returns an array of hash objects, each object representing a directory
entry. Also returns the received HTTP headers.

Examples:

````` ruby

    dir_entries, _ = client.list_directory('/john/stor/plans-world-domination',
                                           :limit  => 50,
                                           :marker => 'take_over_pentagon.txt')

    _, headers = client.list_directory('/john/stor/plans-world-domination',
                                       :head => true)
`````



find(dir_path, _options_)
-----------------------

Finds all Manta objects underneath a given path,

The path must be a valid directory path and point at an actual directory.

The path must be a valid directory path and point at an actual directory.
:limit optionally changes the maximum number of entries; the default is 1000.
If given :marker, an object name in the directory, returned directory entries
will begin from that point. :regex => can optionally be passed in to filter
filenames by a given regular expression.


delete_directory(dir_path, _options_)
-------------------------------------

Removes a directory from Manta at a given path.

The path must be a valid directory path, and point at an actual directory.
The directory must be empty.

Returns true along with received HTTP headers.

Example:

````` ruby

    client.delete_directory('/john/stor/plans-world-domination')
`````



put_snaplink(orig_path, link_path, _options_)
-----------------------------------------

Creates a snaplink from on object in Manta at a given path to a different path.
This effectively creates another reference to the same object. Since objects
are immutable, PUTting over that object reduces the refcount on the object;
other references (i.e. snaplinks) continue to see the original version.

Both paths should be valid object paths. orig_path should point at an existing
object.

Returns true along with received HTTP headers.

Example:

````` ruby

    client.put_snaplink('/john/stor/character_assassination.txt',
                        '/john/public/media_consultation.txt')
`````



create_job(job_description, _options_)
--------------------------------------

Creates a job in Manta.

The job must be a hash, containing at minimum a :phases key. See README.md
or the Manta docs to see the format and options for setting up a job on
Manta; this method effectively just converts the job hash to JSON and sends
to the Manta service.

Returns the path for the new job, along with received HTTP headers.

Example:

````` ruby

    job_desc = { :phases => [{ :exec => 'grep skynet' }] }
    job_path, _ = client.create_job(job_desc)
`````



get_job(job_path, _options_)
----------------------------

Gets various information about a job in Manta at a given job path.

The path must point at an actual job. :head => true can optionally be passed
in to do a HEAD instead of a GET.

Returns a hash with job information, along with received HTTP headers.

Example:

````` ruby

    job_path = '/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7'
    job_info, _ = client.get_job(job_path)
`````



get_job_errors(job_path, _options_)
-----------------------------------

Gets errors that occured during the execution of a job in Manta at a given
job path.

The must point at an actual job. :head => true can optionally be passed in to
do a HEAD instead of a GET.

Returns an array of hashes, each hash containing information about an
error; this information is best-effort by Manta, so it may not be complete.
Also returns received HTTP headers.

Examples:

````` ruby

    job_path = '/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7'
    job_errors, _ = client.get_job_errors(job_path)

    _, headers = client.get_job_errors(job_path, :head => true)
`````



cancel_job(job_path, _options_)
-------------------------------

Cancels a running job in Manta at a given path.

The job path must point at an actual job.

Returns true along with received HTTP headers.

Example:

````` ruby

    client.cancel_job('/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7')
`````



add_job_keys(job_path, object_keys, _options_)
----------------------------------------------

Adds objects for a running job in Manta to process.

The job_path must point at an actual running job. The obj_paths must be an
array of object paths pointing at actual objects.

Returns true, along with received HTTP headers.

Example:

````` ruby

    client.add_job_keys('/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7',
                        ['/john/stor/skynet_plans.txt',
                         '/john/stor/the_matrix.txt'])
`````



end_job_input(job_path, _options_)
----------------------------------

Inform Manta that no more objects will be added for processing by a job,
and that the job should finish all phases and terminate.

The job path must point at an actual running job.

Returns true, along with received HTTP headers.

Example:

````` ruby

    client.end_job_input('/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7')
`````




get_job_input(job_path, _options_)
----------------------------------

Get a list of objects that have been given to a Manta job for processing.

The job path must point at an actual running job.

Returns an array of object paths, along with received HTTP headers.

Example:

````` ruby

    job_path = '/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7'
    obj_paths, _ = client.get_job_input(job_path)
`````



get_job_output(job_path, _options_)
-----------------------------------

Get a list of objects that contain the intermediate and/or final results of a
running Manta job.

The job_path must point at an actual running job.

Returns an array of object paths, along with received HTTP headers.

Example:

````` ruby

    job_path = '/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7'
    obj_paths, _ = client.get_job_output(job_path)
`````



get_job_failures(job_path, _options_)
-------------------------------------

Get a list of objects that had failures during processing in a Manta job.

The job path must point at an actual running job.

Returns an array of object paths, along with received HTTP headers.

Example:

````` ruby

    job_path = '/john/jobs/80e481c4-8567-47e7-bdba-f0c5705af1c7'
    obj_failures, _ = client.get_job_failures(job_path)
`````



list_jobs(state, _options_)
---------------------------

Get a list of Manta jobs.

The state indicates which kind of jobs to return. :running is for jobs
that are currently processing, :done and :all should be obvious. Be careful
of the latter two if you've run a lot of jobs -- the list could be quite
long.

Returns an array of hashes, each hash containing some information about a job.
Also returns received HTTP headers.

Example:

````` ruby

    running_jobs, _ = client.list_jobs(:running)
`````



gen_signed_url(expiry_date, http_method, path, _query_args_)
----------------------------------------------------------

Generates a signed URL which can be used by unauthenticated users to
make a request to Manta at the given path. This is typically used to GET
an object.

expires is a Time object or integer representing time after epoch; this
determines how long the signed URL will be valid for. The method is either a
single HTTP method (:get, :put, :post, :delete, :options) or a list of such
methods that the signed URL is allowed to be used for. The path must start
with /<user>/stor. Lastly, the optional args is an array containing pairs of
query args that will be appended at the end of the URL.

The returned URL is signed, and can be used either over HTTP or HTTPS until
it reaches the expiry date.

Example:

````` ruby

    url = client.gen_signed_url(Time.now + 5000, :get, '/john/stor/pass.txt')
`````



License
=======

(c) 2012 Joyent, licensed under MIT. See LICENSE for details, you legal geek
you.

