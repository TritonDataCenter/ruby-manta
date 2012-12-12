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


Example
-------

If you're like the author, examples are worth reams of explanation. Here,
hurried friend, is an example demonstrating some of ruby-manta's usage:

    require 'ruby-manta'

    # You'll need to provide these four environment variables to run this
    # example. E.g.:
    # USER=john KEY=~/.ssh/john HOST=https://manta.joyent.com DIR=. ruby example.rb
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
      file_data = File.read(file_path)
      client.put_object(dir_path + '/' + file_name, file_data)
    end

    # This example job runs the wc UNIX command on every object for the
    # map phase, then uses awk during reduce to sum up the three numbers each wc
    # returned.
    job_details = {
      :jobName => 'total word count',
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


NB: there's no catching of exceptions above! Real production code should
be prepared for the exceptional -- see "a note on semantics" below.

If you see a PEM pass phrase request, that's because you're using an
encrypted private key. In production on a server, you'd presumably use an
unencrypted key.


Installation
------------

If you're one of the chaps using Ruby 1.9.*, life is easy:

    gem install ruby-manta-1.0.0.gem

Done.

If you're of a more conservative bent and are using Ruby 1.8.*, there might
be a harmless HTTPClient RDoc error; ignore it. To complete the installation
for 1.8, also run:

    gem install net-ssh json_pure minitest


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
an argument it doesn't like, it'll throw ArgumentError. Lastly, you might see
Errno::ECONNREFUSED and HTTPClient::TimeoutError exceptions from the underlying
HTTPClient class.

Lastly, recall that due to Manta's semantics you may on occasionally see 500
errors. When that happens, try again after a minute or three.

--

Working on it. My apologies. In the meantime, please refer to the comments
above each method in the source. Yes, it actually has comments!


License
=======

(c) 2012 Joyent, licensed under MIT. See LICENSE for details, you legal geek
you.

