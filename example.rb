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
