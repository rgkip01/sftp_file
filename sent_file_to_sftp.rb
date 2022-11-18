require "functions_framework"
require "json"
require "google/cloud/storage"
require 'net/sftp'
require 'fileutils'
require 'dotenv/load'
require "sinatra/base"


# This function receives an HTTP request of type Rack::Request
# and interprets the body as JSON. It prints the contents of
# the "message" field, or "Hello World!" if there isn't one.
FunctionsFramework.http "connect_sftp" do |request|
  App.call request.env
end

class App < Sinatra::Base 
  get '/' do
    check_and_sent_to_sftp
    200
  end

  def get_bucket(bucket_name)
    storage = Google::Cloud::Storage.new(
      project_id: ENV['PROJECT_ID'],
      credentials: "storage-account.json"
    )
    storage.bucket(bucket_name)
  end

  def check_and_sent_to_sftp
    bucket_unsent = get_bucket(ENV['TRANSMISSION_UNSET']).files 
    bucket_sent =  get_bucket(ENV['TRANSMISSION_SENT']).files
    
    unsets = []
    sents = []
    
    bucket_unsent.each do |file|
      unsets << file.name
    end

    bucket_sent.each do |file|
      sents << file.name
    end

    repeated_files = unsets.select{|i| sents.include?(i)}
    
    if repeated_files.empty?
      bucket_unsent.each do |file|
        connect_sftp_client(file.name)
        to_bucket_sent(file)
        remove_file(file)
      end
    else
      unless bucket_unsent.empty?
        bucket_unsent.each do |file|
          remove_file(file)
        end
      end
      puts "Arquivo já enviado..."
    end
  end
    
  def connect_sftp_client(filename)
    bucket = get_bucket(ENV['TRANSMISSION_UNSET'])
    found_file = bucket.file(filename)

    downloaded = found_file.download
    content_file = downloaded.read

    Net::SFTP.start(
      ENV['SFTP_CONFIGURATION_URL'],
      ENV['SFTP_CONFIGURATION_USERNAME'],
      :password => ENV['SFTP_CONFIGURATION_PASSWORD']
    ) do |sftp|
      sftp.file.open("/Arquivos_Producao/#{filename}", "w") do |f|
        f.puts(content_file)
      end
    end
  rescue Exception => err
    puts err.message
  end 
    
  def remove_file(filename)
    bucket = get_bucket(ENV['TRANSMISSION_UNSET'])
    file = bucket.file(filename.name)
    file.delete(generation: true)
  end

  def to_bucket_sent(filename)
    bucket = get_bucket(ENV['TRANSMISSION_UNSET'])
    file = bucket.file(filename.name)
    file.copy(ENV['TRANSMISSION_SENT'], "#{file.name}")
  end
end

