require 'rubygems'
require 'sinatra'
require 'java'
require 'json'

jars = Dir["lib/*.jar"]
jars.each{|j| require j}

$CLASSPATH << '/etc'
java.lang.System.set_property('eureka.client.props', 'lipstick')

configure do
  root = File.expand_path(File.dirname(__FILE__))
  set :public_folder, File.join(root, 'app', 'public')
  set :protection, :except => [:json_csrf]
end

# Load the helpers
load "app/helpers/elasticsearch.rb" # load this first

Dir["app/helpers/*.rb"].each { |file| load file }

# Load the controllers.
Dir["app/controllers/*.rb"].each { |file| load file }

at_exit do
  PlanService.close
end
