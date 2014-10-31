require 'init'

set :run, false
set :environment, :production

# deploy httpd server
run Sinatra::Application
