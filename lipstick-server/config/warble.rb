Warbler::Config.new do |config|
  config.dirs = %w(app config lib)
  config.includes = FileList["init.rb"]
  config.excludes = FileList["lib/*jruby*.jar"]
  config.gems -= ["rails"]
  config.jar_name = "pigstats"
end
