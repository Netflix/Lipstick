import 'java.util.Properties'
import 'java.util.concurrent.TimeUnit'
import 'java.util.concurrent.Executors'
import 'java.util.concurrent.ScheduledExecutorService'

import 'org.elasticsearch.client.transport.TransportClient'
import 'org.elasticsearch.common.settings.ImmutableSettings'
import 'org.elasticsearch.common.transport.InetSocketTransportAddress'
import 'org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus'

import 'com.netflix.discovery.DiscoveryManager'
import 'com.netflix.appinfo.InstanceInfo'
import 'com.netflix.appinfo.DataCenterInfo'
import 'com.netflix.appinfo.MyDataCenterInstanceConfig'

class Elasticsearch
  attr_accessor :client, :es_server_set, :checker_handle
  attr_accessor :config, :cluster_name, :port
  
  @@es_server_set = Set.new
  @@scheduler     = Executors.new_scheduled_thread_pool(1)                                                                

  def initialize conf
    @config       = conf
    @cluster_name = conf.get("cluster.name").to_s
    @port         = conf.get("transport.tcp.port").to_i
    
    DiscoveryManager.get_instance.init_component(
      MyDataCenterInstanceConfig.new('netflix.appinfo.', DataCenter.new(conf)),
      com.netflix.discovery.DefaultEurekaClientConfig.new()
      )
  end  

  class RegistryUpdater
    attr_accessor :es
    include java.lang.Runnable
    def initialize es
      @es = es
    end
    
    def run
      es.update_registry
    end    
  end

  class DataCenter
    include DataCenterInfo
    attr_accessor :name
    def initialize props
      @name = props.get('eureka.datacenter').to_s
    end
    
    def getName
      case @name
      when 'myown' then
        DataCenterInfo::Name::MyOwn
      when 'amazon' then
        DataCenterInfo::Name::Amazon
      when 'netflix' then
        DataCenterInfo::Name::Netflix
      end      
    end    
  end
  
  
  def start
    @client = TransportClient.new(ImmutableSettings.settingsBuilder().put(@config).build)
    update_registry
    @checker_handle = @@scheduler.schedule_at_fixed_rate(RegistryUpdater.new(self), 60, 60, TimeUnit::SECONDS)    
  end

  def update_registry
    begin
      instances = DiscoveryManager.get_instance.get_lookup_service
        .get_application(@cluster_name).get_instances
      
      new_server_set = Set.new
      instances.each do |instance|
        new_server_set << instance.get_host_name if instance.get_status == InstanceInfo::InstanceStatus::UP
      end

      (@@es_server_set - new_server_set).each do |removed|
        @client.remove_transport_address(InetSocketTransportAddress.new(removed, @port))
        @@es_server_set.delete(removed)
      end

      (new_server_set - @@es_server_set).each do |added|
        if healthy?(added, @config, @port)
          $stderr.puts("Using Elasticsearch host: [#{added}]")
          @client.add_transport_address(InetSocketTransportAddress.new(added, @port));
          @@es_server_set << added
        end        
      end

    rescue => e
      $stderr.puts("Error updating registry")
      $stderr.puts(e.backtrace)
    end
  end

  def healthy? host, props, port
    local_client = nil
    begin
      local_client = TransportClient.new(ImmutableSettings.settings_builder.put(props).build)
        .add_transport_address(InetSocketTransportAddress.new(host, port))
      health_status = local_client.admin.cluster.prepare_health.set_timeout('1000').execute.get.get_status
      if health_status == ClusterHealthStatus::GREEN || health_status == ClusterHealthStatus::YELLOW
        return true
      else
        return false
      end      
    rescue => e
      return false
    ensure
      local_client.close if local_client
    end    
  end

  def shutdown
    @client.close
    @checker_handle.cancel(true)
  end
end
