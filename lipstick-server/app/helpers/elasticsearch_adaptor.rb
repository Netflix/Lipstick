import 'java.util.Properties'
import 'org.elasticsearch.node.NodeBuilder'
import 'org.elasticsearch.search.sort.SortOrder'
import 'org.elasticsearch.index.query.QueryBuilders'
import 'org.elasticsearch.index.query.FilterBuilders'
import 'org.elasticsearch.common.settings.ImmutableSettings'
import 'org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest'

class ElasticSearchAdaptor
  attr_accessor :node, :client, :conf, :index

  @@config = '/etc/lipstick.properties'
  
  def initialize
    if ENV["RACK_ENV"] == 'development'
      puts "creating dev client"
      @index  = 'lipstick'
      @client = dev_client
    else      
      @conf  = read_config(@@config)
      @index = conf.get('lipstick.index').to_s
    end
    create_index      
  end
    
  def client
    @client ||= get_client
  end

  def dev_client
    es_settings = ImmutableSettings.settings_builder
      .put("node.master", true)
      .put("node.data", true)
      .put("index.store.type", "ram")
      .put("path.data", ".es-data")
    @node = NodeBuilder.node_builder.settings(es_settings).node
    node.client
  end
  
  
  def read_config config    
    props = Properties.new
    ins   = File.open(config).to_inputstream
    props.load(ins)
    props
  end

  def index_exists?
    client.admin.indices.exists(IndicesExistsRequest.new(@index)).action_get.is_exists?
  end
  
  def create_index    
    if !index_exists?
      plan_mapping     = File.read(File.join('config', 'plan-mapping.json'))
      graph_mapping    = File.read(File.join('config', 'graph-mapping.json'))
      template_mapping = File.read(File.join('config', 'template-mapping.json'))
      
      # create plan type
      client.admin.indices.prepare_create(@index).add_mapping('plan', plan_mapping).execute.action_get

      # create plan type
      client.admin.indices.prepare_put_mapping(@index).set_type('graph').set_source(graph_mapping).execute.action_get

      #create node_template
      client.admin.indices.prepare_put_mapping(@index).set_type('node_template').set_source(template_mapping).execute.action_get     
    end    
  end  
  
  def get_client
    discovery_type = (conf.get('discovery.type') || 'list').to_s
    if (discovery_type == 'eureka')
      es = ::Elasticsearch.new(conf)
      es.start
      return es.client
    elsif (discovery_type == 'zen')
      es_settings = ImmutableSettings.settings_builder
        .put("cluster.name", conf.get("cluster.name").to_s)
        .put("node.master", false)
        .put("node.data", false)
      @node = NodeBuilder.node_builder.settings(es_settings).node
      return @node.client
    else
      urls = conf.get("elasticsearch.urls").to_s.split(",")
      addresses = urls.map do |url|
        url = url.split(":", 2)
        InetSocketTransportAddress.new(url.first, url.last.to_i)
      end
      es_settings = ImmutableSettings.settings_builder.put("cluster.name", conf.get_property("cluster.name"))
      return addresses.inject(TransportClient.new(es_settings)) {|c, a| c.add_transport_address(a) }
    end        
  end

  def refresh!
    client.admin.indices.prepare_refresh(@index).execute.action_get
  end
  
  @@instance = ElasticSearchAdaptor.new

  def self.instance
    @@instance
  end
  
  def save doc_id, type, json
    begin
      response = client.prepare_index(@index, type, doc_id)
        .set_source(json)
        .execute
        .action_get
      return response
    rescue => e
      $stderr.puts("Error trying to save: [#{$!}]")
      $stderr.puts(e.backtrace)
      return
    end    
  end

  def fix_empty_counters response
    json = JSON.parse(response)
    return response unless (json['status'] && json['status']['jobStatusMap'])
    
    jsm    = json['status']['jobStatusMap']
    newjsm = {}
    jsm.each do |job_id, job_status|          
      newjsm[job_id] = job_status
      newjsm[job_id]['counters'] ||= {}
    end
    json['status']['jobStatusMap'] = newjsm
    return JSON.generate(json)
  end

  #
  # FIXME: When we move to using new spec only on the
  # frontend, this will go away
  #
  def adapt_node_to_old node
    {
      :alias     => node['properties']['alias'],
      :location  => {:line => -1, :macro => []},
      :mapReduce => {
        :jobId    => node['properties']['jobId'],
        :stepType => node['properties']['stepType']
      },
      :operator => node['properties']['operation'],
      :schema   => node['properties']['schema'],
      :predecessors => [],
      :successors   => []
    }
  end
  
  #
  # FIXME: When we move to using new spec only on the
  # frontend, this will go away
  #
  def adapt_new_to_old response
    json = JSON.parse(response)
    
    # return unless new graph spec    
    return response unless (json['nodes'] && json['edges'])

    json['uuid']      = (json['id'] || json['uuid'])
    json['jobName']   = (json['properties']['jobName'] || "new-graph-"+json['uuid'])
    json['userName']  = (json['properties']['userName'] || 'user')
    if json['properties']['status']
      json['status'] = json['properties']['status']
    else
      json['status'] = {
        'progress'  => 0,
        'startTime' => 0,
        'heartbeatTime' => 0,
        'statusText' => 'running'
      }
    end    

    if json['properties']['script']
      json['scripts'] = {
        'script' => json['properties']['script']
      }
    else
      json['scripts'] = {
        'script' => ''
      }
    end
    
    json['optimized'] = {
      'plan' => json['nodes'].inject({}){|plan,node| plan[node['id']] = adapt_node_to_old(node); plan},
      'svg'  => json['svg']
    }

    json['unoptimized'] = {
      'plan' => json['nodes'].inject({}){|plan,node| plan[node['id']] = adapt_node_to_old(node); plan},
      'svg'  => json['svg']
    }
    return JSON.generate(json)
  end  
  
  def get uuid, type
    begin
      response = client.prepare_get(@index, type, uuid).execute.action_get
      r = response.get_source_as_string
      return unless r
      # handle special case with null counters
      return fix_empty_counters(r)
    rescue => e
      $stderr.puts("Error trying to get document [#{uuid}]: [#{$!}]")
      $stderr.puts(e.backtrace)
      return
    end    
  end

  def get_fields uuid, fields
    begin
      response = client.prepare_get(@index, 'plan', uuid)
        .set_fetch_source(fields.to_java(:string), [].to_java(:string))
        .execute.action_get
      
      r = response.get_source_as_string                  
      return unless r      
      return fix_empty_counters(r)      
    rescue => e
      $stderr.puts("Error trying to get fields for document [#{uuid}]: [#{$!}]")
      $stderr.puts(e.backtrace)
      return
    end    
  end

  def list_templates

    fields = [
      "name", "view","template"
    ]
    
    builder = client.prepare_search(@index)
      .set_types('node_template')
      .add_fields(*fields)

    begin
      response = builder.execute.action_get
      
      hits = response.get_hits
      ret = hits.hits.map do |hit|
        hit_fields = hit.get_fields
        {
          :name     => hit_fields.get("name").value,
          :view     => hit_fields.get("view").value,
          :template => hit_fields.get("template").value
        }
      end
      
      return {:templates => ret, :total => ret.size}
    rescue => e
      $stderr.puts("Error trying to list templates: [#{$!}]")
      $stderr.puts(e.backtrace)
      return
    end        
  end
  
  def list_graphs params
    max    = [params[:max] ? params[:max].to_i : 10, 100].min
    sort   = params[:sort]   || "updated_at"
    offset = params[:offset] ? params[:offset].to_i : 0

    order = SortOrder::DESC
    if (params[:order] && params[:order] == "asc")
      order = SortOrder::ASC
    end
    
    fields = [
      "name", "id", "created_at","updated_at"
    ]

    builder = client.prepare_search(@index)
      .set_types('graph')
      .add_fields(*fields)

    if (params[:search] && !params[:search].empty?)
      q = QueryBuilders.boolQuery()
      # FIXME - this will NOT scale well; need to use ngrams for partial
      # matching. See:
      # http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_ngrams_for_partial_matching.html
      #
      q.should(QueryBuilders.regexp_query("name", ".*#{params[:search]}.*"))
      #
      builder = builder.set_query(q)
    else
      builder = builder.set_query(QueryBuilders.match_all_query)
    end    
    
    begin
      response = builder.set_from(offset).set_size(max)
        .add_sort(sort, order)
        .execute.action_get
      
      hits = response.get_hits
      ret = hits.hits.map do |hit|
        hit_fields   = hit.get_fields
        {          
          :id         => hit_fields.get("id").value,
          :name       => hit_fields.get("name").value,
          :created_at => hit_fields.get("created_at").value,
          :updated_at => hit_fields.get("updated_at").value
        }                      
      end
      
      return {:jobs => ret, :jobsTotal => hits.get_total_hits}
    rescue => e
      $stderr.puts("Error trying to list graphs: [#{$!}]")
      $stderr.puts(e.backtrace)
      return
    end        
  end

  def list_plans params
    max    = [params[:max] ? params[:max].to_i : 10, 100].min
    sort   = params[:sort]   || "startTime"
    offset = params[:offset] ? params[:offset].to_i : 0

    order = SortOrder::DESC
    if (params[:order] && params[:order] == "asc")
      order = SortOrder::ASC
    end
    
    case sort
    when "startTime" then
      sort = "status.startTime"
    when "heartbeatTime" then
      sort = "status.heartbeatTime"
    when "user" then
      sort = "userName"
    when "progress" then
      sort = "status.progress"
    end

    fields = [
      "uuid","userName","jobName",
      "status.progress","status.statusText",
      "status.heartbeatTime", "status.startTime"
    ]

    builder = client.prepare_search(@index)
      .set_types('plan')
      .add_fields(*fields)

    if (params[:search] && !params[:search].empty?)
      q = QueryBuilders.boolQuery()
      # FIXME - this will NOT scale well; need to use ngrams for partial
      # matching. See:
      # http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_ngrams_for_partial_matching.html
      #
      q.should(QueryBuilders.regexp_query("userName", ".*#{params[:search]}.*"))
      q.should(QueryBuilders.regexp_query("jobName", ".*#{params[:search]}.*"))
      #
      builder = builder.set_query(q)
    else
      builder = builder.set_query(QueryBuilders.match_all_query)
    end    
    
    if (params[:status] && !params[:status].empty?) # status filter
      q = QueryBuilders.match_query("status.statusText", params[:status])
      builder = builder.set_post_filter(FilterBuilders.query_filter(q))
    end

    begin
      response = builder.set_from(offset).set_size(max)
        .add_sort(sort, order)
        .execute.action_get
      
      hits = response.get_hits
      ret = hits.hits.map do |hit|
        hit_fields   = hit.get_fields
        {
          :userName => hit_fields.get("userName").value,
          :uuid     => hit_fields.get("uuid").value,
          :jobName  => hit_fields.get("jobName").value,
          :progress => hit_fields.get("status.progress").value,
          :status   => {
            :name     => hit_fields.get("status.statusText").value,
            :enumType => "com.netflix.lipstick.model.P2jPlanStatus$StatusText"
          },
          :heartbeatTime => hit_fields.get("status.heartbeatTime").value,
          :startTime     => hit_fields.get("status.startTime").value
        }                      
      end
      
      return {:jobs => ret, :jobsTotal => hits.get_total_hits}
    rescue => e
      $stderr.puts("Error trying to list plans: [#{$!}]")
      $stderr.puts(e.backtrace)
      return
    end        
  end
  
  def close
    @client.close
    if @node
      @node.close
    end    
  end
  
end
