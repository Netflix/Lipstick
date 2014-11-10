class JobControllerTest < Test::Unit::TestCase
  include Rack::Test::Methods
  
  def app
    Sinatra::Application
  end

  def setup
    @graph = {
      "id"   => "doublynested",
      "name" => "doubly-nested-graph",
      "created_at" => 1415215545000,
      "updated_at" => 1415215545000,
      "nodes" => [
        {"id" => "optimized-a", "properties" => {"alias" => "one", "operation" =>"operation-A", "step_type"  => "mapper"}},
        {"id" => "optimized-b", "properties" => {"alias" => "two", "operation" =>"operation-B", "step_type"  => "mapper"}},
        {"id" => "unoptimized-a", "properties" => {"alias" => "one", "operation" =>"operation-A", "step_type"  => "mapper"}},
        {"id" => "unoptimized-b", "properties" => {"alias" => "two", "operation" =>"operation-B", "step_type"  => "mapper"}},
        {"id" => "optimized-job1", "child" => "1"},
        {"id" => "optimized-job2", "child" => "2"},
        {"id" => "unoptimized-job1", "child" => "3"},
        {"id" => "unoptimized-job2", "child" => "4"},    
        {"id" => "optimized", "child" => "5"},
        {"id" => "unoptimized", "child" => "6"} 
      ],
      "edges" => [
        {"u" => "optimized-a", "v" => "optimized-b",
          "properties" =>{
            "sampleOutput" =>["a\u00011\nb\u00012"],
            "schema" =>[
              {"type" =>"CHARARRAY", "alias" =>"name"},
              {"type" =>"INTEGER", "alias" =>"value"}
            ]
          }
        },
        {"u" => "unoptimized-a", "v" => "unoptimized-b"}
      ],
      "node_groups" => [
        {"id" => "1", "children" => ["optimized-a"]},
        {"id" => "2", "children" => ["optimized-b"]},
        {"id" => "3", "children" => ["unoptimized-a"]},
        {"id" => "4", "children" => ["unoptimized-b"]},
        {"id" => "5", "children" => ["optimized-job1","optimized-job2"]},
        {"id" => "6", "children" => ["unoptimized-job1","unoptimized-job2"]}
      ]
    }
    @status_update = {
      "node_groups" => [
        {"id" => "3", "status" => {
            "progress" => 10,
            "statusText" => "running",
            "startTime" => 1415215545000,
            "heartbeatTime" => 1415215545000,
            "endTime" => -1
          }
        }
      ],
      "nodes" => [
        {"id" => "optimized-a", "status" => {
            "statusText" => "borken"
          }
        }
      ]
    }
    @expected_list_response = {
      'jobs' => [{
          'id'   => @graph['id'],
          'name' => @graph['name'],
          'created_at' => @graph['created_at']
        }],
      'jobsTotal' => 1
    }
    @expected_post_response = {
      'id' => @graph['id']
    }
    @expected_put_response = {
      'id' => @graph['id']
    }
    @node_update = {
      'id' => 'optimized-a',
      'status' => {'statusText' => 'failed'}
    }
    @node_group_update = {
      'id' => '3',
      'status' => {'statusText' => 'failed'}
    }
    @edge_update = {
      'u' => 'unoptimized-a',
      'v' => 'unoptimized-b',
      'properties' => {
        'sampleOutput' => ['a\u00011\nb\u00012'],
        'schema' => [
          {'type' => 'CHARARRAY', 'alias' => 'name'},
          {'type' => 'INTEGER', 'alias' => 'value'}
        ]
      }
    }
    PlanService.save_graph({}, @graph.to_json)
    ElasticSearchAdaptor.instance.refresh!
  end

  def test_should_list_jobs
    get '/v1/job'
    assert last_response.ok?
    response     = JSON.parse(last_response.body)
    first_job    = @expected_list_response['jobs'].first
    response_job = response['jobs'].first 
    assert_equal @expected_list_response['jobsTotal'], response['jobsTotal']
    assert_equal first_job['id'], response_job['id']
    assert_equal first_job['name'], response_job['name']
    assert_equal first_job['created_at'], response_job['created_at']
    assert_operator response_job['updated_at'], :>=, first_job['created_at']
  end

  def test_should_post_jobs
    post '/v1/job', @graph.to_json
    assert last_response.ok?
    assert_equal @expected_post_response, JSON.parse(last_response.body)
  end

  def test_update_job
    put '/v1/job/'+@graph['id'], @status_update.to_json
    assert last_response.ok?
    assert_equal @expected_put_response, JSON.parse(last_response.body)
    
    ElasticSearchAdaptor.instance.refresh!
    get '/v1/job/'+@graph['id']
    assert last_response.ok?

    updated = JSON.parse(last_response.body)

    expected_ng     = @status_update['node_groups'].first
    expected_status = expected_ng['status']
    expected_node   = @status_update['nodes'].first
    
    ng   = updated['node_groups'].find{|x| x['id'] == expected_ng['id']}
    node = updated['nodes'].find{|x| x['id'] == expected_node['id']}

    assert_equal(
      [expected_status['startTime'], expected_status['progress']],
      [ng['status']['startTime'], ng['status']['progress']]
      )
    assert_equal expected_node['status']['statusText'], node['status']['statusText']
  end

  def test_update_node
    put '/v1/job/'+@graph['id']+'/node/'+@node_update['id'], @node_update.to_json
    assert last_response.ok?
    assert_equal @expected_put_response, JSON.parse(last_response.body)

    ElasticSearchAdaptor.instance.refresh!
    get '/v1/job/'+@graph['id']
    assert last_response.ok?

    updated = JSON.parse(last_response.body)
    node = updated['nodes'].find{|x| x['id'] == @node_update['id']}
    assert_equal @node_update['status']['statusText'], node['status']['statusText']
  end

  def test_update_edge
    put '/v1/job/'+@graph['id']+'/edge/?u='+@edge_update['u']+'&v='+@edge_update['v'], @edge_update.to_json
    assert last_response.ok?
    assert_equal @expected_put_response, JSON.parse(last_response.body)

    ElasticSearchAdaptor.instance.refresh!
    get '/v1/job/'+@graph['id']
    assert last_response.ok?

    updated = JSON.parse(last_response.body)
    edge = updated['edges'].find{|x| (x['u'] == @edge_update['u'] && x['v'] == @edge_update['v'])}
    assert_equal @edge_update['properties'], edge['properties']
  end

  def test_update_node_group
    put '/v1/job/'+@graph['id']+'/nodeGroup/'+@node_group_update['id'], @node_group_update.to_json
    assert last_response.ok?
    assert_equal @expected_put_response, JSON.parse(last_response.body)

    ElasticSearchAdaptor.instance.refresh!
    get '/v1/job/'+@graph['id']
    assert last_response.ok?

    updated = JSON.parse(last_response.body)
    node_group = updated['node_groups'].find{|x| x['id'] == @node_group_update['id']}
    assert_equal @node_group_update['status']['statusText'], node_group['status']['statusText']
  end
  
end
