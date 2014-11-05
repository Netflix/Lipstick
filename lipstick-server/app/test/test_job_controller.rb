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
          'updated_at' => @graph['updated_at'],
          'created_at' => @graph['created_at']
        }],
      'jobsTotal' => 1
    }
    @expected_post_response = {
      'id' => @graph['id']
    }
    @expected_put_response = {
      'status' => 'updated uuid '+@graph['id']
    }
    PlanService.save_graph({}, @graph.to_json)
    ElasticSearchAdaptor.instance.refresh!
  end

  def test_should_list_jobs
    get '/v1/job'
    assert last_response.ok?
    assert_equal @expected_list_response, JSON.parse(last_response.body)
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

    expected_ng   = @status_update['node_groups'].first
    expected_node = @status_update['nodes'].first
    
    ng   = updated['node_groups'].find{|x| x['id'] == expected_ng['id']}
    node = updated['nodes'].find{|x| x['id'] == expected_node['id']}

    assert_equal expected_ng['status'], ng['status']
    assert_equal expected_node['status']['statusText'], node['status']['statusText']
  end
  
end
