class TemplateControllerTest < Test::Unit::TestCase
  include Rack::Test::Methods
  
  def app
    Sinatra::Application
  end

  def setup
    @sample = {
      'name' => 'Simple',
      'view' => 'function Simple(properties) {self.name = properties.name}',
      'template' => '<b>{{name}}</b>'
    }
    @expected_post_response = {'name' => @sample['name']}
    @expected_get_total = 2
    @expected_names = ['PigEdge', 'PigNode']
    ElasticSearchAdaptor.instance.refresh!
  end

  def test_should_list_templates
    get '/template'
    assert last_response.ok?
    response = JSON.parse(last_response.body)
    response_names = response['templates'].map{|t| t['name']}
    assert_equal @expected_get_total, response['total']
    assert_equal (@expected_names - response_names).size, 0
  end

  def test_should_post_templates
    post '/template/'+@sample['name'], @sample.to_json
    assert last_response.ok?
    assert_equal @expected_post_response, JSON.parse(last_response.body)
  end  
end
