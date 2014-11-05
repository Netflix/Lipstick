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
    @expected_get_response  = {'total' => 1, 'templates' => [@sample]}
    TemplateService.save({:name => @sample['name']}, @sample)
    ElasticSearchAdaptor.instance.refresh!
  end

  def test_should_list_templates
    get '/template'
    assert last_response.ok?
    assert_equal @expected_get_response, JSON.parse(last_response.body)
  end

  def test_should_post_templates
    post '/template/'+@sample['name'], @sample.to_json
    assert last_response.ok?
    assert_equal @expected_post_response, JSON.parse(last_response.body)
  end  
end
