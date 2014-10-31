#
# Copyright 2014 Netflix, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
class TemplateService
  @@es = ElasticSearchAdaptor.instance
    
  def self.list
    @@es.list_templates
  end
  
  def self.save params, json
    if @@es.save(params[:name], 'node_template', json)
      return {
        :name => params[:name]
      }
    else
      return {
        :error => "failed to save data"
      }
    end    
  end
end

get '/template' do
  res = TemplateService.list
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end

post '/template/:name' do
  request.body.rewind
  ret = TemplateService.save(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end
