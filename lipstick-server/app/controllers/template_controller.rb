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

  #
  # Save template to Elasticsearch index under 'node_template'
  #
  # @param params [Hash] Key value POST request params
  # @param json [String] Request body, saved as document to Elasticsearch
  # @return [Hash] Reponse, either containing the name of the
  #   template saved or an error if the save failed.
  #   
  #
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

# @!macro sinatra.get
#   @method $1
#   @overload GET '$1'
#   Lists currently defined templates
#   @return [String] A json structure with a listing of all
#       templates currently defined for nodes and edges.
#       The basic structure is:
#
#        {
#          "templates": <list_of_templates>,
#          "total": <total>
#        }
#
#       where each template in the list is a json object like:
#
#        {
#         "name" : The name of the template,
#         "template" : Mustache HTML template,
#         "view": JavaScript view class corresponding to the template
#        }
#
get '/template' do
  res = TemplateService.list
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end

# @!macro sinatra.post
#   @method $1
#   @overload POST '$1'
#   Create a new template for rendering nodes or edges.
#   @param name [String] Name of the template to be created
#   @return [String] A json string with either containing the name of the
#       template saved or an error if the save failed.
post '/template/:name' do
  request.body.rewind
  ret = TemplateService.save(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end
