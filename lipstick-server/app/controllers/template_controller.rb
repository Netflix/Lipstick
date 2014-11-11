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
  
  # 
  # Post default node and edge templates
  #
  def self.load_defaults
    templates = Dir["app/templates/*.mustache"].inject({}) do |hsh, f|
      hsh[File.basename(f).gsub(".mustache","")] = f
      hsh
    end
    
    views = Dir["app/templates/*.js"].each do |view|
      name     = File.basename(view).gsub(".js","")
      template = templates[name]
      
      view_data = {
        :name     => name,
        :view     => File.read(view),
        :template => File.read(template)
      }
      
      save({:name => name}, view_data.to_json)
    end
  end

  load_defaults
end

# @method get_templates
# @overload GET "/template"
# Lists currently defined templates
# @return [String] A json structure with a listing of all
#     templates currently defined for nodes and edges.
#     The basic structure is:
#
#      {
#        "templates": <list_of_templates>,
#        "total": <total>
#      }
#
#     where each template in the list is a json object like:
#
#      {
#       "name" : The name of the template,
#       "template" : Mustache HTML template,
#       "view": JavaScript view class corresponding to the template
#      }
# @example
#   $: curl -XGET "localhost:9292/template"
#   {"templates":[...],"total":2}
# 
get '/template' do
  res = TemplateService.list
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end

# @method create_template
# @overload POST "/template/:name"
# Create a new named template for rendering nodes or edges.
# @param name [String] Name of the template to be created. <b>Important:</b> this
#   name is used as the <em>type</em> field on nodes and edges.
# @param body [String] Body of request must be a JSON encoded string with "name",
#   "template", and "view" fields. The <b>name</b> is the name of the new template,
#   the <b>template</b> is an html template using Mustache js syntax, and the <b>view</b>
#   is a Javascript class that is initialized using the properties attribute of the
#   {Lipstick::Graph::Node} or {Lipstick::Graph::Edge} that refers to it. See
#   {http://gist.github.com/thedatachef/b7d24cf57a3ae414b4ed this} gist for a simple example.
# @return [String] A json string either containing the name of the template saved or
#   an error if the save failed.
# @example
#   $: curl -H 'Content-Type: application/json' -XPOST "localhost:9292/template/PigNode" -d@pig-node-template.json
#   $: cat pig-node-template.json
#   {
#     "name":"PigNode",
#     "view":"function PigNode(properties) {\n    var self = this;\n    self.properties = properties;\n    self.operation = properties.operation;\n    \n    self.additional_info = function() {\n        if (self.properties.macro) {\n            return \" (MACRO: \"+self.properties.macro+\")\";\n        }\n        if (self.properties.limit) {\n            return \" (\"+self.properties.limit+\")\";\n        }\n        if (self.properties.join &&\n            self.properties.join.strategy &&\n            self.properties.join.type) {\n            var strategy = self.properties.join.strategy;\n            var type = self.properties.join.type;\n            return \" (\"+type+\", \"+strategy+\")\";\n        }\n    };\n\n    self.step_type_color = function() {\n        if (self.properties.step_type) {\n            switch (self.properties.step_type.toLowerCase()) {\n            case 'mapper':\n                return '#3299BB';\n            case 'reducer':\n                return '#FF9900';\n            case 'tez':\n                return '#F5D04C';\n            default:\n                return '#BF0A0D';\n            }\n        } else {\n            return '#BF0A0D';\n        }\n    };\n    \n    self.join = function() {\n        var j = self.properties.join;\n        if (j && j.by) {\n            var relations = j.by.map(function(rel){ return rel.alias; });\n            var fields = [];\n            var to = j.by[0].fields.length;\n            for (var i = 0; i < to; i++) {\n                fields[i] = [];\n                for (var k = 0; k < j.by.length; k++) {\n                    var rel = j.by[k];\n                    fields[i].push(rel.fields[i]);\n                }\n            }\n            \n            var result = {\n                relations: relations,\n                by: fields.map(function(field_list) { return {fields: field_list}; })\n            };\n            return result;\n        }\n        return;\n    };\n    \n    self.colspan = function() {\n        var j = self.properties.join;\n        if (j && j.by) {\n            return j.by.length;\n        } else {\n            return 1;\n        }\n    };\n    \n    self.alias = properties.alias;\n    self.expression = properties.expression;\n    self.storage_location = properties.storage_location;\n    self.storage_function = properties.storage_function;\n    \n    self.schema = function() {\n        if (self.properties.schema) {            \n            switch (self.operation) {\n                case 'GROUP':\n                case 'COGROUP':\n                case 'JOIN':\n                case 'LOAD':\n                case 'STORE':\n                        return;\n                default:\n                        return {columns: self.properties.schema};\n            }\n        } else {\n            return;\n        }\n    };\n}\n",
#     "template":"<table border=\"1\" cellborder=\"1\" cellspacing=\"0\">\n  <tbody style=\"font-size: 1.5em; font-family: Times, serif; text-align: center; border: 1px solid black;\">\n    <tr style=\"border: 1px solid black;\">\n      <td style=\"border-collapse: collapse; background-color: {{step_type_color}};\" colspan=\"{{join.relations.length}}{{^join.relations}}2{{/join.relations}}\">\n        {{operation}}{{additional_info}}\n      </td>\n    </tr>\n    \n    {{#expression}}\n    <tr style=\"border: 1px solid black;\"><td style=\"border-collapse: collapse;\" colspan=\"{{join.relations.length}}{{^join.relations}}2{{/join.relations}}\">{{expression}}</td></tr>\n    {{/expression}}\n    \n    {{#storage_location}}\n    <tr style=\"border: 1px solid black;\"><td style=\"border-collapse: collapse;\" colspan=\"{{join.relations.length}}{{^join.relations}}2{{/join.relations}}\">{{storage_location}}</td></tr>\n    {{/storage_location}}\n\n    {{#storage_function}}\n    <tr style=\"border: 1px solid black;\"><td style=\"border-collapse: collapse;\" colspan=\"{{join.relations.length}}{{^join.relations}}2{{/join.relations}}\">{{storage_function}}</td></tr>\n    {{/storage_function}}\n    \n    <tr style=\"border: 1px solid black;\">\n      <td style=\"border-collapse: collapse;\" bgcolor=\"#424242\" colspan=\"{{join.relations.length}}{{^join.relations}}2{{/join.relations}}\">\n        <font color=\"#FFFFFF\">{{alias}}</font>\t\t\t\t\n      </td>\n    </tr>\n\n    {{#join}}\n    <tr style=\"border: 1px solid black;\">\n      {{#relations}}\n      <td style=\"border-collapse: collapse;\" bgcolor=\"#BCBCBC\">{{.}}</td>\n      {{/relations}}\n    </tr>\n    \n    {{#by}}\n    <tr style=\"border: 1px solid black;\">\n      {{#fields}}\n      <td style=\"border-collapse: collapse;\" bgcolor=\"#FFFFFF\">{{.}}</td>\n      {{/fields}}\n    </tr>          \n    {{/by}}        \n    {{/join}}\n\n    {{#schema}}\n    {{#columns}}\n    <tr style=\"border: 1px solid black;\" bgcolor=\"#FFFFFF\">\n      <td style=\"border-collapse: collapse;\" bgcolor=\"#FFFFFF\">{{alias}}</td><td bgcolor=\"#FFFFFF\">{{type}}</td>\n    </tr>\n    {{/columns}}\n    {{/schema}}\n  </tbody>\n</table>\n"
#   }
# @example
#   $: curl -H 'Content-Type: application/json' -XPOST "localhost:9292/template/PigEdge" -d@pig-edge-template.json
#   $: cat pig-edge-template.json
#   {
#     "name":"PigEdge",
#     "view":"function PigEdge(properties) {\n    var self = this;\n    self.properties = properties;    \n    self.hasSampleData = function() {\n        if (self.properties.sampleOutput) {\n            return true;\n        } \n        return false;\n    };\n\n    self.hasSchema = function() {\n        if (self.properties.schema) {\n            return true;\n        }\n        return false;\n    };\n\n    self.sampleOutput = function() {\n        var sol = self.properties.sampleOutput;\n        var result = sol.map(function(so, i) {\n            return {\n                data: so.split(\"\\n\").map(function(record, j) {\n                    return {fields: record.split(\"\\u0001\")};\n                })\n            };\n        });\n        return result;\n    };\n\n    self.schema = properties.schema;\n}\n",
#     "template":"{{#hasSampleData}}\n<h4>Sample Data: </h4>\n{{#sampleOutput}}\n<table class=\"table table-condensed table-bordered table-hover\">\n  {{#hasSchema}}\n  <thead>\n    <tr style=\"background-color: #fcf8e3;\">\n      {{#schema}}\n      <td><strong>{{alias}}</strong></td>\n      {{/schema}}\n    </tr>\n  </thead>\n  {{/hasSchema}}\n  {{^hasSchema}}\n  <thead><tr style=\"background-color: #fcf8e3;\"><td colspan=\"200\">&nbsp;</td></tr></thead>\n  {{/hasSchema}}\n  <tbody>\n    {{#data}}\n    <tr>\n    {{#fields}}\n      <td>{{.}}</td>\n    {{/fields}}\n    </tr>\n    {{/data}}\n  </tbody>\n</table>\n{{/sampleOutput}}\n{{/hasSampleData}}\n{{^hasSampleData}}\n    <table class=\"table table-condensed table-bordered table-hover\"><tr><td>No sample output available at this time.</td></tr></table>\n{{/hasSampleData}}\n"
#   }
#
post '/template/:name' do
  request.body.rewind
  ret = TemplateService.save(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end
