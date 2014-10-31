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
import 'java.util.Properties'
import 'org.codehaus.jackson.map.ObjectMapper'
import 'org.codehaus.jackson.map.annotate.JsonSerialize'
import 'com.netflix.lipstick.Pig2DotGenerator'
import 'com.netflix.lipstick.model.P2jPlanStatus'
import 'com.netflix.lipstick.model.P2jPlanPackage'
import 'com.netflix.lipstick.model.P2jSampleOutputList'

class PlanService
  @@es = ElasticSearchAdaptor.instance
  @@om = ObjectMapper.new
  # don't index empty&null values
  @@om.set_serialization_inclusion(JsonSerialize::Inclusion::NON_NULL)
  
  def self.save params, json
    ser  = @@om.read_value(json, P2jPlanPackage.java_class)
    uuid = ser.get_uuid
    
    svg  = Pig2DotGenerator.new(ser.get_optimized).generate_plan("svg")
    ser.get_optimized.set_svg(svg)

    svg = Pig2DotGenerator.new(ser.get_unoptimized).generate_plan("svg")
    ser.get_unoptimized.set_svg(svg)

    # post ser.to_json to elasticsearch
    if @@es.save(uuid, 'plan', @@om.write_value_as_string(ser))
      return {
        :uuid    => uuid,
        :aliases => ser.get_optimized.get_plan.map{|kv| kv.last.get_alias}
      }
    else
      return {
        :error => "failed to save data"
      }
    end
  end

  #
  # Save lipstick 2.0 graph
  #
  def self.save_graph params, json
    graph = Lipstick::Graph.from_json(json)
    if @@es.save(graph.id.to_s, 'graph', graph.to_json)
      return {
        :id => graph.id.to_s
      }
    else
      return {
        :error => "failed to save data"
      }
    end
  end

  def self.add_sample_output id, job_id, json
    plan = @@es.get(id, 'plan')
    return unless plan
    
    sample_output_list = @@om.read_value(json, P2jSampleOutputList.java_class)
    plan = @@om.read_value(plan, P2jPlanPackage.java_class)
    
    output_map = plan.getSampleOutputMap || {}
    output_map[job_id] = sample_output_list
    plan.setSampleOutputMap(output_map)

    if @@es.save(id, 'plan', @@om.write_value_as_string(plan))
      return {:uuid => id, :jobId => job_id}
    else
      return {:error => "failed to save sampleoutput"}
    end    
  end

  def self.list params
    @@es.list_plans(params)
  end

  def self.list_graphs params
    @@es.list_graphs(params)
  end

  #
  # Updates plan status
  #
  def self.update params, json
    plan = @@es.get(params[:id], 'plan')
    return unless plan
    
    plan = @@om.read_value(plan, P2jPlanPackage.java_class)
    plan.status.update_with(@@om.read_value(json, P2jPlanStatus.java_class))
    if @@es.save(params[:id], 'plan', @@om.write_value_as_string(plan))
      return {:status => "updated uuid #{params[:id]}"}
    else      
      return
    end    
  end

  #
  # Updates graph status
  #
  def self.update_graph params, json
    graph = @@es.get(params[:id], 'graph')
    return unless graph

    graph = Lipstick::Graph.from_json(graph)    
    data  = JSON.parse(json)
    graph.status = data['status']

    if (data['node_groups'] && data['node_groups'].is_a?(Array))
      data['node_groups'].each do |ng|
        if ng['status']
          group = graph.get_node_group(ng['id'])
          group.status = Lipstick::Graph::Status.from_hash(ng['status'])
        end        
      end      
    end

    if (data['nodes'] && data['nodes'].is_a?(Array))
      data['nodes'].each do |n|
        if n['status']
          node = graph.get_node(n['id'])
          node.status = Lipstick::Graph::Status.from_hash(n['status'])
        end        
      end      
    end
    graph.updated_at = Time.now.to_i*1000
    if @@es.save(graph.id.to_s, 'graph', graph.to_json)
      return {:status => "updated uuid #{params[:id]}"}
    else      
      return
    end    
  end
  
  def self.get params
    if (!params[:status] && !params[:scripts] && !params[:optimized] && !params[:unoptimized] && !params[:sampleOutput])
      params[:full] = true
    end

    plan = nil
    if !params[:full]
      fields = ['uuid', 'userName', 'jobName']
      
      fields << 'status'      if params[:status]
      fields << 'scripts'     if params[:scripts]
      fields << 'optimized'   if params[:optimized]
      fields << 'unoptimized' if params[:unoptimized]
      
      ret = @@es.get_fields(params[:id], fields)
      return unless ret

      if params[:sampleOutput]
        temp = @@om.read_value(@@es.get(params[:id], 'plan'), P2jPlanPackage.java_class)
        j = JSON.parse(ret)
        j[:sampleOutputMap] = temp.sample_output_map
        ret = @@om.write_value_as_string(j)
      end
      
      return ret
    else
      plan = @@es.get(params[:id], 'plan')
    end
    return plan
  end

  def self.get_graph params
    @@es.get(params[:id], 'graph')
  end

  def self.close
    @@es.close
  end
  
end

get '/' do
  send_file File.join(settings.public_folder, 'index.html')
end

#
# Get a listing of jobs
#
get '/job' do
  res = PlanService.list(params)
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end


get '/v1/job' do
  res = PlanService.list_graphs(params)
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end

#
# Get a specific plan
#
get '/job/:id' do
  ret = PlanService.get(params)
  if !ret
    res = {:error => "plan (#{params[:id]}) not found"}
    return [404, res.to_json]
  end
  [200, {'Content-Type' => 'application/json'}, ret]
end

#
# Get a specific workflow graph
#
get '/v1/job/:id' do
  ret = PlanService.get_graph(params)
  if !ret
    res = {:error => "graph (#{params[:id]}) not found"}
    return [404, res.to_json]
  end
  [200, {'Content-Type' => 'application/json'}, ret]
end


#
# Create a new plan
#
post '/job/?' do
  request.body.rewind
  ret = PlanService.save(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end

post '/v1/job/?' do
  request.body.rewind
  ret = PlanService.save_graph(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end

put '/v1/job/:id' do
  request.body.rewind
  ret = PlanService.update_graph(params, request.body.read)
  if !ret
    return [404, {:error => "graph #{params[:id]} not found"}.to_json]
  end
  ret.to_json
end

#
# Update a plan
#
put '/job/:id' do
  request.body.rewind
  ret = PlanService.update(params, request.body.read)
  if !ret
    return [404, {:error => "plan #{params[:id]} not found"}]
  end
  ret.to_json
end

put "/job/:id/sampleOutput/:jobId" do
  request.body.rewind
  ret = PlanService.add_sample_output(params["id"], params["jobId"], request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end

#
# Various incorrect ways of accessing api
#
put '/job' do
  [400, {:error => "a uuid must be speficied"}]
end

delete '/job/:id' do
  [405, {:error => "service does not support delete currently"}]
end

at_exit do
  PlanService.close
end
