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

  #
  # Wrap deserialization
  #
  def self.p2j_from_json json, java_class
    ser = nil
    begin
      ser = @@om.read_value(json, java_class)
    rescue => e
      $stderr.puts("Error deserializing: [#{$!}]")
      $stderr.puts(e.backtrace)
    end
    return ser
  end
  
  def self.save params, json
    ser  = p2j_from_json(json, P2jPlanPackage.java_class)
    return unless ser
    
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
    
    sample_output_list = p2j_from_json(json, P2jSampleOutputList.java_class)
    return unless sample_output_list
    
    plan = p2j_from_json(plan, P2jPlanPackage.java_class)
    return unless plan
    
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

    plan = p2j_from_json(plan, P2jPlanPackage.java_class)
    return unless plan

    status = p2j_from_json(json, P2jPlanStatus.java_class)
    return unless status
    
    plan.status.update_with(status)
    if @@es.save(params[:id], 'plan', @@om.write_value_as_string(plan))
      return {:status => "updated uuid #{params[:id]}"}
    else      
      return
    end    
  end

  def self.update_graph_node params, json
    graph = @@es.get(params[:id], 'graph')
    return unless graph

    graph = Lipstick::Graph.from_json(graph)    
    data  = JSON.parse(json)

    graph_node = graph.get_node(params[:nodeId])
    if graph_node # update existing node
      graph_node.update_with!(data)
    else # create new node
      graph.nodes << Lipstick::Graph::Node.from_hash(data)
    end

    graph.updated_at = Time.now.to_i*1000
    if @@es.save(graph.id.to_s, 'graph', graph.to_json)
      return {:status => "updated uuid #{params[:id]}"}
    else      
      return
    end 
  end

  def self.update_graph_node_group params, json
    graph = @@es.get(params[:id], 'graph')
    return unless graph

    graph = Lipstick::Graph.from_json(graph)    
    data  = JSON.parse(json)

    node_group = graph.get_node_group(params[:nodeGroupId])
    if node_group # update existing node group
      node_group.update_with!(data)
    else # create new node
      graph.node_groups << Lipstick::Graph::NodeGroup.from_hash(data)
    end

    graph.updated_at = Time.now.to_i*1000
    if @@es.save(graph.id.to_s, 'graph', graph.to_json)
      return {:status => "updated uuid #{params[:id]}"}
    else      
      return
    end 
  end

  def self.update_graph_edge params, json
    return unless (params[:u] && params[:v])
    
    graph = @@es.get(params[:id], 'graph')
    return unless graph

    graph = Lipstick::Graph.from_json(graph)    
    data  = JSON.parse(json)

    graph_edge = graph.get_edge(params[:u], params[:v])
    if graph_edge # update existing node group
      graph_edge.update_with!(data)
    else # create new node
      graph.edges << Lipstick::Graph::Edge.from_hash(data)
    end

    graph.updated_at = Time.now.to_i*1000
    if @@es.save(graph.id.to_s, 'graph', graph.to_json)
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
    graph.status = data['status'] if data['status']

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
        temp = p2j_from_json(@@es.get(params[:id], 'plan'), P2jPlanPackage.java_class)
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

# @method get_jobs
# @overload GET "/job"
# Gets a listing of P2jPlanPackage objects as json.
# @return [String] A json string with keys "jobs" and "jobsTotal"
# @deprecated
get '/job' do
  res = PlanService.list(params)
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end

# @method get_v1_jobs
# @overload GET "/v1/job"
# Get a list of all workflow graphs. See {Lipstick::Graph}
# @param max [Integer] Optional. Default: 10. Integer specifying
#   the maximum number of plans to return.
# @param sort [String] Optional. Default: "startTime". String
#   field name of the field to sort the results by.
# @param offset [Integer] Optional. Default: 0. Offset for results.
#   How paging is possible.
# @param order [String] Optional. Default: "asc". "asc" to sort the
#   results in ascending order, "desc" for descending order.
# @param search [String] Optional. Search string to get plans by a
#   specific user or with a specific name.
# @return [String] A json string with a list of graphs. Has the following
#   structure:
#    {
#      "jobs": [list of graphs],
#      "jobsTotal": <number of jobs>
#    }
#   where each job in the list looks like the following:
#    {
#      "id": <graph id>,
#      "name": <graph name>,
#      "created_at": <graph creation time, unix timestamp, milliseconds>,
#      "updated_at": <graph updated time, unix timestamp, millisecods>
#    }
# @see Lipstick::Graph
# @example
#   $: curl -XGET "localhost:9292/v1/job"
#   {"jobs":[{"id":"037389e6-49e0-450b-b02e-082173b17b4c","name":"PigLatin:bf.pig","created_at":1414704734000,"updated_at":1414704734000},{"id":"05d94692-0a7c-4f0b-81e4-8b4f79fe612b","name":"PigLatin:bf.pig","created_at":1414704734000,"updated_at":1414704734000},{"id":"08cf2150-85fb-4cdd-863e-470d3bed0cf9","name":"PigLatin:profile_takerate_agg_f.pig","created_at":1414704734000,"updated_at":1414704734000},{"id":"0978808f-1334-453f-81ac-1353bb5019ac","name":"PigLatin:aro_diversity_pvr_training_data.pig","created_at":1414704734000,"updated_at":1414704734000}],"jobsTotal":4}
#
get '/v1/job' do
  res = PlanService.list_graphs(params)
  if !res
    return [500, ""]
  end
  [200, {'Content-Type' => 'application/json'}, res.to_json]
end

# @method get_job
# @overload GET "/job/:id"
# Get P2jPlanPackage as json.
# @param id [String] The uuid of the plan to fetch
# @return [String] A json string of the P2jPlanPackage
# @deprecated
get '/job/:id' do
  ret = PlanService.get(params)
  if !ret
    res = {:error => "plan (#{params[:id]}) not found"}
    return [404, res.to_json]
  end
  [200, {'Content-Type' => 'application/json'}, ret]
end

# @method get_v1_job
# @overload GET "/v1/job/:id"
# Get a specific workflow graph by id
# @param id [String] The id of the workflow graph to fetch
# @return [String] A json string representation of the graph requested.
# @see Lipstick::Graph
# @example
#   $: curl -XGET "localhost:9292/v1/job/8"
#   {"id":"8","status":{...},"nodes":[...],"edges":[...],"name":"workflow-8","properties":{...},"node_groups":[...],"created_at":1415140632000,"updated_at":1415140632000}
get '/v1/job/:id' do
  ret = PlanService.get_graph(params)
  if !ret
    res = {:error => "graph (#{params[:id]}) not found"}
    return [404, res.to_json]
  end
  [200, {'Content-Type' => 'application/json'}, ret]
end

# @method post_job
# @overload POST "/job"
# Create a new P2jPlanPackage from json string
# @param body [String] JSON serialization of a P2jPlanPackage object
# @return [String] A json string with keys "uuid" and "aliases" if
#   successful otherwise "error"
# @deprecated
post '/job/?' do
  request.body.rewind
  ret = PlanService.save(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end

# @method post_v1_job
# @overload POST "/v1/job"
# Create a new workflow graph. {Lipstick::Graph} for spec.
# @param body [String] Body of POST request must be a JSON object containing a
#   workflow graph.
# @return [String] A json string either containing the uuid of the
#     graph saved or an error if the save failed.
# @see Lipstick::Graph
# @example
#   $: curl -H 'Content-Type: application/json' -XPOST "localhost:9292/v1/job" -d@examples/graphs/example.json
#   {"id":"8"}
#
post '/v1/job/?' do
  request.body.rewind
  ret = PlanService.save_graph(params, request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end

# @method update_v1_job
# @overload PUT "/v1/job/:id"
# Update an existing workflow graph (eg. with new status). {Lipstick::Graph} for spec.
# @param id [String] The id of the workflow graph to update
# @param body [String] JSON object containing valid {Lipstick::Graph::Status} objects.
#   At most one per graph, node group, and node.
# @return [String] A json string either containing the uuid of the
#     graph updated or an error if the update failed.
# @see Lipstick::Graph::Status
# @example
#   $: curl -XPUT "localhost:9292/v1/job/8" -d @examples/graphs/example.json
#   {"status":"updated uuid 8"}
#
put '/v1/job/:id' do
  request.body.rewind
  ret = PlanService.update_graph(params, request.body.read)
  if !ret
    return [404, {:error => "graph #{params[:id]} not found"}.to_json]
  end
  ret.to_json
end

# @method update_v1_node
# @overload PUT "/v1/job/:id/node/:nodeId"
# Update an existing workflow graph's node. {Lipstick::Graph::Node} for spec. If
# the node doesn't currently exist in the graph it is added. New node properties are
# recursively merged into existing node properties.
# @param id [String] The id of the workflow graph to update
# @param nodeId [String] The id of the node to create or update.
# @param body [String] JSON object containing a valid representation of a
#   {Lipstick::Graph::Node} object. A partial representation (eg. just node
#   status) works.
# @return [String] A json string either containing the id of the
#     graph updated or an error if the update failed.
# @see Lipstick::Graph::Node
# @example
#   # Create graph and add a node
#   $: curl -XPOST "localhost:9292/v1/job" -d -d '{"name":"mygraph","id":"mygraph","nodes":[{"id":"A"}],"edges":[]}'
#   {"id":"mygraph"}
#   $: curl -XPUT "localhost:9292/v1/job/mygraph/node/B" -d '{"id":"B"}'
#   {"status":"updated uuid mygraph"}
# @example
#   # Update a node's status
#   $: curl -XPUT "localhost:9292/v1/job/mygraph/node/B" -d '{"status":{"statusText":"failed"}}'
#   {"status":"updated uuid mygraph"}
put '/v1/job/:id/node/:nodeId' do
  request.body.rewind
  ret = PlanService.update_graph_node(params, request.body.read)
  if !ret
    return [404, {:error => "not found"}.to_json]
  end
  ret.to_json
end

# @method update_v1_node_group
# @overload PUT "/v1/job/:id/nodeGroup/:nodeGroupId"
# Update an existing workflow graph's node group. {Lipstick::Graph::NodeGroup} for spec.
# If the node group doesn't currently exist in the graph it is added. New node group
# properties are recursively merged into existing node group properties.
# @param id [String] The id of the workflow graph to update
# @param nodeGroupId [String] The id of the node group to create or update.
# @param body [String] JSON object containing a valid representation of a
#   {Lipstick::Graph::NodeGroup} object. A partial representation (eg. just node
#   group status) works.
# @return [String] A json string either containing the id of the
#     graph updated or an error if the update failed.
# @see Lipstick::Graph::NodeGroup
# @example
#   # Create graph and add a node group
#   $: curl -XPOST "localhost:9292/v1/job" -d -d '{"name":"mygraph","id":"mygraph","nodes":[{"id":"A"}],"edges":[]}'
#   {"id":"mygraph"}
#   $: curl -XPUT "localhost:9292/v1/job/mygraph/nodeGroup/1" -d '{"id":"1","children":["A"]}' 
#   {"status":"updated uuid mygraph"}
# @example
#   # Update a node group's status
#   $: curl -XPUT "localhost:9292/v1/job/mygraph/nodeGroup/1" -d '{"status":{"progress":"100"}}'
#   {"status":"updated uuid mygraph"}
put '/v1/job/:id/nodeGroup/:nodeGroupId' do
  request.body.rewind
  ret = PlanService.update_graph_node_group(params, request.body.read)
  if !ret
    return [404, {:error => "not found"}.to_json]
  end
  ret.to_json
end

# @method update_v1_edge
# @overload PUT "/v1/job/:id/edge?u=:u&v=:v"
# Update an existing workflow graph's edge. {Lipstick::Graph::Edge} for spec.
# If the edge doesn't currently exist in the graph it is added. New edge
# properties are recursively merged into existing edge properties.
# @param id [String] The id of the workflow graph to update
# @param u [String] The source node id of the edge to update
# @param v [String] The target node id of the edge to update
# @param body [String] JSON object containing a valid representation of a
#   {Lipstick::Graph::Edge} object. A partial representation (eg. just edge
#   properties) works.
# @return [String] A json string either containing the id of the
#     graph updated or an error if the update failed.
# @see Lipstick::Graph::Edge
# @example
#   # Create graph and add an edge
#   $: curl -XPOST "localhost:9292/v1/job" -d -d '{"name":"mygraph","id":"mygraph","nodes":[{"id":"A"}, {"id":"B"}],"edges":[]}'
#   {"id":"mygraph"}
#   $: curl -XPUT "localhost:9292/v1/job/mygraph/edge?u=A&v=B" -d '{"u":"A","v":"B"}' 
#   {"status":"updated uuid mygraph"}
# @example
#   # Update an edge's properties (eg. sample data)
#   $: curl -XPUT "localhost:9292/v1/job/mygraph/edge?u=A&v=B" -d '{"properties":{"sampleOutput":["a\u00011\nb\u00012"],
#         "schema":[
#           {"type":"CHARARRAY", "alias":"name"},
#           {"type":"INTEGER", "alias":"value"}
#         ]
#       }'
#   {"status":"updated uuid mygraph"}
put '/v1/job/:id/edge/?' do
  request.body.rewind
  ret = PlanService.update_graph_edge(params, request.body.read)
  if !ret
    return [404, {:error => "not found"}.to_json]
  end
  ret.to_json
end

# @method update_job
# @overload PUT "/job/:id"
# Update a P2jPlanPackage with status information
# @param id [String] The uuid of the plan to update
# @param body [String] JSON serialization of a P2jPlanStatus
# @return [String] A json string with keys "status" if
#   successful otherwise "error"
# @deprecated
put '/job/:id' do
  request.body.rewind
  ret = PlanService.update(params, request.body.read)
  if !ret
    return [404, {:error => "plan #{params[:id]} not found"}]
  end
  ret.to_json
end

# @method update_sample_output
# @overload PUT "/job/:id/sampleOutput/:jobId"
# Update the sample output data for a single map-reduce job for a single plan
# @param id [String] The uuid of the plan to update
# @param jobId [String] The map-reduce job id of the job to update
# @param body [String] Json body with sample output data
# @return [String] A json string with keys "id" and "uuid" if successful
#   "error" otherwise
# @deprecated
put "/job/:id/sampleOutput/:jobId" do
  request.body.rewind
  ret = PlanService.add_sample_output(params["id"], params["jobId"], request.body.read)
  if ret[:error]
    return [500, ret.to_json]
  end
  ret.to_json
end

put '/job' do
  [400, {:error => "a uuid must be speficied"}]
end

delete '/job/:id' do
  [405, {:error => "service does not support delete currently"}]
end

at_exit do
  PlanService.close
end
