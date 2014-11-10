module Lipstick

  #
  # @version 1.0
  # Represents a workflow graph. All lipstick 2.0 plans share this
  # representation. Use the {from_json} method to create a new graph
  # and {#to_json} to serialize as a json string.
  # @example
  #   {
  #     "id": "6dda0e50-6871-4c62-9719-3724b0cd0d3b",
  #     "name": "example",
  #     "status": {
  #       "progress":20,
  #       "startTime":1412354951,
  #       "heartbeatTime": 1412354796,
  #       "statusText":"running"
  #     },
  #     "nodes": [
  #       {"id": "a", "properties": {"alias": "one", "operation":"start", "step_type" : "mapper"}},
  #       {"id": "b", "properties": {"alias": "two", "operation":"hop", "step_type" : "mapper"}},    
  #       {"id": "c", "properties": {"alias": "three", "operation":"skip", "step_type" : "mapper"}},
  #       {"id": "d", "properties": {
  #         "alias": "four", "operation":"join", "step_type" : "mapper", "join": {
  #           "by":[
  #             {"alias":"one", "fields":["fieldA"]},
  #             {"alias":"two", "fields":["fieldA"]}
  #           ],
  #           "type":"hash",
  #           "strategy":"replicated"
  #         }
  #       }},
  #       {"id": "e", "properties": {"alias": "five", "operation":"read", "step_type" : "mapper"}},
  #       {"id": "f", "properties": {"alias": "six", "operation":"fly", "step_type" : "mapper"}},
  #       {"id": "g", "properties": {"alias": "seven", "operation":"dance", "step_type" : "mapper"}},
  #       {"id": "h", "properties": {"alias": "eight", "operation":"rollerskate", "step_type" : "mapper"}},
  #       {"id": "i", "properties": {"alias": "nine", "operation":"roll", "step_type" : "mapper"}},
  #       {"id": "j", "properties": {"alias": "ten", "operation":"dive", "step_type" : "mapper"}},
  #       {"id": "k", "properties": {"alias": "eleven", "operation":"tuck", "step_type" : "mapper"}},
  #       {"id": "l", "properties": {"alias": "twelve", "operation":"pushup", "step_type" : "mapper"}},
  #       {"id": "m", "properties": {"alias": "thirteen", "operation":"twist", "step_type" : "mapper"}},
  #       {"id": "n", "properties": {"alias": "fourteen", "operation":"kick", "step_type" : "mapper"}},
  #       {"id": "o", "properties": {"alias": "fifteen", "operation":"shout", "step_type" : "mapper"}},
  #       {"id": "p", "properties": {"alias": "sixteen", "operation":"finish", "step_type" : "mapper"}}, 
  #       {"id": "job1", "child": "1"},
  #       {"id": "job2", "child": "2"},
  #       {"id": "job3", "child": "3"},
  #       {"id": "job4", "child": "4"},
  #       {"id": "job5", "child": "5"}
  #     ],
  #     "edges": [
  #       {"u": "a", "v": "d"},
  #       {"u": "b", "v": "d"},
  #       {"u": "c", "v": "e"},
  #       {"u": "d", "v": "f"},
  #       {"u": "f", "v": "g"},
  #       {"u": "g", "v": "h"},
  #       {"u": "h", "v": "i"},
  #       {"u": "i", "v": "j"},
  #       {"u": "j", "v": "k"},
  #       {"u": "k", "v": "l"},
  #       {"u": "l", "v": "m"},
  #       {"u": "m", "v": "n"},
  #       {"u": "n", "v": "o"},
  #       {"u": "o", "v": "p"}
  #     ],
  #     "node_groups": [
  #       {
  #         "id": "1", "children": ["a","b"], "status":{
  #           "progress":10,
  #           "startTime":1412354951,
  #           "heartbeatTime": 1412354796,
  #           "statusText":"failed"
  #         }
  #       },
  #       {"id": "2", "url":"http://localhost:8088/proxy/application_xxx/", "children": ["c","d"]},
  #       {"id": "3", "children": ["e","f","g"]},
  #       {"id": "4", "children": ["h","i","j"],
  #         "properties": {
  #           "counters":{
  #             "mapreduce":{
  #               "num_records":2
  #             },
  #             "file_system":{
  #               "bytes_read":4,
  #               "bytes_written":17
  #             }
  #           }
  #         }
  #       },
  #       {"id": "5", "children": ["k","l","m","n","o","p"],
  #         "properties": {
  #           "counters":{
  #             "mapreduce":{
  #               "num_records":2
  #             },
  #             "file_system":{
  #               "bytes_read":4,
  #               "bytes_written":17
  #             }
  #           }
  #         },
  #         "status":{
  #           "progress":30,
  #           "startTime":1412354951,
  #           "heartbeatTime": 1412354796,
  #           "statusText":"running"
  #         }
  #       }
  #     ],
  #     "properties": {
  #       "userName":"jacob"    
  #     }
  #   }
  #
  class Graph

    # @note Required
    # Unique id for this graph. It is the responsibility of the user to
    # ensure the id is unique. UUID recommended.
    # @example
    #   6dda0e50-6871-4c62-9719-3724b0cd0d3b
    # @return [String]
    attr_accessor :id

    # @note Required
    # List of nodes belonging to this graph. Can be empty.
    # @example
    #   [
    #     {"id":"A"},{"id":"B"},{"id":"C"}
    #   ]
    # @return [Array[Node]]
    attr_accessor :nodes

    # @note Required
    # List of edges belonging to this graph. Can be empty. Any
    # contained edges <b>must</b> reference existing {#nodes} by
    # id
    # @example
    #   [
    #     {"u":"A", "v":"B"},{"u":"A", "v":"C"},{"u":"C", "v":"B"}
    #   ]
    # @return [Array[Edge]]
    attr_accessor :edges

    # @note Optional
    # Defaults to "workflow-{Graph#id}" Human readable name of this graph. This shows up in the graph
    # listing for {#get_v1_jobs GET /v1/job}
    # @example
    #   mygraph
    # @return [String] 
    attr_accessor :name

    # @note Optional
    # Arbitrary set of key-value properties for this graph.
    # @example
    #   {
    #     "mygraph_attribute": "somevalue"
    #   }
    # @return [Hash] 
    attr_accessor :properties

    # @note Optional
    # List of node groups (logical groupings of nodes) belonging to
    # this graph. Every node <b>must</b> reference children that are
    # existing {#nodes} by id
    # @example
    #   [
    #     {"id":"1", "children":["A","B"]},{"id":"2", "children":["C"]}
    #   ]
    # @return [Array[NodeGroup]] 
    attr_accessor :node_groups

    # @note Optional
    # Current status of this graph.
    # @example
    #   {
    #     "progress":10,
    #     "startTime":1415136536000,
    #     "heartbeatTime":1415136536000,
    #     "statusText":"running"
    #   }
    # @return [Status] 
    attr_accessor :status

    # @note Optional
    # Graph creation timestamp in milliseconds. If not set defaults to Time.now
    # @return [Long] 
    attr_accessor :created_at

    # @note Optional
    # Graph update timestamp in milliseconds. If not set defaults to Time.now
    # @return [Long] 
    attr_accessor :updated_at

    def initialize id, nodes, edges, name, properties, node_groups, status, created_at, updated_at
      @id = id
      @nodes = nodes
      @edges = edges
      @name  = name
      @properties  = properties
      @node_groups = node_groups
      @status = status
      @created_at = created_at
      @updated_at = updated_at
    end

    #
    # Get flat hashmap/dictionary representation of this graph with
    # symbolic keys.
    # @return [Hash] The hash (dictionary) representation of this graph
    #
    def to_hash
      {
        :id          => id,
        :status      => (status ? status.to_hash : {}),
        :nodes       => nodes.map{|node| node.to_hash},
        :edges       => edges.map{|edge| edge.to_hash},
        :name        => name,
        :properties  => (properties || {}),
        :node_groups => node_groups.map{|ng| ng.to_hash},
        :created_at  => created_at,
        :updated_at  => updated_at
      }
    end

    #
    # Get JSON serialization of this graph.
    # @return [String]
    #
    def to_json
      to_hash.to_json        
    end

    #
    # Instantiate a new {Graph} from a JSON string.
    # @raise [ArgumentError] if JSON string is missing
    #   required data (nodes, edges, or an id)
    # @return [Graph]
    def self.from_json json
      data = JSON.parse(json)
      raise ArgumentError, "All graphs must have nodes" unless data['nodes'] && data['nodes'].is_a?(Array)
      raise ArgumentError, "All graphs must have edges" unless data['edges'] && data['edges'].is_a?(Array)
      raise ArgumentError, "All graphs must have an id" unless data['id']
      
      id    = data['id']
      nodes = data['nodes'].map{|n| Node.from_hash(n)}
      edges = data['edges'].map{|e| Edge.from_hash(e)}
      name  = data['name'] || "workflow-"+data['id']
      
      node_groups = []
      if (data['node_groups'] && data['node_groups'].is_a?(Array))
        node_groups = data['node_groups'].map{|g| NodeGroup.from_hash(g)}
      end
      
      status = {}
      if (data['status'] && data['status'].is_a?(Hash))
        status = Status.from_hash(data['status'])
      end

      created_at = data['created_at'] || Time.now.to_i*1000
      updated_at = data['updated_at'] || created_at
      
      properties = (data['properties'] || {})
      Graph.new(id, nodes, edges, name, properties, node_groups, status, created_at, updated_at)
    end

    def get_node id
      nodes.find{|x| x.id == id}
    end
    
    def get_node_group id
      node_groups.find{|x| x.id == id}
    end

    def get_edge u, v
      edges.find{|e| (e.u == u && e.v == v)}
    end

    #
    # Insert or update an edge
    #
    def update_edge! u, v, data
      edge = get_edge(u, v)
      if edge # update existing node group
        edge.update_with!(data)
      else # create new node
        edges << Edge.from_hash(data)
      end
    end

    #
    # Insert or update a node
    # 
    def update_node! id, data
      node = get_node(id)
      if node # update existing node
        node.update_with!(data)
      else # create new node
        nodes << Node.from_hash(data)
      end
    end

    #
    # Insert or update a node group
    #
    def update_node_group! id, data
      node_group = get_node_group(id)
      if node_group # update existing node group
        node_group.update_with!(data)
      else # create new node group
        node_groups << NodeGroup.from_hash(data)
      end
    end
    
    #
    # @version 1.0
    # Represents the status of a workflow {Graph}, {Node}, or {NodeGroup}.
    # @example
    #   {
    #     "progress":10,
    #     "startTime":1415136536000,
    #     "heartbeatTime":1415136536000,
    #     "statusText":"running"
    #   }
    #
    class Status

      # @note Optional
      # Current progress. Defaults to 0
      # @return [Integer] 
      attr_accessor :progress

      # @note Optional
      # Represents start time in milliseconds. Defaults to Time.now
      # @return [Long] 
      attr_accessor :startTime

      # @note Optional
      # Represents heartbeat time in milliseconds. Defaults to Time.now
      # @return [Long] 
      attr_accessor :heartbeatTime

      # @note Optional
      # Represents end time in milliseconds.
      # @return [Long] 
      attr_accessor :endTime

      # @note Optional
      # Human readable status text. One of ["running", "terminated", "failed", "finished"]
      # @return [String] 
      attr_accessor :statusText

      STATES = [
        "running", "failed", "finished", "terminated"
      ]
      
      def initialize progress, startTime, heartbeatTime, endTime, statusText
        @progress      = progress
        @startTime     = startTime
        @heartbeatTime = heartbeatTime
        @endTime       = endTime
        @statusText    = statusText
      end

      
      def to_hash
        {
          :progress      => progress,
          :startTime     => startTime,
          :heartbeatTime => heartbeatTime,
          :endTime       => endTime,
          :statusText    => statusText
        }
      end
      
      def self.from_hash hsh
        progress      = hsh['progress']      || 0
        startTime     = hsh['startTime']     || Time.now.to_i*1000
        heartbeatTime = hsh['heartbeatTime'] || Time.now.to_i*1000
        endTime       = hsh['endTime']
        statusText    = hsh['statusText']
        if statusText
          if !STATES.include?(statusText.downcase)
            raise ArgumentError, "statusText must be one of [#{STATES.join(',')}]" 
          end
        end        
        Status.new(progress, startTime, heartbeatTime, endTime, statusText)
      end

      def update_with! data
        @progress   = data['progress'] if data['progress']
        @startTime  = data['startTime'] if data['startTime']
        @endTime    = data['endTime'] if data['endTime']
        @statusText = data['statusText'] if data['statusText'] 
      end
      
    end

    #
    # @version 1.0
    # Represents a single node in the {Graph}
    # @example
    #   {
    #     "id": "A",
    #     "properties": {
    #       "alias": "one",
    #       "operation":"opA",
    #       "step_type" : "mapper"
    #     }
    #   }
    class Node

      # @note Required
      # The unique identifier of the node. Here "unique" means within
      # id space defined by all {Graph#nodes} in the {Graph}.
      # @example
      #   "A"
      # @return [String] 
      attr_accessor :id

      # @note Optional
      # Arbitrary key-value attributes for the node. Templates
      # (referenced by {#type}) will be rendered using properties
      # pulled from here.
      # @example
      #   {
      #     "alias": "one",
      #     "operation":"opA",
      #     "step_type" : "mapper"
      #   }
      # @return [Hash] 
      attr_accessor :properties

      # @note Optional
      # Defaults to PigNode. References a named node template (one returned by
      # the {#get_templates GET /template} call).
      # @example
      #   PigNode
      # @return [String] 
      attr_accessor :type

      # @note Optional
      # Status of this node
      # @return [Status] 
      attr_accessor :status

      # @note Optional
      # Id of a {NodeGroup} that this node contains. Allows nodes to contain
      # collections of other nodes (node groups).
      # @return [String] 
      attr_accessor :child

      # @note Optional
      # Url to more information about this node. Allows a node to reference
      # an external resource.
      # @return [String]
      attr_accessor :url
      
      # @private
      attr_accessor :parent
      
      def initialize id, properties, child, type, status, url
        @id         = id
        @child      = child
        @type       = type
        @properties = properties
        @status     = status
        @url        = url
      end

      def to_hash
        res = {
          :id        => id,
          :properties => properties,
          :type       => type,
          :status     => (status ? status.to_hash : {})          
        }
        res[:child] = child if child
        res[:url]   = url   if url
        res
      end
      
      def self.from_hash hsh
        raise ArgumentError, "All nodes must have an id" unless hsh['id']
        url        = hsh['url']
        child      = hsh['child']
        properties = (hsh['properties'] || {})
        type       = (hsh['type'] || 'PigNode') # every time put or post is done for templates; reload it

        status = {}
        if (hsh['status'] && hsh['status'].is_a?(Hash))
          status = Status.from_hash(hsh['status'])
        end
        
        Node.new(hsh['id'], properties, child, type, status, url)
      end

      def update_with! data
        @id     = data['id']    if data['id']
        @child  = data['child'] if data['child']
        @type   = data['type']  if data['type']
        @url    = data['url']   if data['url']
        @status.update_with!(data['status']) if data['status']
        @properties.merge!(data['properties']) if data['properties']
      end
      
      def has_child?
        (@child != nil)
      end
    end

    #
    # @version 1.0
    # Represents a single <b>directed</b> edge in the {Graph}
    # @example
    #   {
    #     "u": "A",
    #     "v": "B",
    #     "properties": {
    #       "sampleOutput":["a\u00011\nb\u00012"],
    #       "schema":[
    #         {"type":"CHARARRAY", "alias":"name"},
    #         {"type":"INTEGER", "alias":"value"}
    #       ]
    #     }
    #   }
    class Edge

      # @note Required
      # Id of start {Node}. Node referenced must exist in {Graph#nodes}
      # @return [String] 
      attr_accessor :u

      # @note Required
      # Id of end {Node}. Node referenced must exist in {Graph#nodes}
      # @return [String] 
      attr_accessor :v

      # @note Optional
      # Defaults to PigEdge. References a named edge template (one returned by
      # the {#get_templates GET /template} call).
      # @example
      #   PigEdge
      # @return [String] 
      attr_accessor :type

      # @note Optional
      # Edge label. Since this will be rendered on the graph alongside the edge,
      # it's recommended to use simple properties (eg. an edge weight or
      # record count) rather than something that will compete for visual attention.
      # @example
      #   "42"
      # @return [String] 
      attr_accessor :label

      # @note Optional
      # Arbitrary key-value attributes for the edge. Templates
      # (referenced by {#type}) will be rendered using properties
      # pulled from here.
      # @example
      #   {
      #     "sampleOutput":["a\u00011\nb\u00012"],
      #     "schema":[
      #       {"type":"CHARARRAY", "alias":"name"},
      #       {"type":"INTEGER", "alias":"value"}
      #     ]
      #   }
      # @return [Hash] 
      attr_accessor :properties
      
      def initialize u, v, properties, type, label
        @u = u
        @v = v
        @label = label
        @type = type
        @properties = properties
      end

      def to_hash
        r = {
          :u => u,
          :v => v,
          :type => type,
          :properties => properties
        }
        r[:label] = label if label
        r
      end
      
      def self.from_hash hsh
        properties = (hsh['properties'] || {})
        type = (hsh['type'] || 'PigEdge')
        label = hsh['label']
        Edge.new(hsh['u'], hsh['v'], properties, type, label)
      end

      def update_with! data
        @u     = data['u'] if data['u']
        @v     = data['v'] if data['v']
        @label = data['label'] if data['label']
        @type  = data['type'] if data['type']
        @properties.merge!(data['properties']) if data['properties']
      end
      
    end

    #
    # @version 1.0
    # Represents a grouping of nodes in the {Graph}
    # @example
    #   {"id": "1", "children": ["A","B"]}
    #
    class NodeGroup
      
      # @note Required
      # Unique id of node group. Here "unique" means within
      # id space defined by all {Graph#node_groups} in the {Graph}.
      # @example
      #   "1"
      # @return [String] 
      attr_accessor :id

      # @note Required
      # A list of {Node} ids this node group contains. Each referenced
      # {Node} <b>must</b> exist in the {Graph#nodes}.
      # @example
      #   ["A","B"]
      # @return [Array[String]] 
      attr_accessor :children

      # @note Optional.
      # Arbitrary key-value attributes for the node group. 
      # @example
      #   {
      #     "counters":{
      #     "mapreduce":{
      #       "num_records":2
      #     },
      #     "file_system":{
      #       "bytes_read":4,
      #       "bytes_written":17
      #     }
      #   }
      # @return [Hash]
      attr_accessor :properties

      # @note Optional
      # Status of this node group
      # @return [Status] 
      attr_accessor :status

      # @note Optional
      # Url to more information about this node group. Allows a node
      # group to reference an external resource.
      # @return [String]
      attr_accessor :url
      
      # @private
      attr_accessor :parent

      def initialize id, children, properties, status, url
        @id         = id
        @children   = children
        @properties = properties
        @status     = status
        @url        = url
      end

      def to_hash
        r = {
          :id         => id,
          :children   => children,
          :status     => (status ? status.to_hash : {}),
          :properties => properties
        }
        r[:url] = url if url
        r
      end
      
      def self.from_hash hsh
        properties = (hsh['properties'] || {})

        status = {}
        if (hsh['status'] && hsh['status'].is_a?(Hash))
          status = Status.from_hash(hsh['status'])
        end
        url = hsh['url']
        NodeGroup.new(hsh['id'], hsh['children'], properties, status, url)
      end

      def update_with! data
        @id       = data['id']  if data['id']
        @url      = data['url'] if data['url']
        @children = data['children'] if data['children']
        @status.update_with!(data['status']) if data['status']
        @properties.merge!(data['properties']) if data['properties']
      end
      
      def has_parent?
        (@parent != nil)
      end      
    end

    class NodeMissingException < Exception; end
    
  end
end
