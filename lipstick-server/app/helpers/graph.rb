module Lipstick
  class Graph
    
    attr_accessor :id, :nodes, :edges, :name, :properties
    attr_accessor :node_groups, :status, :created_at, :updated_at

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
    
    def to_json
      to_hash.to_json        
    end
    
    def self.from_json json
      data = JSON.parse(json)
      raise ArgumentError, "All graphs must have nodes" unless data['nodes'] && data['nodes'].is_a?(Array)
      raise ArgumentError, "All graphs must have edges" unless data['edges'] && data['edges'].is_a?(Array)
      raise ArgumentError, "All graphs must have an id" unless data['id']
      
      id    = data['id']
      nodes = data['nodes'].map{|n| Node.from_hash(n)}
      edges = data['edges'].map{|e| Edge.from_hash(e)}
      name  = data['name'] || "workflow-"+data['id']
      
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
    
    class Status
      attr_accessor :progress, :startTime, :heartbeatTime, :endTime, :statusText

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
        Status.new(progress, startTime, heartbeatTime, endTime, statusText)
      end            
    end
    
    class Node
      attr_accessor :id, :properties, :type, :status, :child, :parent, :gv_node
      
      def initialize id, properties, child, type, status
        @id         = id
        @child      = child
        @type       = type
        @properties = properties
        @status     = status
      end

      def to_hash
        res = {
          :id        => id,
          :properties => properties,
          :type       => type,
          :status     => (status ? status.to_hash : {})
        }
        res[:child] = child if child
        res
      end
      
      def self.from_hash hsh
        raise ArgumentError, "All nodes must have an id" unless hsh['id']
        child      = hsh['child']
        properties = (hsh['properties'] || {})
        type       = (hsh['type'] || 'PigNode') # every time put or post is done for templates; reload it

        status = {}
        if (hsh['status'] && hsh['status'].is_a?(Hash))
          status = Status.from_hash(hsh['status'])
        end
        
        Node.new(hsh['id'], properties, child, type, status)
      end

      def has_child?
        (@child != nil)
      end
    end

    class Edge
      attr_accessor :u, :v, :type, :properties
      def initialize u, v, properties, type
        @u = u
        @v = v
        @type = type
        @properties = properties
      end

      def to_hash
        {
          :u => u,
          :v => v,
          :type => type,
          :properties => properties
        }
      end
      
      def self.from_hash hsh
        properties = (hsh['properties'] || {})
        type = (hsh['type'] || 'PigEdge')
        Edge.new(hsh['u'], hsh['v'], properties, type)
      end
    end

    class NodeGroup
      attr_accessor :id, :children, :properties, :parent, :status      
      def initialize id, children, properties, status
        @id         = id
        @children   = children
        @properties = properties
        @status     = status
      end

      def to_hash
        {
          :id         => id,
          :children   => children,
          :status     => (status ? status.to_hash : {}),
          :properties => properties          
        }        
      end
      
      def self.from_hash hsh
        properties = (hsh['properties'] || {})

        status = {}
        if (hsh['status'] && hsh['status'].is_a?(Hash))
          status = Status.from_hash(hsh['status'])
        end
        
        NodeGroup.new(hsh['id'], hsh['children'], properties, status)
      end

      def has_parent?
        (@parent != nil)
      end      
    end

    class NodeMissingException < Exception; end
    
  end
end
