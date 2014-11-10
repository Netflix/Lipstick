module Lipstick
  module Adapter
    class P2jPlanPackage
      attr_accessor :optimized
      attr_accessor :unoptimized
      attr_accessor :script, :status, :userName, :jobName, :uuid
      attr_accessor :sampleOutputMap

      def initialize optimized, unoptimized, status, script, uuid, jobName, userName, sampleOutputMap
        @optimized = optimized
        @unoptimized = unoptimized
        @script = script
        @status = status
        @uuid = uuid
        @jobName = jobName
        @userName = userName
        @sampleOutputMap = sampleOutputMap
      end

      def self.from_json json
        data        = JSON.parse(json)
        optimized   = P2jPlan.from_hash(data['optimized'])
        unoptimized = P2jPlan.from_hash(data['unoptimized'])
        status      = P2jPlanStatus.from_hash(data['status'])
        script      = P2jScripts.from_hash(data['scripts'])
        uuid        = data['uuid']
        jobName     = data['jobName']
        userName    = data['userName']
        sampleOutputMap = {}
        if data['sampleOutputMap']
          sampleOutputMap = data['sampleOutputMap'].inject({}) do |hsh, sol|
            if sol.last['sampleOutputList']
              hsh[sol.first] = sol.last['sampleOutputList'].map{|so| P2jSampleOutput.from_hash(so) }
            end        
            hsh
          end      
        end    
        self.new(optimized, unoptimized, status, script, uuid, jobName, userName, sampleOutputMap)
      end

      def translate_node nodeMap, nodeGroupMap, prefix, op_id, op
        node_id = prefix+'-'+op_id.to_s
        ng_id   = prefix+'-'+op.mapReduce.jobId
        nodeMap[node_id] = op.to_node(prefix)
        ng = nodeGroupMap[ng_id]
        ng ||= {}
        ng['children'] ||= []
        ng['children'] << node_id
        nodeGroupMap[ng_id] = ng
      end

      def node_for_node_group id, child
        Lipstick::Graph::Node.from_hash({'id' => id, 'child' => child})
      end

      def translate_node_group id, name, ng
        ng['id'] = id
        original_scope = name.split('-',2).last      
        if status.jobStatusMap
          jobStatus = status.jobStatusMap.values.find{|x| x.scope == original_scope}
          if jobStatus
            mapProgress = jobStatus.mapProgress ? jobStatus.mapProgress.to_f : 0
            reduceProgress = jobStatus.reduceProgress ? jobStatus.reduceProgress.to_f : 0

            statusText = nil
            if jobStatus.isComplete
              if jobStatus.isSuccessful 
                statusText = "finished"
              else
                statusText = "failed"
              end
            elsif (jobStatus.finishTime > 0)
              statusText = "finished"
            elsif (mapProgress > 0)
              statusText = "running"
            end

            ng['status'] = {
              'startTime' => jobStatus.startTime,
              'endTime' => jobStatus.finishTime
            }
            ng['status']['statusText'] = statusText if statusText
            if jobStatus.isSuccessful
              ng['status']['progress'] = 100
            else
              ng['status']['progress'] = ((mapProgress + reduceProgress)/2 * 100).to_i
            end
            ng['url'] = jobStatus.trackingUrl if jobStatus.trackingUrl
            ng['properties'] = {
              'jobId'       => jobStatus.jobId,
              'counters'    => jobStatus.counters.inject({}){|hsh, cnt| hsh[cnt.first] = cnt.last.to_hash; hsh},
              'warnings'    => jobStatus.warnings.inject({}){|hsh, wn| hsh[wn.first] = wn.last.to_hash; hsh}
            }
          end      
        end
        Lipstick::Graph::NodeGroup.from_hash(ng)
      end

      def translate_edge u, v
        edge = {'u' => u.id, 'v' => v.id}
        startScope = u.properties['scope']
        endScope   = v.properties['scope']
        
        if startScope != endScope
          actualScope = startScope.split("-",2).last
          sampleOutput = sampleOutputMap[actualScope]
          if sampleOutput
            schema = u.properties['schema']
            properties = {
              'schema' => schema,
              'sampleOutput' => sampleOutput.map{|x| x.sampleOutput} 
            }
            edge['properties'] = properties
          end          
        end
        Lipstick::Graph::Edge.from_hash(edge)
      end
      
      def to_graph
        nodes = []
        node_groups = []
        edges = []
        properties = {}

        nodeMap = {}
        nodeGroupMap = {}
        
        optimized.plan.each do |op_id, op|
          translate_node(nodeMap, nodeGroupMap, 'optimized', op_id, op)
        end

        unoptimized.plan.each do |op_id, op|
          translate_node(nodeMap, nodeGroupMap, 'unoptimized', op_id, op)
        end

        nodes += nodeMap.values
        
        idx = 0
        optimized_children = []
        unoptimized_children = []
        nodeGroupMap.each do |ng_name, ng|
          nodes       << node_for_node_group(ng_name, idx.to_s)            
          node_groups << translate_node_group(idx.to_s, ng_name, ng)
          if ng_name.start_with?('optimized')
            optimized_children << ng_name
          else
            unoptimized_children << ng_name
          end      
          idx += 1
        end

        # node group for optimized
        nodes       << {'id' => 'optimized', 'child' => idx.to_s}
        node_groups << {'id' => idx.to_s, 'children' => optimized_children}
        idx += 1

        # node group for unoptimized
        nodes       << {'id' => 'unoptimized', 'child' => idx.to_s}
        node_groups << {'id' => idx.to_s, 'children' => unoptimized_children}
        idx += 1
        
        nodeMap.values.each do |node|
          succs = node.properties['successors']
          succs.each do |succ|
            edges << translate_edge(node, nodeMap[succ]) 
          end
        end
        
        properties['userName'] = userName
        properties['script'] = script.to_hash
        created_at = Time.now.to_i*1000
        Lipstick::Graph.new(uuid, nodes, edges,
          jobName, properties, node_groups,
          status.to_graph_status, created_at, created_at)
      end
      
      class P2jPlan
        attr_accessor :plan
        def initialize plan
          @plan = plan
        end    
        def self.from_hash data
          plan_map = data['plan'].inject({}) do |plan, op|            
            case op.last['operator']
            when 'LOCogroup', 'LOGroup' then
              plan[op.first] = P2jLOCogroup.from_hash(op.last)
            when 'LOJoin' then
              plan[op.first] = P2jLOJoin.from_hash(op.last)
            when 'LOStore' then
              plan[op.first] = P2jLOStore.from_hash(op.last)
            when 'LOLoad' then
              plan[op.first] = P2jLOLoad.from_hash(op.last)
            when 'LOFilter' then
              plan[op.first] = P2jLOFilter.from_hash(op.last)
            when 'LOSplitOutput' then
              plan[op.first] = P2jLOSplitOutput.from_hash(op.last)
            when 'LOLimit' then                                          
              plan[op.first] = P2jLOLimit.from_hash(op.last)
            else
              plan[op.first] = P2jLogicalRelationalOperator.from_hash(op.last)
            end            
            plan
          end
          plan_map.each_value do |p2j_op|
            p2j_op.schema_equals_pred = self.schema_equals_pred(p2j_op, plan_map)
          end          
          self.new(plan_map)
        end

        def self.schema_equals_pred p2j_op, opMap
          if p2j_op.schema.length > 0
            p2j_op.predecessors.each do |pred|
              p2j_pred = opMap[pred]
              if p2j_pred.schema.length > 0
                s1 = p2j_pred.schema.map{|x| x.to_hash}
                s2 = p2j_op.schema.map{|x| x.to_hash}                
                return false if !(s1 == s2)
              end              
            end
            return true
          end
          return false
        end
        
      end

      class P2jPlanStatus
        attr_accessor :startTime, :endTime, :heartbeatTime
        attr_accessor :jobStatusMap, :progress, :statusText

        def initialize startTime, endTime, heartbeatTime, jobStatusMap, progress, statusText
          @startTime = startTime
          @endTime = endTime
          @heartbeatTime = heartbeatTime
          @jobStatusMap = jobStatusMap
          @progress = progress
          @statusText = statusText
        end

        def self.from_hash data
          startTime     = data['startTime']
          endTime       = data['endTime']
          heartbeatTime = data['heartbeatTime']
          jobStatusMap  = data['jobStatusMap'].inject({}){|hsh, js| hsh[js.first] = P2jJobStatus.from_hash(js.last); hsh}
          progress      = data['progress']
          statusText    = data['statusText']
          self.new(startTime, endTime, heartbeatTime, jobStatusMap, progress, statusText)
        end
        
        def to_graph_status
          r = {}
          r['startTime'] = startTime if startTime
          r['endTime'] = endTime if endTime
          r['heartbeatTime'] = heartbeatTime if heartbeatTime
          r['progress'] = progress if progress
          r['statusText'] = statusText if statusText
          Lipstick::Graph::Status.from_hash(r)
        end
        
      end

      class P2jJobStatus
        attr_accessor :counters, :warnings, :scope, :jobId, :jobName, :trackingUrl
        attr_accessor :isComplete, :isSuccessful, :mapProgress, :reduceProgress
        attr_accessor :totalMappers, :totalReducers, :startTime, :finishTime
        attr_accessor :recordsWritten, :bytesWritten
        def initialize(counters, warnings, scope, jobId, jobName, trackingUrl,
            isComplete, isSuccessful, mapProgress, reduceProgress, totalMappers,
            totalReducers, startTime, finishTime, recordsWritten, bytesWritten)
          @counters       = counters
          @warnings       = warnings
          @scope          = scope
          @jobId          = jobId
          @jobName        = jobName
          @trackingUrl    = trackingUrl
          @isComplete     = isComplete
          @isSuccessful   = isSuccessful
          @mapProgress    = mapProgress
          @reduceProgress = reduceProgress
          @totalMappers   = totalMappers
          @totalReducers  = totalReducers
          @startTime      = startTime
          @finishTime     = finishTime
          @recordsWritten = recordsWritten
          @bytesWritten   = bytesWritten
        end
        def self.from_hash data
          counters = {}
          if data['counters']
            counters = data['counters'].inject({}) do |hsh, cnter|
              hsh[cnter.first] = P2jCounters.from_hash(cnter.last)
              hsh
            end            
          end

          warnings = {}
          if data['warnings']
            warnings = data['warnings'].inject({}) do |hsh, wn|
              hsh[wn.first] = P2jWarning.from_hash(wn.last)
              hsh
            end
          end
          
          scope          = data['scope']
          jobId          = data['jobId']
          jobName        = data['jobName']
          trackingUrl    = data['trackingUrl']
          isComplete     = data['isComplete']
          isSuccessful   = data['isSuccessful']
          mapProgress    = data['mapProgress']
          reduceProgress = data['reduceProgress']
          totalMappers   = data['totalMappers']
          totalReducers  = data['totalReducers']
          startTime      = data['startTime']
          finishTime     = data['finishTime']
          recordsWritten = data['recordsWritten']
          bytesWritten   = data['bytesWritten']
          
          self.new(counters, warnings, scope, jobId, jobName, trackingUrl,
            isComplete, isSuccessful, mapProgress, reduceProgress, totalMappers,
            totalReducers, startTime, finishTime, recordsWritten, bytesWritten)
        end  
      end

      class P2jCounters
        attr_accessor :counters
        def initialize counters
          @counters = counters
        end
        def self.from_hash data
          self.new(data['counters'])
        end
        def to_hash
          counters
        end    
      end

      class P2jWarning
        attr_accessor :warningAttributes, :jobId, :warningKey
        def initialize warningAttributes, jobId, warningKey
          @warningAttributes = warningAttributes
          @jobId             = jobId
          @warningKey        = warningKey
        end
        def self.from_hash data
          self.new(data['warningAttributes'], data['jobId'], data['warningKey'])
        end
        def to_hash
          {
            :warningAttributes => warningAttributes, :jobId => jobId, :warningKey => warningKey
          }
        end    
      end

      class P2jScripts
        attr_accessor :script
        def initialize script
          @script = script
        end
        def self.from_hash data
          self.new(data['script'])
        end
        def to_hash
          script
        end    
      end

      class P2jSampleOutput
        attr_accessor :schemaString, :sampleOutput
        def initialize schemaString, sampleOutput
          @schemaString = schemaString
          @sampleOutput = sampleOutput
        end
        def self.from_hash data
          self.new(data['schemaString'], data['sampleOutput'])
        end    
      end
      
      class P2jLogicalRelationalOperator
        attr_accessor :alias, :location, :mapReduce, :operator, :uid
        attr_accessor :predecessors, :successors, :schema, :schemaString

        attr_accessor :expression, :storageFunction, :storageLocation, :group, :join
        attr_accessor :schema_equals_pred, :rowLimit
        
        def initialize aliaz, location, mapReduce, operator, uid, preds, succs, schema, schemaStr
          @alias = aliaz
          @location = location
          @mapReduce = mapReduce
          @operator = operator
          @uid = uid
          @predecessors = preds
          @successors = succs
          @schema = schema
          @schemaString = schemaStr
        end
        
        def self.from_hash data          
          aliaz = data['alias']
          location = Location.from_hash(data['location'])
          mapReduce = MRStage.from_hash(data['mapReduce'])
          operator = data['operator']
          uid = data['uid']
          preds = data['predecessors']
          succs = data['successors']
          schema = []
          if data['schema']
            schema = data['schema'].map{|s| SchemaElement.from_hash(s)}
          end      
          schemaStr = data['schemaString']
          self.new(aliaz, location, mapReduce, operator, uid, preds, succs, schema, schemaStr)
        end
        
        def to_node prefix
          r = {
            'type' => "PigNode",
            'id'   => "#{prefix}-#{uid}"
          }
          properties = {
            'alias'        => self.send(:alias),
            'schema'       => schema.map{|se| se.to_hash},
            'location'     => location.to_hash,
            'operation'    => operator.gsub("LO", "").upcase,
            'scope'        => "#{prefix}-#{mapReduce.jobId}",
            'successors'   => successors.map{|succ| "#{prefix}-#{succ}"},
            'step_type'    => mapReduce.stepType,
            'schema_equals_pred' => schema_equals_pred
          }
          properties['expression']       = expression if expression
          properties['storage_function'] = storageFunction if storageFunction
          properties['storage_location'] = storageLocation if storageLocation
          properties['limit']            = rowLimit if rowLimit
          properties['group'] = group.to_hash if group
          properties['join']  = join.to_hash if join
          r['properties']     = properties
          Lipstick::Graph::Node.from_hash(r)
        end
        
        class Join
          attr_accessor :expression, :strategy, :type
          def initialize strategy, type, expression
            @strategy   = strategy
            @type       = type
            @expression = expression
          end
          def self.from_hash data
            strategy   = data['strategy']
            type       = data['type']
            expression = data['expression'].inject({}){|hsh, exp| hsh[exp.first] = JoinExpression.from_hash(exp.last); hsh}
            self.new(strategy, type, expression)
          end
          def to_hash
            {
              'by'       => expression.inject([]){|arr, e| arr << {'alias' => e.first, 'fields' => e.last.fields}; arr},
              'strategy' => strategy,
              'type'     => type
            }
          end      
        end

        class Location
          attr_accessor :filename, :line, :macro
          def initialize line, filename, macro
            @line     = line
            @filename = filename
            @macro    = macro
          end
          def self.from_hash data
            self.new(data['line'], data['filename'], data['macro'])
          end
          def to_hash
            r = {
              'line' => line, 'filename' => filename
            }
            r['macro'] = macro if macro
            r
          end      
        end    

        class MRStage
          attr_accessor :jobId, :stepType
          def initialize jobId, stepType
            @jobId    = jobId
            @stepType = stepType
          end

          def self.from_hash data
            self.new(data['jobId'], data['stepType'])
          end      
        end    
      end

      class P2jLOCogroup < P2jLogicalRelationalOperator
        attr_accessor :group
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.group = Join.from_hash(data['group'])
          c
        end    
      end

      class P2jLOFilter < P2jLogicalRelationalOperator
        attr_accessor :expression
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.expression = data['expression']
          c
        end    
      end

      class P2jLOJoin < P2jLogicalRelationalOperator
        attr_accessor :join
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.join = Join.from_hash(data['join'])
          c
        end    
      end

      class P2jLOLimit < P2jLogicalRelationalOperator
        attr_accessor :rowLimit
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.rowLimit = data['rowLimit']
          c
        end    
      end

      class P2jLOLoad < P2jLogicalRelationalOperator
        attr_accessor :storageLocation, :storageFunction
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.storageLocation = data['storageLocation']
          c.storageFunction = data['storageFunction']
          c
        end    
      end

      class P2jLOSplitOutput < P2jLogicalRelationalOperator
        attr_accessor :expression
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.expression = data['expression']
          c
        end    
      end

      class P2jLOStore < P2jLogicalRelationalOperator
        attr_accessor :storageLocation, :storageFunction
        def self.from_hash data
          c = self.superclass.from_hash(data)
          c.storageLocation = data['storageLocation']
          c.storageFunction = data['storageFunction']
          c
        end    
      end

      class JoinExpression
        attr_accessor :fields
        def initialize fields
          @fields = fields
        end
        def self.from_hash data
          self.new(data['fields'])
        end    
      end

      class SchemaElement
        attr_accessor :alias, :type, :schemaElements
        def initialize aliaz, type, schemaElements
          @alias = aliaz
          @type = type
          @schemaElements = schemaElements
        end
        def self.from_hash data
          schemaElements = []
          if data['schemaElements']
            schemaElements = schemadata['schemaElements'].map{|se| SchemaElement.from_hash(se) }
          end
          self.new(data['alias'], data['type'], schemaElements)
        end
        def to_hash
          r = {
            'alias' => self.send(:alias),
            'type'  => type,       
          }
          if schemaElements.size > 0
            r['schema'] = schemaElements.map{|se| se.to_hash}
          end
          r
        end    
      end
      
    end
  end
end
