define(['jquery', 'lib/dagre-d3.min', 'knockout', './status', './node_group', './node', './edge'],
       function($, dagreD3, ko, Status, NodeGroup, Node, Edge) {
           var WorkflowGraph = {
               Status: Status,
               NodeGroup: NodeGroup,
               Node: Node,
               Edge: Edge,
               
               Graph: function(data) {
                   var self = this;

                   self.depth = 0;
                   self.graphMap = {};
                   self.nodeGroupMap = {};
                   
                   self.status = ko.observable(new Status(data.status || {}));
                   self.properties = ko.observable(data.properties ? data.properties : {});
                   self.nodeGroups = ko.observableArray();
                   self.nodes = ko.observableArray();
                   self.edges = ko.observableArray();

                   self.init = function() {
                       $.each(data.node_groups ? data.node_groups : [], function(i, nodeGroup) {
                           var ng = new NodeGroup(nodeGroup);
                           self.nodeGroupMap[nodeGroup.id] = ng;
                           self.nodeGroups.push(ng);
                       });
                       
                       $.each(data.nodes ? data.nodes : [], function(i, node) {
                           var n = new Node(node);
                           self.graphMap[node.id] = n;
                           self.nodes.push(n);
                           if (node.child) {
                               var ng = self.nodeGroup(node.child);
                               ng.name(node.id); 
                           }
                       });
                       
                       $.each(data.edges ? data.edges : [], function(i, edge) {
                           self.edges.push(new Edge(edge));
                       });

                       self.layers();
                       self.subgraphs();
                       self.sortedLayers();
                   };                                      

                   self.updateWith = function(data) {
                       if (data.status) {self.status().updateWith(data.status);}
                       if (data.properties) {self.properties(data.properties);}
                       if (data.node_groups) {
                           $.each(data.node_groups, function(i, nodeGroup) {
                               var ng = self.nodeGroup(nodeGroup.id);
                               if (ng) {ng.status().updateWith(nodeGroup.status);}
                           });
                       }
                       if (data.nodes) {
                           $.each(data.node_groups, function(i, node) {
                               var n = self.node(node.id);
                               if (n) {n.status().updateWith(node.status);}
                           });
                       }
                   };

                   self.nodesWithStatus = function(statusText) {                       
                       var filtered = self.nodes().filter(function(n) {
                           return (n.status().statusText() === statusText);
                       });
                       return filtered;
                   };

                   self.nodeGroupsWithStatus = function(statusText) {
                       var filtered = self.nodeGroups().filter(function(ng) {
                           return (ng.status().statusText() === statusText);
                       });
                       return filtered;
                   };
                   
                   self.node = function(id) {
                       return self.graphMap[id];
                   };
                   
                   self.nodeGroup = function(ngId) {
                       return self.nodeGroupMap[ngId];
                   };

                   self.nodeGroupNode = function(id) {
                       var nodeGroupNode = self.getNodeWithChild(id);
                       return nodeGroupNode;  
                   };

                   self.nodeWithChild = function(childId) {
                       var n = self.nodes().filter(function(nd, idx, arr) {
                           return (nd.child() === id);
                       })[0];
                       return n;
                   };

                   self.edge = function(id) {
                       var ends = id.split("->");
                       var edge = self.edges().filter(function(e, idx, arr) {
                           return ((e.u() === ends[0]) && (e.v() === ends[1]));
                       })[0];
                       return edge;
                   };

                   //
                   // Return a list of nodeGroups at a specific depth
                   //
                   self.subgraphsAt = function(depth) {
                       var result = self.nodeGroups().filter(function(ng, idx, arr) {
                           return ng.depth === depth;
                       });
                       return result;
                   };

                   //
                   // Return a list of nodes at a specific depth
                   //
                   self.nodesAt = function(depth) {
                       var result = self.nodes().filter(function(node, idx, arr) {
                           return node.depth === depth;
                       });
                       return result;
                   };
                   
                   //
                   // Append node depths
                   //
                   self.layers = function() {
                       function dfs(v, depth) {
                           if (v.child()) {
                               var ng = self.nodeGroup(v.child());
                               var children = ng.children();
                               if (children && children.length) {
                                   $.each(children, function(i, child) {                                       
                                       dfs(self.node(child), depth+1);
                                   });
                               }
                               ng.depth = depth;
                           }
                           v.depth = depth;
                           // set graph depth to max depth
                           if (depth > self.depth) { self.depth = depth; } 
                       };
                       $.each(self.nodes(), function(i, node) {
                           dfs(node, 1);
                       });
                   };

                   self.subgraphs = function() {
                       function dfs(v, parents) {
                           if (v.child()) {
                               var ng = self.nodeGroup(v.child());
                               var children = ng.children();
                               if (children && children.length) {
                                   $.each(children, function(i, child) {                                       
                                       dfs(self.node(child), parents.concat(v.id()));
                                   });
                               }
                               ng.immediateParent(parents[parents.length-1]);
                               ng.parents = parents;
                           }
                           v.parents = parents
                       };
                       
                       var roots = self.nodesAt(1);
                       var rootNodeGroup = new NodeGroup({
                           id: "_rootNodeGroup",
                           name: "_root",
                           children: roots.map(function(n) { return n.id(); })
                       });
                       rootNodeGroup.parents = [];
                       rootNodeGroup.depth = 0;
                       self.nodeGroups.push(rootNodeGroup);
                       
                       $.each(roots, function(i, node) {
                           dfs(node, ['_root']);
                       });
                   };

                   //
                   // Create pre-order of layers
                   //
                   self.sortedLayers = function() {
                       var g = new dagreD3.graphlib.Graph();
                       var edges = [];
                       $.each(self.nodeGroups(), function(i, ng) {
                           g.setNode(ng.name(), ng);
                           if (ng.immediateParent()) {
                               edges.push({u: ng.immediateParent(), v: ng.name()});
                           }
                       });
                       $.each(edges, function(i, e) {
                           g.setEdge(e.u, e.v);
                       });
                       var sorted = dagreD3.graphlib.alg.preorder(g, "_root");
                       self.nodeGroups([]);
                       $.each(sorted, function(i, ngName) {
                           self.nodeGroups.push(g.node(ngName));
                       });                                                                                  
                   };
                   
                   //
                   // subgraph - name of nodeGroup
                   // level - how dep
                   //
                   self.toGraphLib = function(subgraph) {
                       subgraph = typeof subgraph !== 'undefined' ? subgraph : 'root';
                       
                       var g = new dagreD3.graphlib.Graph({compound: true})
                           .setGraph({})
                           .setDefaultEdgeLabel(function() { return {}; });

                       var nodeSubset = self.nodes().filter(function(n, idx, arr) {
                           return (n.parents.indexOf(subgraph) > -1);
                       });

                       var nodeGroupSubset = self.nodeGroups().filter(function(ng, idx, arr) {
                           return (ng.parents.indexOf(subgraph) > -1);
                       });
                       
                       $.each(nodeSubset, function (i, node) {            
                           if (node.child()) {
                               g.setNode(node.child(), {label:''});
                           } else {
                               g.setNode(node.id(), {labelType: "html", id: node.id(), label: node.render(), padding: 2});
                           }
                       });

                       $.each(nodeGroupSubset, function (i, ng) {
                           $.each(ng.children(), function (j, child) {
                               var childNode = self.graphMap[child];
                               if (childNode) {
                                   if (g.hasNode(childNode.id())) {
                                       g.setParent(child, ng.id());
                                   } else {  // childNode is not a leaf node
                                       var childNodeId = childNode.child();
                                       g.setParent(childNodeId, ng.id());
                                   }
                               }
                           });
                       });                
                
                       $.each(self.edges(), function (i, edge) {
                           var edgeId = "edgeLabel_"+edge.u()+"->"+edge.v();            
                           var edgeData = {labelType: "html", lineInterpolate: "basis"};
                           if (_.size(edge.properties()) > 0) {
                               edgeData.label = "<div id='"+edgeId+"' class='edge-label edge-info-label'>&#8505;</div>"                               
                           } else {
                               edgeData.label = "<div id='"+edgeId+"' class='edge-label'></div>"
                           }
                           if (edge.label()) {
                               console.log(edge.label());
                               edgeData.label = edgeData.label + "<h6>"+edge.label()+"</h6>"
                           }
                           if (g.hasNode(edge.u()) && g.hasNode(edge.v())) {
                               g.setEdge(edge.u(), edge.v(), edgeData);
                           }
                       });
                   
                       return g;
                   };

                   self.init();
               }
           }
           return WorkflowGraph;
       });
