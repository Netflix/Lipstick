define(
    ['jquery', 'transit', 'lodash.min', 'knockout', 'd3', 'lib/dagre-d3.min',
     'bootstrap', 'treetable', 'workflow/graph/graph'],
    function($, transit, _, ko, d3, dagreD3, bootstrap, treetable, WorkflowGraph) {              

        function WorkflowViewModel() {
            var self = this;
            
            self.baseUrl = './v1/job/';
            self.graphSel = '#workflow-graph';
            self.uuid = undefined;
            self.active = d3.select(null);
            self.svg = d3.select(null);
            self.svgGraph = d3.select(null);
            self.svgNodes = d3.select(null);
            self.svgClusters = d3.select(null);
            self.svgClusterRects = d3.select(null);
            self.pulseInterval = undefined;
            self.initialZoom = {
                x: 0, y:0, scale: 1
            };
            
            // Main graph object
            self.graph = ko.observable(new WorkflowGraph.Graph({}));            

            // For hiding and showing elements
            self.graphNotRendered = ko.observable(true);
            self.groupListVisible = ko.observable(true);
            self.nodeGroupVisible = ko.observable(false);
            self.nodeVisible = ko.observable(false);

            // For displaying node, edge, and subgraph details
            self.current = {
                subGraph: ko.observable('_root'),
                nodeGroup: ko.observable(new WorkflowGraph.NodeGroup({})),
                node: ko.observable(new WorkflowGraph.Node({})),
                edge: ko.observable(new WorkflowGraph.Edge({}))
            };
            
            self.initialize = function(uuid) {
                self.uuid = uuid;                
                $.ajax({
                    type: 'GET',
                    url:  self.baseUrl + uuid
                }).done(function(json) {
                    self.graph(new WorkflowGraph.Graph(json));
                    if (self.graph().depth > 2) {
                        var sg = self.graph().subgraphsAt(1)[0];
                        self.renderSvg(sg.name());
                    } else {                    
                        self.renderSvg(self.current.subGraph());
                    }
                    self.initializeSvg();
                    self.getUpdatedGraph();
                }).fail(function() {
                });

                clearInterval(self.pulseInterval);
                self.pulseInterval = setInterval(self.pulse, 2000);
            };

            self.getUpdatedGraph = function() {
                $.ajax({
                    type: 'GET',
                    url:  self.baseUrl + self.uuid
                }).done(function(json) {
                    self.graph().updateWith(json);
                    if (self.graph().status().statusText().toLowerCase() === "running") {
                        _.delay(self.getUpdatedGraph, 5000);
                        self.pulse();
                    } else {
                        clearInterval(self.pulseInterval);
                        self.pulse();
                    }
                }).fail(function() {
                    _.delay(self.getUpdatedGraph, 5000);
                });
            };

            self.pulse = function() {
                // Pulse Running jobs
                self.svgClusterRects.filter(function(clusterId, idx) {
                    return (self.graph().nodeGroup(clusterId).status().statusText() === "running");
                }).transition().duration(1000).style('fill', 'rgb(233,233,233)')
                    .transition().duration(1000).style('fill', 'rgb(50,153,187)');
                
                self.svgClusterRects.filter(function(clusterId, idx) {
                    return (self.graph().nodeGroup(clusterId).status().statusText() === "finished");
                }).transition().duration(1000).style('fill', 'rgb(207,225,232)');

                self.svgClusterRects.filter(function(clusterId, idx) {
                    var statusText = self.graph().nodeGroup(clusterId).status().statusText();
                    return (statusText === "terminated" || statusText === "failed");
                }).transition().duration(1000).style('fill', 'rgb(255,235,235)');

                // HACK - Warnings is not a general feature of node groups
                self.svgClusterRects.filter(function(clusterId, idx) {
                    var warnings = self.graph().nodeGroup(clusterId).properties().warnings;
                    return (warnings && Object.keys(warnings).length > 0);
                }).transition().duration(1000).style('fill', 'rgb(255,214,173)');
            };
            
            self.renderSvg = function(subgraph) {
                $(self.graphSel).empty();
                var g = self.graph().toGraphLib(subgraph);
                var render = new dagreD3.render();
                
                // Set up an SVG group so that we can translate the final graph.
                var graphContainer = d3.select(self.graphSel);                
                self.svg = graphContainer.append("svg"),
                self.svgGraph = self.svg.append("g");
                
                // Run the renderer. This is what draws the final graph.
                render(d3.select("svg g"), g);

                // FIXME - better way to size initial svg
                var graphContainerRect = graphContainer.node().getBoundingClientRect();
                self.svg.attr("height", g.graph().height+40+graphContainerRect.height);
                self.svg.attr("width", g.graph().width+40+graphContainerRect.width);

                self.svgGraph.attr("transform", "translate(20, 20)");

                self.svgClusters = d3.selectAll('g.cluster');
                self.svgNodes = d3.selectAll('g.node');

                self.svgClusterRects = self.svgClusters.selectAll("rect");
                self.svgClusters.attr("id", function(id) { return id; });
                self.svgClusterRects.style('fill-opacity', 0.6);
                
                self.graphNotRendered(false);
                self.current.subGraph(subgraph);
            };

            self.clickedNodeGroupSideBar = function(nodeGroup) {
                if (nodeGroup.name() !== self.current.subGraph()) {
                    self.graphNotRendered(true);
                    self.renderSvg(nodeGroup.name());
                    self.initialScale();
                    self.removeListeners();
                    self.initializeListeners();
                }                     
            };
            
            self.initializeSvg = function() {
                self.initialScale();
                self.setupZoom();
                self.initializeListeners();
            };

            self.removeListeners = function() {
                $('button#zoom-reset').unbind('click', self.resetZoom);
                $('.node').unbind('click', self.clickedNode);
                $('.edge-info-label').unbind(self.clickedEdgeLabel);
                $('.cluster').unbind('click', self.clickedCluster);
            };
            
            self.initializeListeners = function() {
                $(document).on('click', self.graphSel, function(event) {
                    self.groupListVisible(true);
                    self.nodeGroupVisible(false);
                    self.nodeVisible(false);
                });
                $('button#zoom-reset').on('click', self.resetZoom);                                
                $('.node').on('click', self.clickedNode);
                $('.cluster').on('click', self.clickedCluster);
                $('.edge-info-label').on('click', self.clickedEdgeLabel);
                $('button').tooltip({container: 'body', delay: { "show": 250, "hide": 100 }});
                $('#ng-list-table').treetable({expandable: true});                
                $('#ng-list-table').treetable("reveal", self.current.subGraph());
                $("#ng-list-table tbody").on("mousedown", "tr", function() {
                    $(".selected").not(this).removeClass("selected");
                    $(this).toggleClass("selected");
                });
            };

            //
            // Fixme - translation doesn't work with right with scaling
            //
            self.scaleAndCenterClicked = function(e) {
                e.stopPropagation();

                self.resetZoom();
                
                if (self.active.node() === e.currentTarget) {
                    self.active.classed("active", false);
                    self.active = d3.select(null);
                    self.nodeVisible(false);
                    self.nodeGroupVisible(false);
                    return;
                }

                self.active = d3.select(e.currentTarget).classed("active", true);                

                var objBox = e.currentTarget.getBBox();
                var objRect = e.currentTarget.getBoundingClientRect(); 
                var objX = (objRect.left + objRect.right)/2;  // x position of obj with respect to window
                var objY = (objRect.top + objRect.bottom)/2;  // y position of obj with respect to window

                var graphBox = self.svgGraph.node().getBBox();
                var graphRect = $('svg').get(0).getBoundingClientRect();
                var graphX = (graphRect.left + graphRect.right)/2; // x position of graph with respect to window
                var graphY = (graphRect.top + graphRect.bottom)/2; // y position of graph with respect to window
                
                var windowRect  = $(self.graphSel).get(0).getBoundingClientRect();
                var windowWidth = windowRect.width - $('.sidebar').width();
                var windowHeight = $(window).height() - $('.navbar').height();

                var scale = 0.6/Math.max(objBox.width/graphBox.width, objBox.height/graphBox.height);
                var scaledX = scale*(objX - graphX) + graphX;
                var scaledY = scale*(objY - graphY) + graphY;

                var dy = (windowHeight/2 - (scaledY));
                var dx = ($('.sidebar').width()+windowWidth/2 - (scaledX));

                d3.select(self.graphSel).call(
                    self.zoomStatic.translate([-dx/scale, -dy/scale]).scale(scale).event
                );
            };
            
            self.clickedNode = function(e) {
                e.stopPropagation();
                //self.scaleAndCenterClicked(e);
                self.current.node(self.graph().node(e.currentTarget.id));
                self.groupListVisible(false);
                self.nodeGroupVisible(false);
                self.nodeVisible(true);
                $('#node-table').treetable({expandable: true});
            };
            
            self.clickedCluster = function(e) {
                e.stopPropagation();
                // FIXME - want dagreD3 to put clusterId on the element rather than in element data
                var clusterId = d3.select(e.currentTarget).data()[0];
                var nodeGroup = self.graph().nodeGroup(clusterId);
                
                self.current.nodeGroup(nodeGroup);
                self.groupListVisible(false);
                self.nodeGroupVisible(true);
                self.nodeVisible(false);

                $('#ng-table').treetable({expandable: true});
            };

            self.clickedEdgeLabel = function(e) {
                e.stopPropagation();                
                var edgeId = e.currentTarget.id.replace("edgeLabel_","");
                var edge = self.graph().edge(edgeId);
                self.current.edge(edge);
                $('#myModal').modal('toggle');
            };
            
            self.resetZoom = function() {
                self.groupListVisible(true);
                self.svgGraph.call(
                    self.zoom.translate([self.initialZoom.x,self.initialZoom.y]).scale(self.initialZoom.scale).event
                );
            };
            
            self.zoom = d3.behavior.zoom().on("zoom", function() {
                self.svgGraph.attr('transform', 'translate('+d3.event.translate+')scale('+d3.event.scale+')');
            });
                        
            self.initialScale = function() {

                var graphRect = self.svg.node().getBBox();                
                var pageRect  = $(self.graphSel).get(0).getBoundingClientRect();
                var width = pageRect.width;
                var height = $(window).height() - $('.navbar').height();
                
                var heightOffset = parseFloat($(self.graphSel).css('padding-top'))
                    + parseFloat($(self.graphSel).css('padding-bottom'))
                    + parseFloat($(self.graphSel).css('margin-top'))
                    + parseFloat($(self.graphSel).css('margin-bottom'));
                var widthOffset  = parseFloat($(self.graphSel).css('padding-left'))
                    + parseFloat($(self.graphSel).css('padding-right'))
                    + parseFloat($(self.graphSel).css('margin-left'))
                    + parseFloat($(self.graphSel).css('margin-right'));
                
                var heightRatio = (height - heightOffset) / graphRect.height;
                var widthRatio  = (width - widthOffset) / graphRect.width;
                var zoomLevel = (heightRatio >= widthRatio) ? widthRatio : heightRatio;
                var zoom = Math.floor(zoomLevel * 10) / 10 - 0.1;
                
                if (zoom > 1.0) {
                    zoom = 1.0;
                } else if (zoom < 0.1) {
                    zoom = 0.1;
                }
                
                var coords = d3.transform(self.svgGraph.attr('transform')).translate;
                var x = coords[0]*zoom;
                var y = coords[1]*zoom;

                self.initialZoom = {
                    x: x, y: y, scale: zoom
                };
                
                self.svgGraph.call(
                    self.zoom.translate([x,y]).scale(zoom).event
                );
            };

            self.setupZoom = function() {
                d3.select(self.graphSel).call(
                    self.zoom
                );
            };
        };

        return new WorkflowViewModel();
    }
);
