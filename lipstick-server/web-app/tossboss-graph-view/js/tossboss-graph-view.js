/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/** tossboss-graph-view.js
 * Responsible for drawing the Graph and applying data to the Graph.
 *
 * LISTENS FOR EVENTS:
 * - clickOutsideGraph.tossboss-graph-view
 * - loadBreakpointCode.tossboss-graph-script
 * - loadGraphModel.tossboss-graph-model
 * - loadRunStatsData.tossboss-graph-model
 * - mouseEnterLogicalOperator.tossboss-graph-view
 * - mouseEnterMRJob.tossboss-graph-view
 * - mouseLeaveLogicalOperator.tossboss-graph-view
 * - mouseLeaveMRJob.tossboss-graph-view
 *
 * TRIGGERS EVENTS:
 * - clickEdge.tossboss-graph-view
 * - clickLogicalOperator.tossboss-graph-view
 * - clickMRJob.tossboss-graph-view
 * - clickOutsideGraph.tossboss-graph-view
 * - drawGraph.tossboss-graph-view
 * - mouseEnterMRJob.tossboss-graph-view
 * - mouseLeaveMRJob.tossboss-graph-view
 */

;GraphView = {
    options: {
        pageHasDrawers: false,
        zoomLevel: 0,
        highlightedIds: [],
        highlightedEdges: [],
        pulseInterval: undefined,
        graphSel: '#pig-graph',
        nodeSel:  'g.node',
        edgeSel:  'g.edge',
        pageSel:  '.page',
        runningMapSel: 'g.cluster.running-map > polygon',
        runningRedSel: 'g.cluster.running-reduce > polygon',
        graphState : {},
    },
    /**
     * Start all custom event listeners.
     */
    startListeners: function() {
        // On outside graph click, highlight all nodes in the graph.
        $(document).on('clickOutsideGraph.tossboss-graph-view', function(event) {
            GraphView.highlightAll();
        });
        // On getting breakpoint code from Script, highlight the appropriate nodes in the graph.
        $(document).on('loadBreakpointCode.tossboss-graph-script', function(event, id) {
            GraphView.highlightPath(id);
        });
        // On getting graph data from GraphModel, draw the graph.
        $(document).on('loadGraphModel.tossboss-graph-model', function(event) {
            GraphView.drawGraph('optimized');
            GraphView.addDataToGraph();
            GraphView.zoom('reset');
        });
        // On getting run stats data from GraphModel, draw run stats on graph.
        $(document).on('loadRunStatsData.tossboss-graph-model', function(event) {
            GraphView.drawRunStats();
            // Check to see if script has stopped running.
            if (GraphModel.options.runStatsData.statusText.toLowerCase() != "running") {
                clearInterval(GraphView.options.pulseInterval);
                GraphView.pulseJobs();
                _.delay(GraphView.drawRunStats, 2000);
            }
        });
        $(document).on('mouseEnterLogicalOperator.tossboss-graph-view', function(event, id) {
        });
        $(document).on('mouseEnterMRJob.tossboss-graph-view', function(event, id) {
        });
        $(document).on('mouseLeaveLogicalOperator.tossboss-graph-view', function(event) {
        });
        $(document).on('mouseLeaveMRJob.tossboss-graph-view', function(event) {
        });
    },
    /**
     * Initialize the GraphView object.
     */
    initialize: function() {
        GraphView.startListeners();
        if ($('div#drawers').length > 0) { GraphView.options.pageHasDrawers = true; }
        // Set interval to pulse running map-reduce jobs.
        clearInterval(GraphView.options.pulseInterval);
        GraphView.options.pulseInterval = setInterval(GraphView.pulseJobs, 2000);
        // Bind events.
        $(document).on('click', GraphView.options.pageSel, function(event) {
            $(document).trigger('clickOutsideGraph.tossboss-graph-view');
        });
        $(document).on('mouseenter', 'text[data-record-count]', function(event) {
            // Could not get 'g.edge.intermediate text[data-record-count]' selector to work properly so have to filter classList
            if (_.contains(event.target.parentElement.classList, 'intermediate')) {
                d3.select(this).classed('mouseover',true);
            }
        });
        $(document).on('mouseleave click', 'text', function(event) {
            d3.select(this).classed('mouseover',false);
        });
        $(document).on('mouseenter', '.sample-output-icon', function(event) {
            $(this).addClass('mouseover');
        });
        $(document).on('mouseleave click', '.sample-output-icon', function(event) {
            $(this).removeClass('mouseover');
        });

        /* Mouse Panning of Zoomed Graph */
        $(document).on('mousedown', GraphView.options.pageSel, function (event) {
            target = $(event.target);

            /* Click is on an inner node with text, don't enable drag 
               behavior so the text can be selected */
            if (target.parent().is("g.node") || target.parent().is("g.edge")) {
                return true;
            }
            this.prevPosY = event.pageY;
            this.prevPosX = event.pageX;
            this.mouseDown = true;
            $(".graph-container").css('cursor', 'move');
            return false;
        });
        $(document).on('mousemove', GraphView.options.pageSel, function (event) {
            if (this.mouseDown) {
                var container = $(".graph-container");

                /* Calculate the new scroll value, equal to the current value plus the
                   distance the mouse has moved since the last event, avoiding negative
                   scroll values */
                var scrollLeft = Math.max(0, container.scrollLeft() + event.pageX - this.prevPosX);
                var scrollTop = Math.max(0, container.scrollTop() + event.pageY - this.prevPosY);
                
                /* Store the last cursor position */
                this.prevPosX = event.pageX;
                this.prevPosY = event.pageY;

                /* And adjust the scrolling */
                container.scrollLeft(scrollLeft);
                container.scrollTop(scrollTop);

            }
        });
        $.each(['mouseleave', 'mouseup'], function(i, eventName) {
            $(document).on(eventName, GraphView.options.pageSel, function(event) {
                this.mouseDown = false;
                $(".graph-container").css( 'cursor', 'auto' );
            });
        });

    },
    /**
     * Draws the graph.
     *
     * @param {String} type The graph type to draw ('optimized' or 'unoptimized')
     */
    drawGraph: function(type) {
        var lowerType = type.toLowerCase();
        // Cache current scroll position and zoom
        GraphView.options.graphState[GraphModel.options.graphType.toLowerCase()] = {
            scroll : { 
                x : $(GraphView.options.pageSel).scrollLeft(),
                y : $(GraphView.options.pageSel).scrollTop(),
            },
            zoom : GraphView.options.zoomLevel
        };

        // Set GraphModel graph type and get SVG data for type.
        GraphModel.options.graphType = type.toLowerCase();
        var svgData = GraphModel.getSvgData();
        // Clear current graph and draw new graph.
        $(GraphView.options.graphSel).empty();
        $(GraphView.options.graphSel).html(svgData);
        // The SVG has a white background polygon, remove it.
        $('g.graph > polygon').remove();
        $('.page').scrollTop(0);

        // Restore to previous state if it exists
        if(GraphView.options.graphState[lowerType]) {
            GraphView.zoom(GraphView.options.graphState[lowerType].zoom);
            $(GraphView.options.pageSel).scrollTop(GraphView.options.graphState[lowerType].scroll.y);
            $(GraphView.options.pageSel).scrollLeft(GraphView.options.graphState[lowerType].scroll.x);
        } else {
            GraphView.zoom('reset');
            scrollTo(0,0);
        }

        // Bind events.
        $('.node').on('click', function(e) {
            e.stopPropagation();
            $(this).trigger('clickLogicalOperator.tossboss-graph-view', [e.currentTarget.id]);
        });
        $('.cluster').on('click', function(e) {
            e.stopPropagation();
            $(this).trigger('clickMRJob.tossboss-graph-view', [e.currentTarget.id]);
        });
        $('.cluster').hover(
            function(e) {
                $(this).trigger('mouseEnterMRJob.tossboss-graph-view', [e.currentTarget.id]);
            },
            function(e) {
                $(this).trigger('mouseLeaveMRJob.tossboss-graph-view', [e.currentTarget.id]);
        });
        $(document).trigger('drawGraph.tossboss-graph-view', [type]);
    },
    /**
     * Zooms the view of the graph in 10% (0.1) increments (1.0 = 100% zoom, 0.5 = 50% zoom).
     * Zoom mode 'reset', will try to show as much of the graph as possible.
     *
     * @param {String} mode The zoom mode ('in', 'out', 'reset', or number), if mode is a number zoom to that level (1.0 = 100%)
     */
    zoom: function(mode) {
        // Zoom in.
        if (mode === 'in') {
            GraphView.options.zoomLevel += 0.1;
        }
        // Zoom out.
        else if (mode === 'out') {
            GraphView.options.zoomLevel -= 0.1;
        }
        // Zoom reset.
        else if (mode === 'reset') {
            // Set zoom to 100% to get accurate numbers.
            $(GraphView.options.graphSel).transition({ scale: 1.0 }, 0);
            // Get bounding rects for SVG and page and any offsets.
            var graphRect = $('svg').get(0).getBoundingClientRect();
            var pageRect  = $(GraphView.options.pageSel).get(0).getBoundingClientRect();
            var heightOffset = parseFloat($(GraphView.options.pageSel).css('padding-top'))
                             + parseFloat($(GraphView.options.pageSel).css('padding-bottom'))
                             + parseFloat($(GraphView.options.pageSel).css('margin-top'))
                             + parseFloat($(GraphView.options.pageSel).css('margin-bottom'));
            var widthOffset  = parseFloat($(GraphView.options.pageSel).css('padding-left'))
                             + parseFloat($(GraphView.options.pageSel).css('padding-right'))
                             + parseFloat($(GraphView.options.pageSel).css('margin-left'))
                             + parseFloat($(GraphView.options.pageSel).css('margin-right'));
            // Calculate height and width ratios and take the smaller value as zoomLevel.
            var heightRatio = (pageRect.height - heightOffset) / graphRect.height;
            var widthRatio  = (pageRect.width - widthOffset) / graphRect.width;
            // Get the smaller ratio.
            var zoomLevel = (heightRatio >= widthRatio) ? widthRatio : heightRatio;
            GraphView.options.zoomLevel = Math.floor(zoomLevel * 10) / 10;
            $(GraphView.options.pageSel).scrollTop(0);
            // Cap zoomLevel at 1.0.
            if (GraphView.options.zoomLevel > 1.0) {
                GraphView.options.zoomLevel = 1.0;
            }
        }
        // Zoom to specific amount.
        else {
            GraphView.options.zoomLevel = mode;
        }
        // Set lower boundary of zoomLevel to 0.1 (10%).
        if (GraphView.options.zoomLevel < 0.1) {
            GraphView.options.zoomLevel = 0.1;
        }
        $(GraphView.options.graphSel).transition({ scale: GraphView.options.zoomLevel }, 0);
    },
    /**
     * Highlight the graph path to a node.
     *
     * @param {Number} id The node id to highlight path to
     */
    highlightPath: function(id) {
        if (id) {
            GraphView.options.highlightedIds = [];
            GraphView.options.highlightedEdges = [];
            GraphView.options.highlightedIds.push(String(id));
            GraphView.unhighlightAll();
            // Get node predecessors and edges for end node.
            GraphView.getPredecessors([String(id)]);
            // Change highlighting of nodes and edges to 'on'.
            GraphView.changeHighlight(GraphView.options.highlightedIds.concat(GraphView.options.highlightedEdges), 'on');
        }
    },
    /**
     * Get the node and edge predecssors for a given node and climb up the tree.
     *
     * @param {Array} ids An array of node ids to get the predecssors for
     */
    getPredecessors: function(ids) {
        // Get the appropriate Pig data.
        var pigData = GraphModel.getPigData();
        var parentIds = [];
        // Loop through node ids.
        _.each(ids, function(id) {
            // Get edges that point to the node.
            _.each($('.edge[data-end="'+id+'"]'), function(element) {
                GraphView.options.highlightedEdges.push($(element).attr('id'));
            });
            // Get all predecessor node ids for the node.
            GraphView.options.highlightedIds = GraphView.options.highlightedIds.concat(pigData[id]['predecessors']);
            parentIds = parentIds.concat(pigData[id]['predecessors']);
        });
        // If there are predecessors, keep going. Otherwise stop.
        if (parentIds.length > 0) {
            GraphView.getPredecessors(parentIds);
        }
    },
    /**
     * Highlight all nodes and edges in the graph.
     */
    highlightAll: function() {
        $(GraphView.options.nodeSel).attr('opacity','1');
        $(GraphView.options.edgeSel).attr('opacity','1');
    },
    /**
     * Unhighlight all nodes and edges in the graph.
     */
    unhighlightAll: function() {
        $(GraphView.options.nodeSel).attr('opacity','0.3');
        $(GraphView.options.edgeSel).attr('opacity','0.3');
    },
    /**
     * Change the highlight of a nodes and/or edges.
     *
     * @param {Array} ids An array of node and/or edge ids to change
     * @param {String} mode Highlight mode ('on' or 'off')
     */
    changeHighlight: function(ids, mode) {
        if (!(ids instanceof Array)) { ids = [ids]; }
        if (mode.toLowerCase() === 'on') {
            _.each(ids, function(id) {
                $('g #'+id).attr('opacity','1');
            });
        }
        else if (mode.toLowerCase() === 'off') {
            _.each(ids, function(id) {
                $('g #'+id).attr('opacity','0.3');
            });
        }
    },
    /**
     * Focus the graph view on to the specified node, cluster, or edge id, and highlight.
     *
     * @param {String} id The cluster, node, or edge id to focus
     */
    focusObject: function(id) {
        var topOffset  = parseFloat($('.navbar').get(0).getBoundingClientRect().height)
                       + parseFloat($(GraphView.options.pageSel).css('padding-top'))
                       + parseFloat($(GraphView.options.pageSel).css('margin-top'));
        var leftOffset = parseFloat($('#left-drawer .handle').get(0).getBoundingClientRect().width)
                       + parseFloat($(GraphView.options.pageSel).css('padding-left'))
                       + parseFloat($(GraphView.options.pageSel).css('margin-left'));
        var pageRect = $(GraphView.options.pageSel).get(0).getBoundingClientRect();
        var focusObj = $('#'+id);
        var focusObjRect = focusObj.get(0).getBoundingClientRect();
        var focusObjTop  = focusObjRect.top  + parseFloat($(GraphView.options.pageSel).get(0).scrollTop);
        var focusObjLeft = focusObjRect.left + parseFloat($(GraphView.options.pageSel).get(0).scrollLeft);
        var horizontalScale = Math.round((pageRect.width  / focusObjRect.width)  * 10) / 10;
        var verticalScale   = Math.round((pageRect.height / focusObjRect.height) * 10) / 10;
        if (horizontalScale > verticalScale) {
            GraphView.zoom(verticalScale - 0.2);
        }
        else {
            GraphView.zoom(horizontalScale - 0.2);
        }
        focusObjRect = focusObj.get(0).getBoundingClientRect();
        var focusObjTop  = focusObjRect.top  + parseFloat($(GraphView.options.pageSel).get(0).scrollTop);
        var focusObjLeft = focusObjRect.left + parseFloat($(GraphView.options.pageSel).get(0).scrollLeft);
        $(GraphView.options.pageSel).scrollTop(focusObjTop - topOffset - 10);
        $(GraphView.options.pageSel).scrollLeft(focusObjLeft - leftOffset - 10);
    },
    /**
     * Display the object at the target selection with the specified position at target.
     *
     * @param {Object} displayObj The object to display
     * @param {String} targetSel The target selector for the object to be displayed at
     * @param {String} position The position at target to display ('top', 'bottom', 'left', or 'right')
     */
    displayObject: function(displayObj, targetSel, position) {
        var displayObj = $(displayObj);
        _.each($(targetSel), function(targetObj, index) {
            // Get the top, left, height, and width of the TargetObj relative 
            // to the SVG container
            var boundingBox = targetObj.getBBox();
            var matrix  = targetObj.getCTM();
            var svg = $(targetObj).closest('svg').get(0);
            var pt_tl  = svg.createSVGPoint();
            var pt_br  = svg.createSVGPoint();

            pt_tl.x = boundingBox.x; 
            pt_tl.y = boundingBox.y; 
            pt_br.x = boundingBox.x + boundingBox.width; 
            pt_br.y = boundingBox.y + boundingBox.height; 
            pt_tl = pt_tl.matrixTransform(matrix);
            pt_br = pt_br.matrixTransform(matrix);

            var targetObjHeight = pt_br.y - pt_tl.y;
            var targetObjWidth  = pt_br.x - pt_tl.x;
            var displayObjTop  = pt_tl.y;
            var displayObjLeft = pt_tl.x;

            // Clone and add display object to page.
            var displayObjCopy = $(displayObj).clone();
            $(GraphView.options.graphSel).append(displayObjCopy);
            displayObjCopy.css('position', 'absolute');

            // Calculate display object's new top and left values according to position setting.
            if (position.toLowerCase() === 'top') {
                displayObjTop -= displayObjCopy.outerHeight();
                displayObjLeft += (targetObjWidth / 2) - (displayObjCopy.outerWidth() / 2);
            }
            else if (position.toLowerCase() === 'bottom') {
                displayObjTop += targetObjHeight;
                displayObjLeft += (targetObjWidth / 2) - (displayObjCopy.outerWidth() / 2);
            }
            else if (position.toLowerCase() === 'left') {
                displayObjTop += (targetObjHeight / 2) - (displayObjCopy.outerHeight() / 2);
                displayObjLeft -= displayObjCopy.outerWidth();
            }
            else if (position.toLowerCase() === 'right') {
                displayObjTop += (targetObjHeight / 2) - (displayObjCopy.outerHeight() / 2);
                displayObjLeft += targetObjWidth;
            }
            displayObjCopy.css({top:  displayObjTop  + 'px',
                                left: displayObjLeft + 'px'}).show();

            displayObjCopy.on('click', function(event) {
                var startScopeId = $(this).attr('data-start-scopeId');
                var startNodeId  = $('g.'+startScopeId+'-out').attr('data-start');
                $(this).trigger('clickEdge.tossboss-graph-view', [startNodeId, '', startScopeId, '']);
            });
        });
    },
    /**
     * Add any additional data to graph SVG.
     */
    addDataToGraph: function() {
        var s3StorageFunctions = ['PigStorage','AegisthusBagLoader','AegisthusMapLoader','PartitionedLoader','PartitionedStorer'];
        var dumpStorageFunctions = ['TFileStorage','InnerStorage'];
        // Get Pig data.
        var pigData = GraphModel.getPigData();
        // Add data to each cluster.
        _.each($('g.cluster'), function(element) {
            var clusterTitle = $(element).find('title').text();
            var mapRedJobId  = 'scope-' + clusterTitle.replace('cluster_scope','');
            $(element).data('mr-jobid', mapRedJobId);
            $(element).attr('id', mapRedJobId);
        });
        // Add data to each node.
        _.each(pigData, function(aliasInfo, id) {
            var lineNumber = -1;
            if (aliasInfo['location']['macro'].length == 0) {
                lineNumber = aliasInfo['location']['line'];
            }
            $('g#'+id).data('line-number',lineNumber);
        });
        // Add data to each edge.
        _.each($('g.edge'), function(element) {
            // Get edge's start and end node objects.
            var title = $(element).find('title').text().split('->');
            var start = title[0];
            var end   = title[1];
            var startObj = pigData[start];
            var endObj   = pigData[end];
            // Get edge's start and end map-reduce scope ids.
            var startScopeId = (startObj.mapReduce) ? startObj.mapReduce.jobId : '';
            var endScopeId   = (endObj.mapReduce)   ? endObj.mapReduce.jobId   : '';
            // Add edge's start node's operator to class
            d3.select(element).classed(startObj.operator,true);
            // If edge's start node is a beginning node (nothing points to it), add data.
            if (startObj.predecessors.length == 0) {
                d3.select(element).classed(startScopeId+'-in',true);
                if (startObj.hasOwnProperty('storageLocation')) {
                    var storageLocation = startObj.storageLocation.slice(-40);
                    if (_.contains(s3StorageFunctions, startObj.storageFunction)) {
                        storageLocation = storageLocation.split('/').reverse()[0].slice(-40);
                    }
                    $(element).attr('data-storagelocation-in',storageLocation);
                }
            }
            // If edge's end node is an end node (nothing after it), add data.
            if (endObj.successors.length == 0) {
                d3.select(element).classed(startScopeId+'-out',true);
                if (endObj.hasOwnProperty('storageLocation')) {
                    var storageLocation = endObj.storageLocation.slice(-40);
                    if (_.contains(s3StorageFunctions, endObj.storageFunction)) {
                        storageLocation = storageLocation.split('/').reverse()[0].slice(-40);
                    }
                    $(element).attr('data-storagelocation-out',storageLocation);
                    if (_.contains(dumpStorageFunctions, endObj.storageFunction)) {
                        d3.select(element).classed('intermediate',true);
                    }
                }
            }
            // If edge crosses map-reduce jobs.
            if (startScopeId != endScopeId) {
                d3.select(element).classed('intermediate',true);
                // If edge is output of a split, label only 1 edge
                if ((startObj.operator !== 'LOSplit') || ($('.edge.'+startScopeId+'-out.'+startObj.operator).length === 0)) {
                    d3.select(element).classed(startScopeId+'-out',true);
                }
            }
            if (startObj.predecessors.length > 0 && endObj.successors.length > 0) {
                d3.select(element).classed('intermediate',true);
            }
            GraphView.addEdgeTextElement(element);
            $(element).attr('data-start',start);
            $(element).attr('data-end',end);
            $(element).attr('data-start-scopeId',startScopeId);
            $(element).attr('data-end-scopeId',endScopeId);
        });
        // Bind events.
        $('g.edge').on('click', function(event) {
            $(this).trigger('clickEdge.tossboss-graph-view', [$(this).attr('data-start'), $(this).attr('data-end'), $(this).attr('data-start-scopeId'), $(this).attr('data-end-scopeId')]);
        });
    },
    /**
     * Display a record count on edges.
     *
     * @param {String} cls The element class for the edge
     * @param {String} recordCount The record count to display
     * @param {String} counterName If record count is part of a multi-counter, the counter name (optional)
     */
    displayRecordCount: function(cls, recordCount, counterName) {
        if (counterName) {
            var counterStorageLocation = counterName.split('_').slice(2).join('_');
            // Loop through edges.
            _.each($('g.edge.'+cls), function(edge) {
                // Get storage location data for the edge.
                edgeStorageLocationIn  = $(edge).attr('data-storagelocation-in');
                edgeStorageLocationOut = $(edge).attr('data-storagelocation-out');
                if (counterStorageLocation === edgeStorageLocationIn) {
                    $(edge).children('text').text(GraphView.addCommas(recordCount)).attr('data-record-count', recordCount);
                }
                if (counterStorageLocation === edgeStorageLocationOut) {
                    $(edge).children('text').text(GraphView.addCommas(recordCount)).attr('data-record-count', recordCount);
                }
            });
        }
        else {
            $('g.edge.'+cls+' text').text(GraphView.addCommas(recordCount)).attr('data-record-count', recordCount);
        }
        // If "-out" edge and has record count, display sample-output-icon.
        if (cls.match(/-out/g) && typeof(recordCount) === "number") {
            var startScopeId = cls.replace('-out','');
            GraphView.displayObject('<i class="icon-info-sign sample-output-icon intermediate '+cls+'" data-start-scopeId="'+startScopeId+'"></i>',
                                    'g.edge.'+cls+'.intermediate text[data-record-count]',
                                    'right');
        }
    },
    /**
     * Draw run stats data on graph.
     */
    drawRunStats: function() {
        // Get appropriate run stats data.
        var runStatsData = GraphModel.options.runStatsData;
        progress = runStatsData['progress'];
        isScriptSuccessful = true;
        // Clear running class on clusters.
        $('g.cluster[class*="running"]').attr('class','cluster');
        // Get all running map-reduce jobs and loop through them.
        runningJobs = _.filter(runStatsData.jobStatusMap, function(job) { return !job['isComplete'] && !job['isSuccessful']; });
        _.each(runningJobs, function(jobStats, jobTrackerId) {
            // Add running class to cluster element.
            if (jobStats['mapProgress'] < 1.0) {
                $('g#'+jobStats['scope']).attr('class','cluster running-map');
            }
            else if (jobStats['totalReducers'] > 0) {
                $('g#'+jobStats['scope']).attr('class','cluster running-reduce');
            }
        });
        // Detect if script was successful.
        if (_.find(runStatsData['jobStatusMap'], function(job) { return job['isComplete'] && !job['isSuccessful']; })) {
            isScriptSuccessful = false;
        }
        // Remove all sample-output-icons (they will get reapplied when record counts are updated).
        $('.sample-output-icon').remove();
        // Loop through all map-reduce jobs.
        _.each(runStatsData.jobStatusMap, function(jobStats, jobTrackerId) {
            var count_in = 0;
            var count_out = 0;
            var scopeId = jobStats.scope;
            var jobId = jobStats.jobId;
            var mapProgress = Math.floor(parseFloat(jobStats.mapProgress) * 100);
            var reduceProgress = Math.floor(parseFloat(jobStats.reduceProgress) * 100);
            var progress = (jobStats.totalReducers>0) ? (mapProgress+reduceProgress)/2 : mapProgress;
            // Display record counts.
            if (jobStats.counters.hasOwnProperty('Map-Reduce Framework')) {
                // Input counters.
                if (jobStats.counters.hasOwnProperty('MultiInputCounters')) {
                    _.each(jobStats.counters.MultiInputCounters.counters, function(count, counter) {
                        GraphView.displayRecordCount(scopeId+'-in', count, counter);
                    });
                }
                else {
                    count_in = jobStats.counters['Map-Reduce Framework'].counters['Map input records'];
                    GraphView.displayRecordCount(scopeId+'-in', count_in);
                }
                // Output counters.
                if (jobStats.counters.hasOwnProperty('MultiStoreCounters')) {
                    _.each(jobStats.counters.MultiStoreCounters.counters, function(count, counter) {
                        GraphView.displayRecordCount(scopeId+'-out', count, counter);
                    });
                }
                else {
                    count_out = (jobStats.counters['Map-Reduce Framework'].counters.hasOwnProperty('Reduce output records')) ? jobStats.counters['Map-Reduce Framework'].counters['Reduce output records'] : '';
                    if (jobStats.totalReducers === 0) {
                        count_out = jobStats['counters']['Map-Reduce Framework']['counters']['Map output records'];
                    }
                    GraphView.displayRecordCount(scopeId+'-out', count_out);
                }
            }
            // Update jobId's progress bars.
            $('.'+jobId+' div.map').css('width',mapProgress+'%');
            $('.'+jobId+' div.map').html(mapProgress+'%');
            $('.'+jobId+' div.reduce').css('width',reduceProgress+'%');
            $('.'+jobId+' div.reduce').html(reduceProgress+'%');

            // Add class for finished job.
            var jobWarnings = GraphView.findJobWarnings(jobStats);
            if (0 < jobWarnings.length) {
                $('g#'+scopeId).attr('class','cluster warnings');
            } else if (jobStats.isComplete && jobStats.isSuccessful) {
                $('g#'+scopeId).attr('class','cluster success');
            }
            else if (jobStats.isComplete && !jobStats.isSuccessful) {
                $('g#'+scopeId).attr('class','cluster fail');
            }

            /* And rendering warnings if complete or not */
            if (0 < jobWarnings.length) {
                GraphView.renderJobWarnings(jobId, scopeId, jobWarnings);
            }
        });

        // Draw script progress bar.
        $('div.navbar .progress').show();
        $('div.navbar .progress').removeClass('active').removeClass('progress-success').removeClass('progress-info').removeClass('progress-danger').removeClass('progress-warning').removeClass('progress-striped');
        switch(runStatsData.statusText)
        {
            case "finished":
                $('div.navbar .progress').addClass('progress-success');
                break;
            case "failed":
                $('div.navbar .progress').addClass('progress-danger');
                break;
            case "terminated":
                $('div.navbar .progress').addClass('progress-warning');
                break;
            default:
                $('div.navbar .progress').addClass('progress-striped').addClass('active');
        }
        $('#overall-progress-bar').css('width',progress+'%');
        $('#overall-progress-bar').html(progress+'%');
        // Script is finished (successfully or failed).
        if (progress === 100 || !isScriptSuccessful) {
            $('div.navbar .progress').removeClass('active');
        }
        // Change bgcolor of finished map-reduce jobs.
        $('g.cluster.success polygon').css('fill','#CFE1E8');
        $('g.cluster.fail polygon').css('fill','#FFEBEB');
        $('g.cluster.warnings polygon').css('fill','#FFD6AD');
    },

    /* Find and construct any warning messages relevant to this specific
       map reduce job.  */
    findJobWarnings: function(jobStats) {
        var jobWarnings = [];
        for (warningKey in jobStats.warnings) {
            jobWarnings.push(
                _.template(Templates.jobWarningMessages[warningKey], 
                           jobStats.warnings[warningKey], 
                           {variable:'data'}));
        }
        return jobWarnings;
    },

    /* Draw out warnings modal and icon for a job */
    renderJobWarnings: function(jobId, scopeId, warnings) {
        if (!(0 < warnings.length)) {
            console.log("No warnings supplied so I will not render these");
            return;
        }
        if (0 == $("#warning-modal-" + scopeId).length) {
            GraphView.addWarningModal(jobId, scopeId);
        }

        if (0 == $("#warning-icon-" + scopeId).length) {
            GraphView.addWarningIcon(scopeId);
        }
        
        GraphView.setJobWarnings(scopeId, warnings);
    },

    /* Draw out warnings icon for a job */
    addWarningIcon: function (scopeId) {
        var icon = $("<i>", {
            'class': 'icon-warning-sign sample-output-icon intermediate',
            'id': "warning-icon-" + scopeId,
        });

        var scopeBox = $("#" + scopeId);
        if (0 == scopeBox.length) {
            return;
        }
        scopeBox = scopeBox[0];
        var boundingBox = scopeBox.getBBox();
        var matrix  = scopeBox.getCTM();
        var svg = $(scopeBox).closest('svg').get(0);
        var pt_tl  = svg.createSVGPoint();
        var pt_br  = svg.createSVGPoint();

        pt_tl.x = boundingBox.x; 
        pt_tl.y = boundingBox.y; 
        pt_br.x = boundingBox.x + boundingBox.width; 
        pt_br.y = boundingBox.y + boundingBox.height; 
        pt_tl = pt_tl.matrixTransform(matrix);
        pt_br = pt_br.matrixTransform(matrix);
        
        $(GraphView.options.graphSel).append(icon);
        icon.css('position', 'absolute');
        iconTop = pt_tl.y - icon.outerHeight() + 12;
        iconLeft = pt_br.x + 10;

        icon.css({top:  iconTop  + 'px',
                      left: iconLeft + 'px'}).show();
        $("#warning-icon-" + scopeId).on('click', function () {
            console.log("Warning Icon clicked for scope " + scopeId);
            $("#warning-modal-" + scopeId).modal('toggle'); 
        });
    },

    /* Draw out warnings modal for a job */
    addWarningModal: function (jobId, scopeId) {
        $('body').prepend(_.template(Templates.jobWarningModalTmpl, {'job_id': jobId, 'scope_id': scopeId}, {variable:'data'}));
        $("#warning-modal-" + scopeId).css({
            'width': function () {
                return ($(document).width() * .9) + 'px';
            },
            'margin-left': function () {
                return -($(this).width() / 2);
            }
        })
    },

    setJobWarnings: function(scopeId, warnings) {
        console.log("Setting job warnings for " + scopeId);
        var modal = $('#warning-modal-body-' + scopeId);
        modal.empty();
        var html = _.template(Templates.jobWarningModalBodyTmpl, {'job_warnings': warnings}, {variable:'data'})
        modal.append(html);
    },

    /**
     * Adds a text element to the edge
     *
     * @param {Object} edge The edge element object
     */
    addEdgeTextElement: function(edge) {
        var edgeObj = $(edge);
        var edge    = edgeObj.get(0);
        // Get head and tail X,Y coordinates
        var headCoordinates = $(edge).find('polygon').attr('points').split(' ')[1].split(',');
        var tailCoordinates = $(edge).find('path').attr('d').replace('M','').split('C')[0].split(',');
        var headX = parseFloat(headCoordinates[0]);
        var headY = parseFloat(headCoordinates[1]);
        var tailX = parseFloat(tailCoordinates[0]);
        var tailY = parseFloat(tailCoordinates[1]);
        // Default text area's X,Y coordinates to display on right side of edge
        var x = tailX + 5;
        var y = tailY + 23;
        $(edge).attr('data-text-placement','right');
        //d3.select(edge).append('rect').attr('x',x).attr('y',y-12).attr('width','100').attr('height','15').attr('fill','white');
        d3.select(edge).append('text').attr('x',x).attr('y',y).attr('font-size','0.7em');
        // If arrow favors right, move text X,Y coordinates to left side of edge
        if (headX > tailX) {
            x = tailX - 15;
            y = tailY + 23;
            $(edge).attr('data-text-placement','left');
            d3.select(edge).select('text').attr('x',x).attr('y',y).attr('text-anchor','end');
        }
    },
    /**
     * Changes background color of cluster for running map and reduce jobs.
     */
    pulseJobs: function() {
        $(GraphView.options.runningMapSel).transition({fill:'#E9E9E9', duration: 1000}).transition({fill:'#3299bb', duration: 500});
        $(GraphView.options.runningRedSel).transition({fill:'#E9E9E9', duration: 1000}).transition({fill:'#ff9900', duration: 500});
    },
    /**
     * Add commas to numbers.
     * Example: addCommas('123123') -> '123,123'
     *
     * @param {String} nStr The number to add commas to
     * @return {String} Returns the number with commas added
     */
    addCommas: function(nStr) {
        nStr += '';
        x = nStr.split('.');
        x1 = x[0];
        x2 = x.length > 1 ? '.' + x[1] : '';
        var rgx = /(\d+)(\d{3})/;
        while (rgx.test(x1)) {
            x1 = x1.replace(rgx, '$1' + ',' + '$2');
        }
        result = (x1 + x2 == 'undefined') ? '' : x1 + x2;
        return result;
    }
};
