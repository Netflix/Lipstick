 /** tossboss-detail-view.js
  * Responsible for drawing and maintaining the details of the tossboss-graph-view.
  *
  * LISTENS FOR EVENTS:
  * - clickLogicalOperator.tossboss-graph-view
  * - clickMRJob.tossboss-graph-view
  * - clickOutsideGraph.tossboss-graph-view
  * - loadRunStatsData.tossboss-graph-model
  *
  */
;DetailView = {
    options: {
        containerSel: '#right-drawer .container',
        objDefaults: {
            groupName: '',
            objectName: '',
            title: '',
            html: 'Hello World!',
        },
    },

    /**
     * Start all custom event listeners.
     */
    startListeners: function() {
        // On logical operator click, populate DetailView.
        $(document).on('clickLogicalOperator.tossboss-graph-view', function(event, uid) {
            $(DetailView.options.containerSel).scrollTop(0);
            DetailView.populateLOInfo(uid);
        });
        // On map-reduce job click, populate DetailView.
        $(document).on('clickMRJob.tossboss-graph-view', function(event, scopeId) {
            $(DetailView.options.containerSel).scrollTop(0);
            DetailView.populateMRInfo(scopeId);
        });
        // On outside graph click, populate DetailView.
        $(document).on('clickOutsideGraph.tossboss-graph-view', function(event) {
            $(DetailView.options.containerSel).scrollTop(0);
            DetailView.populateScriptInfo();
        });
        // On getting run stats data from GraphModel, update DetailView information.
        $(document).on('loadRunStatsData.tossboss-graph-model', function(event) {
            var runStatsData = GraphModel.options.runStatsData;
            // Display script status.
            var startTime, endTime, heartbeatTime = undefined;
            if (runStatsData.startTime) {
                var myDate = new Date(runStatsData.startTime);
                startTime = myDate.toLocaleString();
            }
            if (runStatsData.endTime) {
                var myDate = new Date(runStatsData.endTime);
                endTime = myDate.toLocaleString();
            }
            if (runStatsData.heartbeatTime) {
                var myDate = new Date(runStatsData.heartbeatTime);
                heartbeatTime = myDate.toLocaleString();
            }

            var data = {
                'statusText': runStatsData.statusText,
                'startTime': startTime,
                'endTime': endTime,
                'heartbeatTime': heartbeatTime
            };

            $('.script-status-info .detail-view-object-body').empty();
            $('.script-status-info .detail-view-object-body').html(_.template(Templates.scriptStatusTmpl, data, {variable:'data'}));
            // If script map-reduce jobs is displayed, update.
            if ($('.script-mr-jobs').length > 0) {
                DetailView.populateScriptInfo();
            }

            // Loop through map-reduce jobs
            _.each(runStatsData.jobStatusMap, function(jobStats, jobTrackerId) {
                var jobId = jobStats.jobId;
                // If map-reduce job's counters are displayed, update.
                if ($('.MRJob-counters.'+jobId).length > 0) {
                    DetailView.populateMRInfo(jobStats.scope);
                }
            });
        });
    },
    /**
     * Initialize the DetailView object.
     */
    initialize: function() {
        DetailView.startListeners();
        DetailView.addObject({groupName: 'script-status freeze', objectName: 'script-status-info', title: 'Script Status:', html: ''});
        DetailView.populateScriptInfo();
    },
    /**
     * Add an object to a detailView.
     * Example: addObject(options)
     * options: {
     *     groupName: '',
     *     objectName: '',
     *     title: '',
     *     html: '',
     * }
     *
     * options.groupName = name of group the object belongs to (adding 'freeze' will keep the contents in the detailView when clearing, example: 'myGroup freeze')
     * options.objectName = unique name for object
     * options.title = title for object (optional)
     * options.html = html markup for object
     *
     * @param {Object} options The options for the object
     */
    addObject: function(options) {
        var opts = $.extend({}, DetailView.options.objDefaults, options);
        $(DetailView.options.containerSel).append(_.template(Templates.detailViewObjTmpl, opts, {variable:'data'}));
    },
    /**
     * Clear Detail View contents.
     */
    clearContents: function() {
        $(DetailView.options.containerSel + ' .detail-view-object-container:not(.freeze)').remove();
    },
    /**
     * Populate map-reduce job's information.
     *
     * @param {String} scopeId The map-reduce job's scopeId
     */
    populateMRInfo: function(scopeId) {
        DetailView.clearContents();
        var mrInfo = GraphModel.getMRInfo(scopeId);

        // Add MRJob-aliases object to MRJob-info group.
        DetailView.addObject({groupName: 'MRJob-info', objectName: 'MRJob-aliases', title: 'Aliases:',
            html: _.template(Templates.aliasesTmpl, _.unique(mrInfo.aliases), {variable: 'data'})});

        // If map-reduce job has run stats data, display.
        if (mrInfo.mrJobInfo) {
            var mrJobInfo = mrInfo.mrJobInfo;
            var jobId = mrJobInfo.jobId;
            var trackingUrl = mrJobInfo.trackingUrl;
            var progressData = {
                'jobId' : jobId,
                'progressInfo': mrJobInfo.progressInfo
            };
            var counterHtml = _.template(Templates.counterTmpl, mrJobInfo.counters, {variable: 'data'});
            var trackingHtml = trackingUrl 
                                ? '<a href="'+trackingUrl+'" target="_blank"><h3>'+jobId+'</h3></a>' 
                                : '<h3>'+jobId+'</h3>';

            // Add MRJob-jobId object to MRJob-info group in DetailView.
              DetailView.addObject({groupName: 'MRJob-info', objectName: 'MRJob-jobId', title: 'Job ID:',
                  html: trackingHtml});
            // Add MRJob-progress object to MRJob-info group in DetailView.
            DetailView.addObject({groupName: 'MRJob-info', objectName: 'MRJob-progress', title: 'Progress:',
                html: _.template(Templates.progressTmpl, progressData, {variable: 'data'})});
            // Add MRJob-counters object to MRJob-info group in DetailView.
            DetailView.addObject({groupName: 'MRJob-info', objectName: 'MRJob-counters '+jobId, title: '',
                html: counterHtml});
        }
    },
    /**
     * Populate node's information.
     *
     * @param {String} id The node's id
     */
    populateLOInfo: function(id) {
        DetailView.clearContents();
        // Get data about node.
        var pigData = GraphModel.getPigData();
        var aliasInfo = pigData[id];
        var alias = aliasInfo['alias'];
        var operator = aliasInfo['operator'].replace("LO","");
        if (aliasInfo.hasOwnProperty('join')) {
            var joinType = aliasInfo['join']['type'];
            var joinStrategy = aliasInfo['join']['strategy'];
            operator += ' ('+joinType.toLowerCase()+', '+joinStrategy.toLowerCase()+')';
        }
        // Add LO-alias object to LO-info group.
        DetailView.addObject({groupName: 'LO-info', objectName: 'LO-alias', title: 'Alias: '+alias,
            html: _.template(Templates.nodeMenuTmpl, id, {variable:'data'})});
        // Add LO-operator object to LO-info group.
        DetailView.addObject({groupName: 'LO-info', objectName: 'LO-operator', title: 'Operator: '+operator,
            html: ''});
        // Add LO-schema object to LO-info group.
        if (aliasInfo.hasOwnProperty('schema') && aliasInfo.schema) {
            DetailView.addObject({groupName: 'LO-info', objectName: 'LO-schema', title: 'Schema:',
                html: _.template(Templates.schemaTmpl, aliasInfo['schema'], {variable:'data'})});
        }
        $('button').tooltip();
    },
    /**
     * Populate the overall script information.
     */
    populateScriptInfo: function() {
        DetailView.clearContents();
        // Get all run stats data for all map-reduce jobs in the graph.
        var data = GraphModel.getScriptInfo();
        // Add script-mr-jobs object to script-mr-jobs group.
        DetailView.addObject({groupName: 'script-mr-jobs', objectName: 'script-mr-jobs', title: '',
            html: _.template(Templates.mrJobsTmpl, data, {variable:'data'})});
    }
};

