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
;define(['jquery', 'underscore'], function($, _) {
    var progressTmpl = '<div class="job-progress progress <%switch (job.status.name) {' +
        'case "finished": %>progress-success<% break;' + 
        'case "failed": %>progress-danger<% break;' +
        'case "terminated": %>progress-warning<% break;' +
        'default: %>progress-striped active<% break;' + 
        '}%>"><div class="bar" style="width:<%=job.progress%>%"' + 
        ' id="progress-<%=job.uuid%>"><%=job.progress%>%</div></div>';
    var tmpl = _.template(progressTmpl); 
    var History = {
        format: function(jobs) {
            return _.map(jobs, function(job) {
                return {
                    user: job.userName,
                    jobName: '<a href="graph.html#graph/'+job.uuid+'">' + job.jobName+ '</a>',
                    startTime: new Date(job.startTime).toLocaleString(),
                    heartbeatTime: new Date(job.heartbeatTime).toLocaleString(),
                    progress: tmpl({job: job})
                }
            });
        },
        getHistory: function(options, callback) {
            var offset = (options.pageIndex)*options.pageSize;
            var data = {
                progress: 1,
                offset: offset,
                sort: options.sortProperty,
                order: options.sortDirection, 
                max: options.pageSize,
                search: options.search,
            };
            if (options.filter) {
                data['status'] = options.filter.value;
            }
            $.ajax({
                type: 'GET',
                data: data,
                url: './job'                
            }).done(function(json) {
                callback({
                    data: History.format(json.jobs),
                    start: offset + 1,
                    end: offset + json.jobs.length,
                    count: json.jobsTotal,
                    pages: Math.ceil(json.jobsTotal/options.pageSize),
                    page: options.pageIndex + 1
                });
            }).fail(function() {
                Tossboss.error("failed to history");
            });
        },
        columns: function() {
            return [ 
                {
                    property: 'user',
                    label: 'User',
                    sortable: true
                },
                {
                    property: 'jobName',
                    label: 'Job',
                    sortable: true
                },
                {
                    property: 'startTime',
                    label: 'Start Time',
                    sortable: true
                },
                {
                    property: 'heartbeatTime',
                    label: 'Heartbeat Time',
                    sortable: true
                },
                {
                    property: 'progress',
                    label: 'Progress',
                    sortable: false
                }
            ]
        },
        data: function(options, callback) {
            History.getHistory(options, callback);
        },

    };

    return History;
});
