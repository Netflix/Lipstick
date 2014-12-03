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
    var progressTmpl = '<div class="progress" style="width: 100%;">'+
        '<div role="progressbar" class="job-progress progress-bar <%switch (job.status_text) {' +
        'case "finished": %>progress-bar-success<% break;' + 
        'case "failed": %>progress-bar-danger<% break;' +
        'case "terminated": %>progress-bar-warning<% break;' +
        'default: %>progress-bar-striped active<% break;' + 
        '}%>" style="width:<%=job.progress%>%;" aria-valuenow="<%=job.progress%>"'+
        ' aria-valuemin="0" aria-valuemax="100"' + 
        ' id="progress-<%=job.id%>"><%=job.progress%>%</div></div>';
    var tmpl = _.template(progressTmpl); 
    var WorkflowHistory = {
        format: function(jobs) {
            return _.map(jobs, function(job) {
                return {
                    user: job.user,
                    name: '<a href="workflow.html#graph/'+job.id+'">' + job.name+ '</a>',
                    created_at: new Date(job.created_at).toLocaleString(),
                    updated_at: new Date(job.updated_at).toLocaleString(),
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
                url: './v1/job'                
            }).done(function(json) {
                callback({
                    items: WorkflowHistory.format(json.jobs),
                    count: json.jobsTotal,
                    start: offset + 1,
                    end: offset + json.jobs.length,
                    pages: Math.ceil(json.jobsTotal/options.pageSize),
                    page: options.pageIndex,
                    columns: WorkflowHistory.columns
                });
            }).fail(function() {
                Tossboss.error("failed to history");
            });
        },
        columns: [
            {
                property: 'user',
                label: 'User',
                sortable: true
            },
            {
                property: 'name',
                label: 'Name',
                sortable: true
            },
            {
                property: 'created_at',
                label: 'Created At',
                sortable: true
            },
            {
                property: 'updated_at',
                label: 'Updated At',
                sortable: true
            },
            {
                property: 'progress',
                label: 'Progress',
                sortable: true
            }
        ],
        
        data: function(options, callback) {
            WorkflowHistory.getHistory(options, callback);
        },

    };

    return WorkflowHistory;
});
