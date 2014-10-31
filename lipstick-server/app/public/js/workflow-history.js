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
    var WorkflowHistory = {
        format: function(jobs) {
            return _.map(jobs, function(job) {
                return {
                    name: '<a href="workflow.html#graph/'+job.id+'">' + job.name+ '</a>',
                    created_at: new Date(job.created_at).toLocaleString(),
                    updated_at: new Date(job.updated_at).toLocaleString()
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
                    page: options.pageIndex + 1,
                    columns: WorkflowHistory.columns
                });
            }).fail(function() {
                Tossboss.error("failed to history");
            });
        },
        columns: [ 
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
            }
        ],
        
        data: function(options, callback) {
            WorkflowHistory.getHistory(options, callback);
        },

    };

    return WorkflowHistory;
});
