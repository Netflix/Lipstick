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
/** tossboss-templates.js
 */

;Templates = {

modalTmpl: ' \
<div id="myModal" class="modal hide fade" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true"> \
    <div class="modal-header"><button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button><h3><%= data.title %></h3></div> \
    <div class="modal-body"><%= data.html %></div> \
</div> \
',

codeMirror: ' \
<div id="script"> \
    <div class="row"> \
        <div class="navbar-form"> \
            <button class="btn btn-small pull-right" id="remove-highlights"> \
                <i class="icon icon-remove-circle"></i> Remove highlighting \
            </button> \
        </div> \
        <div class="controls well" id="code-editor"> </div> \
    </div> \
</div> \
',

breakpointCodeTmpl: ' \
<pre class="pre-scrollable"><%= data %></pre> \
',

nodeMenuTmpl: ' \
<div class="btn-group" id="node-menu" data-uid="<%= data %>"> \
    <button class="btn btn-mini" rel="tooltip" data-placement="bottom" id="node-menu-path" title="Show path to node."> \
        <i class="icon icon-road"></i> \
    </button> \
    <button class="btn btn-mini" rel="tooltip" data-placement="bottom" id="node-menu-breakpoint" title="Set breakpoint."> \
        <i class="icon icon-stop"></i> \
    </button> \
    <button class="btn btn-mini" rel="tooltip" data-placement="bottom" id="node-menu-reset-graph" title="Reset graph."> \
        <i class="icon icon-refresh"></i> \
    </button> \
</div> \
',

scriptStatusTmpl: ' \
<table class="table table-condensed"> \
    <% if (data.statusText) { %> \
        <tr><td>Status:</td><td><%= data.statusText %></td></tr> \
    <% } %> \
    <% if (data.startTime) { %> \
        <tr><td>Start:</td><td><%= data.startTime %></td></tr> \
    <% } %> \
    <% if (data.endTime) { %> \
        <tr><td>End:</td><td><%= data.endTime %></td></tr> \
    <% } %> \
    <% if (data.heartbeatTime) { %> \
        <tr><td>Heartbeat:</td><td><%= data.heartbeatTime %></td></tr> \
    <% } %> \
</table> \
',

progressTmpl: ' \
<table class="table table-condensed <%= data.jobId %>"> \
<% for (var i=0; i<data.progressInfo.length; i++) { %> \
    <tr><td><%= data.progressInfo[i].type %> (<%= data.progressInfo[i].number_tasks %>)</td> \
        <td><div class="progress"> \
            <div class="bar <%= data.progressInfo[i].type.toLowerCase() %>" style="width: <%= data.progressInfo[i].progress %>%;"> \
            <%= data.progressInfo[i].progress %>%</div></div></td></tr> \
<% } %></table> \
',

counterTmpl: ' \
<% _.each(data, function(counterObj, counterGroup) { %> \
    <h3><%= counterGroup %>:</h3> \
    <table class="table table-bordered table-condensed"> \
    <% _.each(counterObj.counters, function(counterValue, counterName) { %> \
        <tr> \
            <td><%= counterName %></td> \
            <td><%= GraphView.addCommas(counterValue) %></td> \
        </tr> \
    <% }); %> \
    </table> \
<% }); %> \
',

schemaTmpl: ' \
<table class="table table-bordered table-condensed"> \
<% for (var i = 0; i < data.length; i++) { %> \
    <tr><td><%= data[i].alias %></td><td><%= data[i].type %></td></tr> \
<% } %></table> \
',

detailViewObjTmpl: ' \
<div class="detail-view-object-container <%= data.groupName %> <%= data.objectName %>"> \
    <h3 class="detail-view-object-title"><%= data.title %></h3> \
    <div class="detail-view-object-body"> \
        <%= data.html %> \
    </div> \
</div> \
',

aliasesTmpl: ' \
<table class="table table-condensed"> \
<% _.each(data, function(aliasName, index) { %> \
    <tr><td><%= aliasName %></td></td> \
<% }); %> \
</table> \
',

sampleOutputDataTmpl: ' \
<% if ((! $.isEmptyObject(data)) && data.sampleOutputData.sampleOutputList.length > 0) { %> \
    <% _.each(data.sampleOutputData.sampleOutputList, function(sampleOutputObj, index) { %> \
        <% if (sampleOutputObj.sampleOutput.length > 0) { %> \
            <table class="table table-condensed table-bordered table-hover"> \
                <% if (data.sampleOutputData.sampleOutputList.length == 1) { %> \
                    <thead><tr style="background-color: #fcf8e3;"> \
                    <% _.each(data.schema, function(colInfo, index) { %> \
                        <td><strong><%= colInfo.alias %></strong></td> \
                    <% }); %> \
                    </tr></thead> \
                <% } else { %> \
                    <thead><tr style="background-color: #fcf8e3;"><td colspan="200">&nbsp;</td></tr></thead> \
                <% } %> \
                <tbody> \
                <% _.each(sampleOutputObj.sampleOutput.split("\\n"), function(record, index) { %> \
                    <tr> \
                    <% _.each(record.split("\u0001"), function(value) { %> \
                       <td><%= value %></td> \
                    <% }); %> \
                    </tr> \
                <% }); %> \
                </tbody> \
            </table> \
        <% } else { %> \
            <table class="table table-condensed table-bordered table-hover"><tr><td>No sample output available at this time.</td></tr></table> \
        <% } %> \
    <% }); %> \
<% } else { %> \
    <table class="table table-condensed table-bordered table-hover"><tr><td>No sample output available at this time.</td></tr></table> \
<% } %> \
',

mrJobsTmpl: ' \
<table class="table table-condensed"> \
    <thead> \
        <tr> \
            <td><strong>Jobs</strong></td> \
            <td><strong>Map</strong></td> \
            <td><strong>Reduce</strong></td> \
            <td><strong>Duration</strong></td> \
        </tr> \
    </thead> \
    <tbody> \
    <% _.each(data, function(mrJobInfo, index) { %> \
        <tr> \
            <% if (mrJobInfo.runData.trackingUrl) { %> \
                <td><a href="<%= mrJobInfo.runData.trackingUrl %>" target="_blank"><%= mrJobInfo.runData.jobId %></a></td> \
            <% } else if (mrJobInfo.runData.jobId) { %> \
                <td><%= mrJobInfo.runData.jobId %></td> \
            <% } else { %> \
                <td><%= mrJobInfo.scopeId %></td> \
            <% } %> \
            <% if (mrJobInfo.runData.totalMappers > 0) { %> \
                <td><%= Math.floor(mrJobInfo.runData.mapProgress * 100) %>%</td> \
            <% } else { %> \
                <td></td> \
            <% } %> \
            <% if (mrJobInfo.runData.totalReducers > 0) { %> \
                <td><%= Math.floor(mrJobInfo.runData.reduceProgress * 100) %>%</td> \
            <% } else { %> \
                <td></td> \
            <% } %> \
            <% if ((mrJobInfo.runData.startTime) && (mrJobInfo.runData.finishTime)) { %> \
              <% var duration = moment().startOf("day").add("ms", (mrJobInfo.runData.finishTime - mrJobInfo.runData.startTime)) %> \
              <td><%= duration.format("HH:mm:ss") %></td> \
            <% } else { %> \
              <td>--</td> \
            <% } %> \
        </tr> \
    <% }); %> \
    </tbody> \
</table> \
',

    jobWarningModalTmpl: ' \
<div id="warning-modal-<%= data.scope_id %>" class="modal hide fade" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true"> \
    <div class="modal-header"><button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button><h3> \
      Map/Reduce Job Warnings for <%= data.job_id %> \
    </h3></div> \
    <div id="warning-modal-body-<%= data.scope_id %>" class="modal-body"> \
    </div> \
</div> \
',
    jobWarningModalBodyTmpl: ' \
      <ul> \
      <% for (var i=0; i < data.job_warnings.length; i++) { %> \
        <li><%= data.job_warnings[i] %></li> \
      <% } %> \
      </ul> \
',

};
