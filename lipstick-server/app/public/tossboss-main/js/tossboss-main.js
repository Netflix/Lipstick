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
/** tossboss-main.js
 * Responsible for the initialization of objects and the page.
 * 
 * LISTENS FOR EVENTS:
 * 
 * TIGGERS EVENTS:
 */

var uuid;
function getJobInfo(uuid) {
    window.uuid = uuid;
    Script.initialize();
    GraphView.initialize();
    GraphModel.initialize(uuid);
    DetailView.initialize();
}

$(function () {
    Modal.initialize();
    Drawer.initialize(0,1,1,0);
    Drawer.initDisplay(0,1,0,0);
    var TestRouter = Backbone.Router.extend({
        routes: {
            "job/:uuid": "getJobInfo",
            ":page/:uuid": "drawPageWithInfo"
        },
        getJobInfo: function(uuid) {
            getJobInfo(uuid);
            window.location.hash = '#graph/'+uuid;
        },
        drawPageWithInfo: function(page, uuid) {
            if (window.uuid != uuid) {
                getJobInfo(uuid);
            }
            scrollTo(0,0);
            $('div .progress').show();
            $('div.page').hide();
            $('div.page#'+page).show();
            $('#graph-menu').width($('#graph-type button').width() * 2.45);
        }
    }); 
    router = new TestRouter();
    Backbone.history.start();
    // Bind events on graph type buttons (optimized, unoptimized).
    $('.graph-type-menu button').on('click', function(e) {
        Script.removeHighlight();
        Script.options.myCodeMirror.scrollTo(0,0);
        if ($(this).context.id === 'optimized-graph-btn') {
            $('#unoptimized-graph-btn').removeClass('active').removeClass('btn-primary').removeAttr('disabled');
            $('#optimized-graph-btn').addClass('active').addClass('btn-primary').attr('disabled','disabled');
            GraphView.drawGraph('optimized');
        }
        else {
            $('#optimized-graph-btn').removeClass('active').removeClass('btn-primary').removeAttr('disabled');
            $('#unoptimized-graph-btn').addClass('active').addClass('btn-primary').attr('disabled','disabled');
            GraphView.drawGraph('unoptimized');
        }
        GraphView.addDataToGraph();
        GraphModel.getRunStats();
    });
    // Bind events on graph zoom buttons.
    $('button#zoom-in').on('click', function(event) {
        GraphView.zoom('in');
    });
    $('button#zoom-out').on('click', function(event) {
        GraphView.zoom('out');
    });
    $('button#zoom-reset').on('click', function(event) {
        GraphView.zoom('reset');
    });
    // Bind events on logical operator menu buttons.
    $(document).on('click', 'button#node-menu-path', function(event) {
        event.stopPropagation();
        var uid = $('div#node-menu').data('uid');
        GraphView.highlightPath(uid);
    });
    $(document).on('click', 'button#node-menu-breakpoint', function(event) {
        event.stopPropagation();
        var uid = $('div#node-menu').data('uid');
        Script.getBreakpointCode(uid);
    });
    $(document).on('click', 'button#node-menu-reset-graph', function(event) {
        event.stopPropagation();
        GraphView.highlightAll();
    });
    $('button#remove-highlights').on('click', function(event) {
        Script.removeHighlight();
    });
    $('button').tooltip();
});
