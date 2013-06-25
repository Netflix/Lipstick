var allData;
var pigData;
var svgData;
var zoomLevel;

function checkStatus(uuid) {
    $.ajax({
        'type': 'GET',
        'url':  '/pig2json/p2j/'+uuid,
        'success': function(data) {
            if (data['status'] == 'COMPLETE') {
                allData = data;
                $('#status').hide();
                populateGraph('unoptimized');
                createClicks();
                populateAliases();
                populateMoreInfo();
                $('#nav-links li').removeClass('disabled');
                window.location = window.location.origin + window.location.pathname + '#graph';
                $('a[href="#graph"]').parent().addClass('active');
                window.opener.$('button#draw-graph').button('reset');
                window.opener.repopulateMain(data['request']['script'], data['request']['parameters']);
            }
            else if (data['status'] == 'ERROR') {
                plotError(data['error'], 'error.gif');
                window.opener.$('button#draw-graph').button('reset');
                window.opener.repopulateMain(data['request']['script'], data['request']['parameters']);
            }
            else {
                setTimeout(function() { checkStatus(uuid); }, 1000);
            }
        },
        'error': function() {
            plotError('Graph not found.', '404.gif');
        }
    }); 
}

function drawPage(page) {
    closeMenu();
    $('#nav-links li').removeClass('active');
    $('a[href="#'+page+'"]').parent().addClass('active');
    $('div.page').hide();
    $('div.page#'+page).show();
    $('#graph-menu').width($('#graph-type button').width() * 2.45);
}

function populateGraph(type) {
    pigData = allData['results'][type]['plan'];
    svgData = allData['results'][type]['svg'];
    $('#pig-graph').empty();
    $('#pig-graph').html(svgData);
    scrollTo(0,0);
}

function populateAliases() {
    $('#alias-info').empty();
    _.each(allData['results']['unoptimized']['plan'], function(aliasInfo, uid) {
        var lineNumber = aliasInfo['location']['line'];
        var aliasName = aliasInfo['alias'];
        var schema = "";
        _.each(aliasInfo['schema'], function(columnInfo) {
            schema += "<tr><td>" + columnInfo['alias'] + "</td><td>" + columnInfo['type'] + "</td></tr>"
        });
        html = '<div class="accordion-group"><div class="accordion-heading">\
                  <a class="accordion-toggle" data-toggle="collapse" data-parent="#alias-info" href="#alias'+uid+'">\
                  '+aliasName+'</a></div>\
                <div id="alias'+uid+'" class="accordion-body collapse">\
                  <div class="accordion-inner">\
                  <table class="table table-bordered table-condensed"><thead><tr><th>Column</th><th>Type</th></tr></thead>\
                  <tbody>'+schema+'</tbody></table></div></div></div>'
        $('#alias-info').append(html);
        $('a[href="#alias'+uid+'"]').data('line_number', lineNumber);
    });
    $('a.accordion-toggle').bindClicks(highlightLine, highlightLine);
}

function populateMoreInfo() {
    var numJobs = allData['results']['optimized']['svg'].match(/class="cluster"/g).length;
    $('#source-info ul').empty();
    $('#target-info ul').empty();
    $('#number-mr-jobs a').html(numJobs);
    for(i=1; i<= numJobs; i++) {
        var lineNumbers = '';
        $('.modal-body').append('<div id="mr_job_'+i+'"><a href="#moreinfo"><strong>Job '+i+'</strong></a><ul></ul></div>');
        _.each(allData['results']['optimized']['plan'], function(aliasInfo) {
            if (aliasInfo['mapReduce'] != null && aliasInfo['mapReduce']['jobId'] == i && aliasInfo['alias'] != null) {
                $('#mr_job_'+i+' ul').append('<li>'+aliasInfo["alias"]+'</li>');
                lineNumbers += ','+aliasInfo['location']['line'];
            }
        });
        $('#mr_job_'+i).data('line_number', lineNumbers.substring(1));
    }
    _.each(allData['results']['unoptimized']['plan'], function(aliasInfo, uid) {
        if (aliasInfo['storageLocation'] != undefined) {
            html = '<li id="alias'+uid+'"><a href="#moreinfo">'+aliasInfo['storageLocation']+'</a></li>'
            if (aliasInfo['operator'] == 'LOLoad') {
                $('#source-info ul').append(html);
                $('li#alias'+uid).data('line_number',aliasInfo['location']['line']);
            }
            else if (aliasInfo['operator'] == 'LOStore') {
                $('#target-info ul').append(html);
                $('li#alias'+uid).data('line_number',aliasInfo['location']['line']);
            }
        }
    });
    $('div#moreinfo li').bindClicks(highlightLine, highlightLine);
}

function populateBreakpointCode(script) {
    closeMenu();
    $('pre#breakpoint-code').show();
    $('pre#breakpoint-code').html(script);
}

function plotError(msg, image) {
    $('#status-msg').addClass('alert-error');
    $('#status-msg').html('<h3>ERROR:</h3>'+msg);
    $('#status-img').attr('src','resources/img/'+image);
}

function createClicks() {
    window.lineNumbers = new Array();
    // set starting zoom level
    svgHeightPx = Math.round($('svg').attr('height').replace('pt','') * 1.333333333);
    zoomLevel = (Math.round((screen.height / svgHeightPx) * 10) / 10) + 0.1;
    zoomLevel = (zoomLevel > 1.0) ? 1.1 : zoomLevel;
    graphZoom({'currentTarget': { 'id': 'zoom-out'}});
    // add data to each node
    _.each(pigData, function(aliasInfo, uid) {
        var lineNumber = aliasInfo['location']['line'];
        $('g #'+uid).data('line_number',lineNumber);
        window.lineNumbers.push(lineNumber);
    });
    window.lineNumbers.sort(function(a,b){return a-b})
    window.lineNumbers = _.unique(window.lineNumbers, true);
    // add data to each edge
    _.each($('g .edge title'), function(element) {
        var start = $(element).text().split("->")[0];
        var end   = $(element).text().split("->")[1];
        $(element).parent().attr('data-start',start);
        $(element).parent().attr('data-end',end);
    });
    // mouse events for node menu
    $('div#node-menu').hover(function(e) { clearInterval(window.menuCloseInterval); },
                             function(e) { window.menuCloseInterval = _.delay(closeMenu, 1000, e); });
    $('div#node-menu button#node-menu-path').click(highlightPath);
    $('div#node-menu button#node-menu-breakpoint').click(setBreakpoint);
    $('div#node-menu button#node-menu-reset-graph').click(highlightAllPaths);
    // mouse events for node
    $('.node').hover(function(e) { clearInterval(window.menuCloseInterval);
                                   window.menuDisplayInterval = _.delay(displayMenu, 1000, e); },
                     function(e) { clearInterval(window.menuDisplayInterval);
                                   window.menuCloseInterval = _.delay(closeMenu, 1000, e); });
    $('.node').bindClicks(highlightLine, highlightLine);
}

function graphZoom(e) {
    var mode = e.currentTarget.id;
    if (mode == 'zoom-in') {
        zoomLevel += 0.1;
    }
    else if (mode == 'zoom-out') {
        if (Math.round(zoomLevel * 100)/100 > 0.1) { zoomLevel -= 0.1; }
        else { zoomLevel = 0.1; }
    }
    else {
        zoomLevel = 1.0;
    }
    zoomLevel = Math.round(zoomLevel * 100) / 100;
    $('#pig-graph').transition({ scale: zoomLevel }, 0);
}

function highlightLine() {
    var lineNumber = $(this).data('line_number');
    window.opener.highlightLine(lineNumber, window.lineNumbers);
}

function highlightPath() {
    window.finalUids = new Array();
    window.finalEdges = new Array();
    var uid = $('div#node-menu').data('uid');
    $('g.node').attr('opacity','0.3');
    $('g.edge').attr('opacity','0.3');
    window.finalUids.push(String(uid));
    getPredecessors([String(uid)]);
    _.each(window.finalUids.concat(window.finalEdges), function(id) {
        $('g #'+id).attr('opacity','1');
    });
}

function getPredecessors(uids) {
    var parentUids = new Array();
    _.each(uids, function(uid) {
        _.each($('.edge[data-end="'+uid+'"]'), function(element) { window.finalEdges.push($(element).attr('id')); });
        window.finalUids = window.finalUids.concat(pigData[uid]['predecessors']);
        parentUids = parentUids.concat(pigData[uid]['predecessors']);
    }); 
    if (parentUids.length > 0) {
        getPredecessors(parentUids);
    }   
    return
}

function highlightAllPaths() {
    $('g.node').attr('opacity','1');
    $('g.edge').attr('opacity','1');
    $('pre#breakpoint-code').hide();
}

function displayMenu(e) {
    var uid = $(e.target).parent().attr('id');
    var pos = $('g #'+uid).position();
    $('div#node-menu').css({top:  (pos.top - 27.0) + 'px',
                            left: (pos.left) + 'px'}).show();
    $('div#node-menu').data('uid',uid);
}

function closeMenu(e) {
    $('div#node-menu').hide();
}

function setBreakpoint(e) {
    keys = new Array();
    var codeLines = new Array();
    var newScript = '';
    var uid = $('div#node-menu').data('uid');
    var alias = pigData[uid]['alias'];
    var wholeScript = window.opener.myCodeMirror.getValue().split('\n');
    var limitCmd = "LIMIT "+alias+" 100;";
    var storeCmd = "STORE "+alias+" INTO '';";
    var dumpCmd  = "DUMP "+alias+";";
    highlightPath();
    // get setup commands
    _.each(wholeScript, function(line, lineNumber) {
        if (line.trim().toUpperCase().indexOf('SET')==0 ||
            line.trim().toUpperCase().indexOf('REGISTER')==0 ||
            line.trim().toUpperCase().indexOf('DEFINE')==0) {
            newScript += line + '\n';
        }
    });
    // get line numbers and grab necessary code
    _.each(window.finalUids, function(uid) { codeLines.push($('g #'+uid).data('line_number')); });
    codeLines.sort(function(a,b){return a-b});
    codeLines = _.uniq(codeLines);
    _.each(codeLines, function(lineNumber) {
        startLine = lineNumber-1;
        endLine   = window.lineNumbers[_.indexOf(window.lineNumbers, lineNumber,true)+1];
        if (endLine == undefined) { endLine = window.opener.myCodeMirror.lineCount()-1; }
        else { endLine -= 2; }
        newScript += '\n' + window.opener.myCodeMirror.getRange({'ch':0,'line':startLine}, {'ch':999999999,'line':endLine});
    });
    // substitute parameters
    _.each(allData['request']['parameters'], function(p_val, p_name) { keys.push(p_name); });
    keys.sort();
    keys.reverse();
    _.each(keys, function(p_name) {
        var p_value = allData['request']['parameters'][p_name];
        var regex = new RegExp("\\$"+p_name+"+","ig");
        newScript = newScript.replace(regex,p_value);
    });
    newScript += '\n'+limitCmd+'\n'+dumpCmd;
    // populate breakpoint page and switch to page
    populateBreakpointCode(newScript);
    window.location = window.location.origin + window.location.pathname + '#breakpointcode';
}

function resetWindow() {
    window.resizeTo(window.opener.windowWidth, screen.height);
    window.moveTo(window.opener.windowWidth, 0);
}

$(function () {
    checkStatus(window.opener.uuid);
    $(document).on('click', '.disabled', function() { return false; });
    $(document).on('click', '#zoom-menu button', graphZoom);
    $(document).on('click', '#nav-links li:not(.disabled)', function(e) {
        $('#nav-links li').removeClass('active');
        $(e.target).parent().addClass('active');
    });
    $(document).on('click', '#graph-type button', function(e) {
        if ($(this).context.id == 'optimized-graph') {
            populateGraph('optimized');
        }
        else {
            populateGraph('unoptimized');
        }
        createClicks();
        window.opener.removeHighlighting();
        closeMenu();
    });
    $('button').tooltip();
    $('button#reset-window').click(resetWindow);
    //$(':radio:eq(0)').click(function(e) {
    //    populateGraph('unoptimized');
    //    createClicks();
    //    window.opener.removeHighlighting();
    //    closeMenu();
    //}); 
    //$(':radio:eq(1)').click(function(e) {
    //    populateGraph('optimized');
    //    createClicks();
    //    window.opener.removeHighlighting();
    //    closeMenu();
    //});
    var TestRouter = Backbone.Router.extend({
        routes: {
            ":page": "drawPage"
        },
        drawPage: drawPage
    });
    router = new TestRouter();
    Backbone.history.start();
});

;(function($) {
    var defaults = { 
        timeout: 300 
    };  
    $.fn.bindClicks = function(clickFunc, dblClickFunc, options) {
        options = $.extend({}, defaults, options);
        $.each($(this), function() { 
            var _this = this;
            var debounce = _.debounce(function(e) { if (e.type == "click") {clickFunc.apply(_this);} else {dblClickFunc.apply(_this);} }, options.timeout);
            $(this).on('click', debounce);
            $(this).on('dblclick', debounce);
        }); 
    };  
})(jQuery);
