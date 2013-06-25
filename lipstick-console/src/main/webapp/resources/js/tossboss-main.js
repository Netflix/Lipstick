var refreshCodeMirror = true;

function findParameters() {
    var codeParams = new Array();
    var formParams = new Array();
    // get all parameters in CodeMirror
    _.each(myCodeMirror.getValue().match(/\$\w+/ig), function(v) {
        v = v.replace(/\$/g,'');
        if (isNaN(v)) { codeParams.push(v.replace(/\$/g,'')); }
    });
    codeParams = _.unique(codeParams);
    // delete stale parameters from parameters form
    _.each($('div#parameters div'), function(v) {
        id = $(v).attr('id');
        if (_.indexOf(codeParams,id)==-1) {
            $('div#parameters form #'+id).remove();
        }
        else {
            formParams.push(id);
        }
    });
    // find only new parameters and add to parameters form
    codeParams = _.difference(codeParams, formParams);
    _.each(codeParams, function(v,k) {
        var paramForm = '\
            <div id="'+v+'" class="control-group">\
            <label class="control-label" for="'+v+'">'+v+'</label>\
            <div class="controls"><input class="param span10" id="'+v+'" type="text"></div>\
            </div>'
        $('div#parameters form').append(paramForm);
    });
}

var refreshParameters = _.debounce(findParameters,2000);
var myCodeMirror = CodeMirror($('#code-editor').get(0), {
    lineNumbers: true,
    matchBrackets: true,
    indentUnit: 4,
    smartIndent: false,
    onChange: refreshParameters
});

function removeHighlighting() {
    lines = myCodeMirror.lineCount();
    for(i=0; i<=lines; i++) {
        myCodeMirror.setLineClass(i);
    }
}

function highlightLine(lineNumber, lineNumbers) {
    removeHighlighting();
    if (lineNumber > 0) {
        var nextLineNumber = lineNumbers[_.indexOf(lineNumbers,lineNumber,true)+1] - 1;
        if (isNaN(nextLineNumber)) { nextLineNumber = myCodeMirror.lineCount() - 1; }
        --lineNumber;
        --nextLineNumber;
        var doHighlight = false;
        while(lineNumber <= nextLineNumber) {
            if (doHighlight ||
                    (myCodeMirror.getLine(nextLineNumber).trim().length > 0 &&
                     myCodeMirror.getLine(nextLineNumber).trim().substring(0,2) != '--'))
            {
                myCodeMirror.setLineClass(nextLineNumber,'','highlight');
                doHighlight = true;
            }
            --nextLineNumber;
        }
        myCodeMirror.scrollTo(0,(lineNumber-10)*($(".CodeMirror-lines pre").height())+7);
    }
}

function getParamString() {
    var paramString = "";
    _.each($('input.param'), function(v, k) {
        param = $(v).attr('id');
        value = $(v).val();
        paramString += "&" + param + "=" + value;
    });
    return paramString.substring(1);
}

function repopulateMain(script, params) {
    if (refreshCodeMirror) {
        myCodeMirror.setValue(script);
        findParameters();
        _.each(params, function(p_value, p_name) {
            $('div#parameters #'+p_name).val(p_value);
        });
    }
    refreshCodeMirror = true;
}

function redrawIndex() {
    $('div#history').hide();
    $('div#index').show();
    removeHighlighting();
}

function getHistory() {
    $.ajax({
        'type': 'GET',
        'url': '/pig2json/p2j/',
        'success': function(data) {
            $('div#index').hide();
            $('div#history').show();
            _.each(data['requests'], function(v, uuid) {
                $('div#past-scripts').empty();
                populateHistory(uuid);
            });
        }
    });
}

function populateHistory(uuid) {
    $.ajax({
        'type': 'GET',
        'url': '/pig2json/p2j/'+uuid,
        'success': function(data) {
            paramString = '';
            script = data['request']['script'];
            params = data['request']['parameters'];
            result = data['status'];
            _.each(params, function(p_value, p_key) {
                paramString += "$" + p_key + ": " + p_value + "; "
            });
            paramString = (paramString=='') ? ''  : '<code>' + paramString + '</code>';
            html = '<div class="accordion-group"><div class="accordion-heading">\
                      <a class="accordion-toggle" data-toggle="collapse" data-parent="#past-scripts" href="#'+uuid+'">\
                        '+uuid+' ('+result+')</a></div>\
                    <div id="'+uuid+'" class="accordion-body collapse">\
                      <div class="accordion-inner">\
                        <button class="btn btn-mini btn-primary">Show graph</button><br/><br/>\
                        '+paramString+'\
                        <pre>'+script+'</pre></div></div></div>'
            $('div#past-scripts').append(html);
            $('div#past-scripts #'+uuid+' button').click(function() {
                redrawIndex();
                window.location = window.location.origin + window.location.pathname + '#script/' + uuid;
            });
        }
    });
}

function getUuid() {
    window.refreshCodeMirror = false;
    var pigScript = myCodeMirror.getValue();
    var paramString = getParamString();
    $('button#draw-graph').button('loading');
    $.ajaxSetup({'timeout': 90000});
    $.ajax({
        'type': 'POST',
        'url':  '/pig2json/p2j?'+paramString,
        'data': pigScript,
        'dataType': 'json',
        'contentType': 'application/json',
        'timeout': 90000,
        'success': function(data) {
            window.uuid = data['uuid'];
            window.location = window.location.origin + window.location.pathname + '#script/' + window.uuid;
        },
        // Define the error method.
        error: function( objAJAXRequest, strError ){
        }
    });
}

function openWindow(uuid) {
    window.uuid = uuid
    redrawIndex();
    window.w = window.open('graph.html','graph','width='+window.windowWidth+',height='+screen.height+',toolbar=0,menubar=0,titlebar=0,location=0,status=0,scrollbars=1,resizable=1,left='+window.windowWidth+',top=0');
}

$(function () {
    window.windowWidth = (screen.width/3)*2;
    if (window.windowWidth < 1200) window.windowWidth = 1200;
    $('#code-menu button').tooltip();
    $('button#draw-graph').click(getUuid);
    $('button#remove-highlights').click(function() { removeHighlighting(window); });
    var TestRouter = Backbone.Router.extend({
        routes: {
            "" : "redrawIndex",
            "script/:uuid": "openWindow",
            "history": "getHistory"
        },
        redrawIndex: redrawIndex,
        openWindow: openWindow,
        getHistory: getHistory
    }); 
    router = new TestRouter();
    Backbone.history.start();
});
