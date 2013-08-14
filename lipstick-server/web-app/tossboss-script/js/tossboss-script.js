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
/** tossboss-script.js
 * Responsible for the script.
 *
 * LISTENS FOR EVENTS:
 * - clickLogicalOperator.tossboss-graph-view
 * - clickMRJob.tossboss-graph-view
 * - clickOutsideGraph.tossboss-graph-view
 * - loadGraphModel.tossboss-graph-model
 *
 * TRIGGERS EVENTS:
 * - highlightCodeLine.tossboss-graph-script
 * - loadBreakpointCode.tossboss-graph-script
 */

;Script = {
    options: {
        script: '',
        myCodeMirror: CodeMirror($('#code-editor').get(0), {
            lineNumbers: true,
            matchBrackets: true,
            indentUnit: 4,
            smartIndent: false,
            readOnly: true
        }),
        codeMirrorSel: '#code-editor',
        lineNumbers: []
    },
    /**
     * Start all custom event listeners.
     */
    startListeners: function() {
        // On logical operator click, highlight line in script.
        $(document).on('clickLogicalOperator.tossboss-graph-view', function(event, id) {
            var lineNumber = $('g#'+id).data('line-number');
            Script.highlightLine(lineNumber);
        });
        // On map-reduce job click, remove all line highlighting in script.
        $(document).on('clickMRJob.tossboss-graph-view', function(event, id) {
            Script.removeHighlight();
        });
        // On outside graph click, remove all line highlighting in script.
        $(document).on('clickOutsideGraph.tossboss-graph-view', function(event) {
            Script.removeHighlight();
        });
        // On getting graph data from GraphModel, initialize the Script object.
        $(document).on('loadGraphModel.tossboss-graph-model', function(event) {
            Script.initialize(GraphModel.options.allData.scripts.script);
        });
    },
    /**
     * Initialize the Script object.
     *
     * @param {String} script The Pig script being run
     */
    initialize: function(script) {
        Script.initPage();
        Script.startListeners();
        Script.options.script = script;
        // Populate Code Mirror with script and get script line numbers from Pig data.
        if (script) {
            var pigData = GraphModel.getPigData();
            Script.options.myCodeMirror.setValue(Script.options.script);
            _.each(pigData, function(aliasInfo, uid) {
                if (aliasInfo['location']['macro'].length == 0) {
                    Script.options.lineNumbers.push(aliasInfo['location']['line']);
                }
            });
            Script.options.lineNumbers.sort(function(a,b){return a-b})
            Script.options.lineNumbers = _.unique(Script.options.lineNumbers, true);
        }
    },
    /**
     * Setup elements on the page
     */
    initPage: function() {
        // Add Script section to bottom drawer.
        $('#bottom-drawer .container').append($('div#script'));
        $('div#script').removeClass();
        $('div#script').removeAttr('style');
        $('div#script').show();
        // Remove Script link from navbar.
        $('#nav-links li#script').remove();
    },
    /**
     * Get relevant Pig code to the specified node's id.
     *
     * @param {Number} id The node's id to get Pig code
     */
    getBreakpointCode: function(id) {
        var pigData = GraphModel.getPigData();
        var codeLines = [];
        var newScript = '';
        var alias = pigData[id].alias;
        var wholeScript = Script.options.myCodeMirror.getValue().split('\n');
        var limitCmd = "breakpoint_"+alias+" = LIMIT "+alias+" 100;";
        var dumpCmd  = "DUMP breakpoint_"+alias+";";
        // Get setup commands.
        _.each(wholeScript, function(line, lineNumber) {
            if (line.trim().toUpperCase().indexOf('SET')==0 ||
                line.trim().toUpperCase().indexOf('REGISTER')==0 ||
                line.trim().toUpperCase().indexOf('IMPORT')==0 ||
                line.trim().toUpperCase().indexOf('DEFINE')==0) {
                newScript += line + '\n';
            }
        });
        // Get line numbers and grab necessary code.
        _.each(GraphView.options.highlightedIds, function(id) {
            var lineNumber = $('g #'+id).data('line-number');
            if (lineNumber >= 0) {
                codeLines.push(lineNumber);
            }
        });
        codeLines.sort(function(a,b){return a-b});
        codeLines = _.uniq(codeLines);
        _.each(codeLines, function(lineNumber) {
            startLine = lineNumber-1;
            endLine   = Script.options.lineNumbers[_.indexOf(Script.options.lineNumbers, lineNumber,true)+1];
            if (endLine === undefined) { endLine = Script.options.myCodeMirror.lineCount()-1; }
            else { endLine -= 2; }
            newScript += '\n' + Script.options.myCodeMirror.getRange({'ch':0,'line':startLine}, {'ch':999999999,'line':endLine});
        });
        newScript += '\n'+limitCmd+'\n'+dumpCmd+'\n';
        // Populate modal and show.
        Main.displayModal({title:"Breakpoint Code", html:_.template(Templates.breakpointCodeTmpl, newScript, {variable:'data'})});
        $(document).trigger('loadBreakpointCode.tossboss-graph-script', [id]);
    },
    /**
     * Highlight the line number in the script.
     *
     * @param {Number} lineNumber The line number to highlight
     */
    highlightLine: function(lineNumber) {
        Script.removeHighlight();
        if (lineNumber > 0) {
            var nextLineNumber = Script.options.lineNumbers[_.indexOf(Script.options.lineNumbers,lineNumber,true)+1] - 1;
            if (isNaN(nextLineNumber)) { nextLineNumber = Script.options.myCodeMirror.lineCount(); }
            --lineNumber;
            --nextLineNumber;
            var doHighlight = false;
            while(lineNumber <= nextLineNumber) {
                if (doHighlight ||
                        (Script.options.myCodeMirror.getLine(nextLineNumber).trim().length > 0 &&
                         Script.options.myCodeMirror.getLine(nextLineNumber).trim().substring(0,2) != '--'))
                {
                    Script.options.myCodeMirror.setLineClass(nextLineNumber,'','highlight');
                    doHighlight = true;
                }
                --nextLineNumber;
            }
            Script.options.myCodeMirror.scrollTo(0,(lineNumber-5)*($(".CodeMirror-lines pre").height())+7);
        }
        $(document).trigger('highlightCodeLine.tossboss-graph-script', [lineNumber]);
    },
    /**
     * Remove all line highlighting in the script.
     */
    removeHighlight: function() {
        lines = Script.options.myCodeMirror.lineCount();
        for(i=0; i<=lines; i++) {
            Script.options.myCodeMirror.setLineClass(i);
        }
    }
};
