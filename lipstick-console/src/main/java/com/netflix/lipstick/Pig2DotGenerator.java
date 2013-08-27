/**
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
package com.netflix.lipstick;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.kohsuke.graphviz.Edge;
import org.kohsuke.graphviz.Graph;
import org.kohsuke.graphviz.Node;
import org.kohsuke.graphviz.Style;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.jatl.Html;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLOCogroup;
import com.netflix.lipstick.model.operators.P2jLOFilter;
import com.netflix.lipstick.model.operators.P2jLOJoin;
import com.netflix.lipstick.model.operators.P2jLOLimit;
import com.netflix.lipstick.model.operators.P2jLOLoad;
import com.netflix.lipstick.model.operators.P2jLOSplitOutput;
import com.netflix.lipstick.model.operators.P2jLOStore;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator.Join;
import com.netflix.lipstick.model.operators.elements.JoinExpression;
import com.netflix.lipstick.model.operators.elements.SchemaElement;

/**
 * Transforms P2jPlan into graphviz output.
 *
 * @author jmagnusson
 *
 */
public class Pig2DotGenerator {

    private static final Log LOG = LogFactory.getLog(Pig2DotGenerator.class);

    protected static final String BG_CLUSTER = "#E9E9E9";
    protected static final String BG_ALIAS = "#424242";
    protected static final String BG_EXPRESSION = "#BCBCBC";
    protected static final String BG_MAP_TASK = "#3299BB";
    protected static final String BG_RED_TASK = "#FF9900";
    protected static final String BG_UNK_TASK = "#BF0A0D";
    protected static final String BG_WHITE = "#FFFFFF";

    protected Map<String, P2jLogicalRelationalOperator> p2jMap = null;
    protected P2jPlan p2jPlan;

    /**
     * Instantiates a new Pig2DotGenerator.
     *
     * @param p2jPlan the P2jPlan to convert to dot / graphical format
     */
    public Pig2DotGenerator(P2jPlan p2jPlan) {
        this.p2jPlan = p2jPlan;
        p2jMap = p2jPlan.getPlan();
    }

    /**
     * Returns the column span for the html representation of a logical operator.
     *
     * @param oper the logical operator
     * @return a string representing the column span
     */
    protected String getColspan(P2jLogicalRelationalOperator oper) {
        Integer colspan = null;
        String operator = oper.getOperator().toLowerCase();
        if (operator.equals("lojoin") || operator.equals("locogroup")) {
            Join join;
            if (operator.equals("lojoin")) {
                join = ((P2jLOJoin) oper).getJoin();
            } else {
                join = ((P2jLOCogroup) oper).getGroup();
            }
            colspan = join.getExpression().size();
        }
        return colspan != null ? colspan.toString() : "2";
    }

    /**
     * Returns the header color for a logical operator based on MR step type.
     *
     * @param oper the logical operator
     * @return a string representing the header color
     */
    protected String getJobColor(P2jLogicalRelationalOperator oper) {
        if (oper.getMapReduce() != null) {
            String stepType = oper.getMapReduce().getStepType();
            if (stepType.equals("MAPPER")) {
                return BG_MAP_TASK;
            }
            if (stepType.equals("REDUCER")) {
                return BG_RED_TASK;
            }
        }
        return BG_UNK_TASK;
    }

    /**
     * Checks if a logical operator's schema is equal to that of its predecessor.
     *
     * @param oper the logical operator
     * @return True if the schemas are equal, otherwise False
     */
    protected Boolean schemaEqualsPredecessor(P2jLogicalRelationalOperator oper) {
        if (oper.getSchemaString() != null) {
            String operString = oper.getSchemaString().substring(1, oper.getSchemaString().length() - 1);
            for (String predName : oper.getPredecessors()) {
                P2jLogicalRelationalOperator pred = p2jMap.get(predName);
                try {
                    if (pred.getSchemaString() != null) {
                        String predString = pred.getSchemaString().substring(1, pred.getSchemaString().length() - 1);
                        if (!Schema.equals(Utils.getSchemaFromString(predString),
                                           Utils.getSchemaFromString(operString),
                                           true,
                                           false)) {
                            return false;
                        }
                    }
                } catch (ParserException e) {
                    LOG.warn("Error comparing operator predecessors: ", e);
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Check if the schema should be displayed for a logical operator.
     *
     * @param oper the logical operator
     * @return a boolean indicating whether or not the schema should be displayed
     */
    protected Boolean displaySchema(P2jLogicalRelationalOperator oper) {
        if (oper.getLocation().getLine() != null
            && !schemaEqualsPredecessor(oper)
            && !oper.getOperator().equalsIgnoreCase("LOSplit")
            && !oper.getOperator().equalsIgnoreCase("LOFilter")
            && !oper.getOperator().equalsIgnoreCase("LODistinct")
            && !oper.getOperator().equalsIgnoreCase("LOLimit")
            && !oper.getOperator().equalsIgnoreCase("LOJoin")
            && !oper.getOperator().equalsIgnoreCase("LOCogroup")) {
            return true;
        }
        return false;
    }

    /**
     * Append the html representation of a logical operator's schema to preexisting html.
     *
     * @param html the html to append to
     * @param oper the logical operator
     * @return the appended html
     */
    protected Html genSchema(Html html, P2jLogicalRelationalOperator oper) {
        if (displaySchema(oper)) {
            if (oper.getSchema() != null) {
                Iterator<SchemaElement> iter = oper.getSchema().iterator();
                while (iter.hasNext()) {
                    SchemaElement e = iter.next();
                    html.tr().bgcolor(BG_WHITE);
                    html.td().bgcolor(BG_WHITE).text(e.getAlias() != null ? e.getAlias() : "?").end();
                    html.td().bgcolor(BG_WHITE).text(e.getType()).end();
                    html.end();
                }
            }
        }
        return html;
    }

    /**
     * Append the html representation of join expressions to some preexisting html.
     *
     * @param html the html to append to
     * @param oper the logical operator
     * @return the appened html
     */
    protected Html genJoinExpressions(Html html, P2jLogicalRelationalOperator oper) {
        if (oper.getOperator().equalsIgnoreCase("LOJoin") || oper.getOperator().equalsIgnoreCase("LOCogroup")) {
            Join join;
            if (oper.getOperator().equalsIgnoreCase("LOJoin")) {
                join = ((P2jLOJoin) oper).getJoin();
            } else {
                join = ((P2jLOCogroup) oper).getGroup();
            }
            Set<Entry<String, JoinExpression>> expressions = join.getExpression().entrySet();
            List<List<String>> exp = Lists.newArrayList();
            for (Entry<String, JoinExpression> entry : expressions) {
                exp.add(entry.getValue().getFields());
            }
            if (expressions.size() > 1) {
                html.tr();
                for (Entry<String, JoinExpression> entry : expressions) {
                    html.td().bgcolor(BG_EXPRESSION).text(entry.getKey() == null ? "null" : entry.getKey()).end();
                }
                html.end();
            }
            for (int i = 0; i < exp.get(0).size(); i++) {
                html.tr();
                for (int j = 0; j < exp.size(); j++) {
                    html.td().bgcolor(BG_WHITE).text(exp.get(j).get(i)).end();
                }
                html.end();
            }
            html.end();
        }
        return html;
    }

    /**
     * Generate additional info for a logical operator's node header.
     *
     * @param oper the logical operator
     * @return a string containing additional header text
     */
    protected String getAdditionalInfo(P2jLogicalRelationalOperator oper) {
        if (oper.getLocation().getMacro().size() > 0) {
            return "MACRO: " + oper.getLocation().getMacro().get(0);
        }
        if (oper.getOperator().equalsIgnoreCase("LOLimit")) {
            return Long.toString(((P2jLOLimit) oper).getRowLimit());
        }
        if (oper.getOperator().equalsIgnoreCase("LOJoin")) {
            P2jLOJoin loj = (P2jLOJoin) oper;
            return loj.getJoin().getType() + ", " + loj.getJoin().getStrategy();
        }
        return "";
    }

    /**
     * Append a row containing misc data for a logical operator to preexisting html.
     *
     * @param html the html to append to
     * @param oper the logical operator
     * @return the appended html
     */
    protected Html genMiscRow(Html html, P2jLogicalRelationalOperator oper) {
        String expression = null;
        if (oper.getOperator().equalsIgnoreCase("LOFilter")) {
            expression = ((P2jLOFilter) oper).getExpression();
        }
        if (oper.getOperator().equalsIgnoreCase("LOSplitOutput")) {
            expression = ((P2jLOSplitOutput) oper).getExpression();
        }
        if (expression != null) {
            html.tr().td().colspan(getColspan(oper)).bgcolor(BG_EXPRESSION);
            html.text(expression).end(2);
        }
        String storageLocation = null;
        String storageFunction = null;
        if (oper.getOperator().equalsIgnoreCase("LOStore")) {
            storageLocation = ((P2jLOStore) oper).getStorageLocation();
            storageFunction = ((P2jLOStore) oper).getStorageFunction();
        } else if (oper.getOperator().equalsIgnoreCase("LOLoad")) {
            storageLocation = ((P2jLOLoad) oper).getStorageLocation();
            storageFunction = ((P2jLOLoad) oper).getStorageFunction();
        }
        if (storageLocation != null) {
            html.tr().td().colspan(getColspan(oper)).bgcolor(BG_EXPRESSION);
            html.text(storageLocation).end(2);
            html.tr().td().colspan(getColspan(oper)).bgcolor(BG_EXPRESSION);
            html.text(storageFunction).end(2);
        }
        return html;
    }

    /**
     * Append a row describing the operation type of a logical operator to preexisting html.
     *
     * @param html the html to append to
     * @param oper the logical operator
     * @return the appended html
     */
    protected Html genOperationRow(Html html, P2jLogicalRelationalOperator oper) {
        String additionalInfo = getAdditionalInfo(oper);
        if (additionalInfo.length() > 0) {
            additionalInfo = " (" + additionalInfo + ")";
        }
        html.tr().td().colspan(getColspan(oper)).bgcolor(getJobColor(oper));
        String op;
        if (oper.getOperator().equalsIgnoreCase("LOCogroup")
            && ((P2jLOCogroup) oper).getGroup().getExpression().size() < 2) {
            op = "GROUP";
        } else {
            op = oper.getOperator().substring(2).toUpperCase();
        }
        html.text(op + additionalInfo).end(2);
        return html;
    }

    /**
     * Append a row containing the pig alias responsible for a logical operator to preexisting html.
     *
     * @param html the html to append to
     * @param oper the logical operator
     * @return the appended html
     */
    protected Html genAliasRow(Html html, P2jLogicalRelationalOperator oper) {
        if (oper.getAlias() != null && !oper.getOperator().equalsIgnoreCase("LOSplit")) {
            html.tr().td().colspan(getColspan(oper)).bgcolor(BG_ALIAS).font().color("#FFFFFF");
            html.text(oper.getAlias()).end(3);
        }
        return html;
    }

    /**
     * Generate the html describing a logical operator.
     *
     * @param oper the logical operator
     * @return a string representation of the html
     */
    protected String genNodeHtml(P2jLogicalRelationalOperator oper) {
        StringWriter writer = new StringWriter();
        Html html = new Html(writer);

        html.font().attr("point-size", "12").table().border("0").attr("cellborder", "1").cellspacing("0");
        genOperationRow(html, oper);
        genMiscRow(html, oper);
        genAliasRow(html, oper);
        genJoinExpressions(html, oper);
        genSchema(html, oper);
        html.endAll();
        return writer.toString();
    }

    /**
     * Put attributes on a node in the graph (id, html, shape) based on logical operator.
     *
     * @param node the node
     * @param oper the logical operator
     */
    protected void attributeGraphNode(Node node, P2jLogicalRelationalOperator oper) {
        node.id(oper.getUid());
        node.attr("id", oper.getUid());
        node.attr("html", genNodeHtml(oper));
        node.attr("shape", "none");
    }

    /**
     * Append a node to the proper subgraph based on map/reduce job.
     *
     * @param subgraphs map of M/R job scope to subgraph
     * @param node the graph node to append
     * @param oper the logical operator associated with the graph node
     * @return a boolean indicating whether the node was appended to a subgraph
     */
    protected Boolean appendToSubgraph(Map<String, Graph> subgraphs, Node node, P2jLogicalRelationalOperator oper) {
        String jid = null;
        if (oper.getMapReduce() != null && oper.getMapReduce().getJobId() != null) {
            jid = oper.getMapReduce().getJobId();
            if (!subgraphs.containsKey(jid)) {
                Graph g = new Graph();
                g.id("cluster_" + jid.replaceAll("-", ""));
                g.attr("bgcolor", BG_CLUSTER);
                Style s = new Style();
                s.attr("rounded");
                s.attr("filled");
                g.style(s);
                subgraphs.put(jid, g);
            }
            subgraphs.get(jid).node(node);
            return false;
        }
        return false;
    }

    /**
     * Generate a graph object for the logical plan.
     *
     * @return the graph object
     */
    protected Graph generateGraph() {
        Graph gv = new Graph();
        Map<String, Graph> subgraphs = Maps.newHashMap();

        gv.attr("rankdir", "TB");
        Map<P2jLogicalRelationalOperator, Node> graphMap = Maps.newHashMap();
        for (Entry<String, P2jLogicalRelationalOperator> e : p2jMap.entrySet()) {
            Node node = new Node();
            graphMap.put(e.getValue(), node);
        }
        for (Entry<P2jLogicalRelationalOperator, Node> e : graphMap.entrySet()) {
            Node node = e.getValue();
            attributeGraphNode(node, e.getKey());
            if (!appendToSubgraph(subgraphs, node, e.getKey())) {
                gv.node(node);
            }
            for (String i : e.getKey().getSuccessors()) {
                P2jLogicalRelationalOperator dst = p2jMap.get(i);
                Edge edge = new Edge(node, graphMap.get(dst));
                gv.edge(edge);
            }
        }
        for (Entry<String, Graph> sg : subgraphs.entrySet()) {
            gv.subGraph(sg.getValue());
        }
        return gv;
    }

    /**
     * Generate a dot representation of the P2jPlan in the specified format.
     *
     * @param format the format
     * @return a string representation of the plan in the format specified
     * @throws InterruptedException an interrupted exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public String generatePlan(String format) throws InterruptedException, IOException {
        LOG.info("Generating script graphic of type " + format);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        List<String> args = Lists.newArrayList();
        args.add("dot");
        args.add("-T" + format);
        Graph g = generateGraph();
        g.generateTo(args, os);
        return os.toString();
    }
}
