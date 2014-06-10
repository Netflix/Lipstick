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
package com.netflix.lipstick.adaptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.parser.PigParserNode.InvocationPoint;
import org.apache.pig.parser.SourceLocation;

import com.google.common.collect.Lists;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

/**
 * Base adaptor class to convert pig LogicalRelationalOperator to P2jLogicalRelationalOperator.
 *
 * @author jmagnusson
 */
public class LOJsonAdaptor {

    protected static final Map<Byte, String> TYPESMAP = DataType.genTypeToNameMap();
    protected final P2jLogicalRelationalOperator p2j;

    private static final Log LOG = LogFactory.getLog(LOJsonAdaptor.class);

    /**
     * Instantiate a new LOJsonAdaptor.
     *
     * @param node the LogicalRelationalOperator to convert
     * @param lp the LogicalPlan containing the LogicalRelationalOperator
     * @throws FrontendException
     */
    public LOJsonAdaptor(LogicalRelationalOperator node, LogicalPlan lp) throws FrontendException {
        this(node, new P2jLogicalRelationalOperator(), lp);
    }

    protected LOJsonAdaptor(LogicalRelationalOperator node, P2jLogicalRelationalOperator p2j, LogicalPlan lp) throws FrontendException {
        this.p2j = p2j;
        p2j.setSchemaString(node.getSchema() != null ? Util.translateSchema(node.getSchema()).toString() : null);
        p2j.setOperator(node.getClass().getSimpleName());
        p2j.setAlias(node.getAlias());
        p2j.setLocation(node.getLocation().line(), node.getLocation().file(), getMacroList(node));
    }

    /**
     * Get the newly created P2jLogicalRelationalOperator.
     * @return the newly created P2jLogicalRelationalOperator
     */
    public P2jLogicalRelationalOperator getToP2jOperator() {
        return p2j;
    }

    /**
     * Get the list of macros for an operator.
     * @param node the operator
     * @return a list of macros
     */
    protected List<String> getMacroList(LogicalRelationalOperator node) {
        List<String> macro = Lists.newArrayList();
        SourceLocation loc = node.getLocation();
        if (loc.node() != null) {
            InvocationPoint p = loc.node().getNextInvocationPoint();
            while (p != null) {
                if (p.getMacro() != null) {
                    macro.add(p.getMacro());
                }
                p = loc.node().getNextInvocationPoint();
            }
        }
        return macro;
    }

    /**
     * Return the uids for a list of operators.
     *
     * @param nodes the list of operators
     * @param reverseMap a mapping of operator to uid
     * @return a list of uids
     */
    protected List<String> getChain(List<Operator> nodes, Map<Operator, String> reverseMap) {
        List<String> chain = Lists.newLinkedList();
        if (nodes != null && !nodes.isEmpty()) {
            for (Operator i : nodes) {
                chain.add(reverseMap.get(i));
            }
        }
        return chain;
    }

    /**
     * Convertor of LogicalExpressionPlan to readable text string.
     *
     * @author jmagnusson
     */
    static class LogicalExpressionPlanSerializer {

        private static final Map<String, Op> SPECOPS = createMap();

        private static class Op {
            protected String string;
            protected Integer priority;

            public Op(String string, Integer priority) {
                this.string = string;
                this.priority = priority;
            }
        }

        private static Map<String, Op> createMap() {
            Map<String, Op> aMap = new HashMap<String, Op>();
            aMap.put("Multiply", new Op(" * ", 7));
            aMap.put("Divide", new Op(" / ", 7));
            aMap.put("Add", new Op(" + ", 6));
            aMap.put("Subtract", new Op(" - ", 6));
            aMap.put("GreaterThan", new Op(" > ", 5));
            aMap.put("GreaterThanEqual", new Op(" >= ", 5));
            aMap.put("LessThan", new Op(" < ", 5));
            aMap.put("LessThanEqual", new Op(" <= ", 5));
            aMap.put("Equal", new Op(" == ", 4));
            aMap.put("NotEqual", new Op(" != ", 4));
            // aMap.put("Not", new Op("!", 3));
            aMap.put("And", new Op(" and ", 2));
            aMap.put("Or", new Op(" or ", 1));
            return Collections.unmodifiableMap(aMap);
        }

        private static String paren(String str, Operator parent, Operator child) {

            String pKey = parent.getName();
            String cKey = child.getName();

            if (SPECOPS.containsKey(cKey) && SPECOPS.containsKey(pKey)) {
                if (SPECOPS.get(pKey).priority > SPECOPS.get(cKey).priority) {
                    return "(" + str + ")";
                }
            }
            return str;
        }

        private static String nodeToString(LogicalExpression node, LogicalExpressionPlan plan) throws FrontendException {
            if (node instanceof ConstantExpression) {
                Object value = ((ConstantExpression) node).getValue();
                if (value != null) {
                    return value.toString();
                } else {
                    // This should ONLY happen in edge cases when the constant expression is built
                    // incorrectly, eg with the ASSERT operator in the case where there's no message
                    return "null"; 
                }                
            }
            
            List<String> outList = Lists.newArrayList();
            List<Operator> s = plan.getSuccessors(node);
            if (s != null && !s.isEmpty()) {
                ListIterator<Operator> iter = s.listIterator();
                if (node instanceof BinCondExpression) {
                    BinCondExpression n = (BinCondExpression) node;
                    return "("
                           + nodeToString(n.getCondition(), plan)
                           + " ? "
                           + nodeToString(n.getLhs(), plan)
                           + " : "
                           + nodeToString(n.getRhs(), plan)
                           + ")";
                }
                while (iter.hasNext()) {
                    Operator op = iter.next();
                    String outString = nodeToString((LogicalExpression) op, plan);
                    if (node instanceof MapLookupExpression) {
                        outString += "#'" + ((MapLookupExpression) node).getLookupKey() + "'";
                    }
                    outList.add(paren(outString, node, op));
                }
                if (node instanceof MapLookupExpression) {
                    return StringUtils.join(outList, ", ");
                }
                if (node instanceof UserFuncExpression) {
                    String className = ((UserFuncExpression) node).getFuncSpec().getClassName();
                    ArrayList<String> split = Lists.newArrayList(StringUtils.split(className, "."));
                    className = split.get(split.size() - 1);
                    return className + '(' + StringUtils.join(outList, ", ") + ')';
                }
                if (SPECOPS.containsKey(node.getName())) {
                    if (outList.size() == 1) {
                        return SPECOPS.get(node.getName()).string + outList.get(0);
                    }
                    return StringUtils.join(outList, SPECOPS.get(node.getName()).string);
                }
                String name = node.getName();
                if (name.equals("Cast")) {
                    return outList.get(0);
                }
                return name + '(' + StringUtils.join(outList, ", ") + ')';
            }
            if (node.getFieldSchema().alias != null) {
                return node.getFieldSchema().alias;
            }
            if (node instanceof ProjectExpression) {
                return '$' + ((Integer) ((ProjectExpression) node).getColNum()).toString();
            }
            return "?";
        }

        /**
         * Convert a LogicalExpressionPlan to a human readable string.
         *
         * @param src the LogicalExpressionPlan
         * @return a human readable string
         */
        public static String serialize(LogicalExpressionPlan src) {
            List<Operator> sources = src.getSources();
            if (!sources.isEmpty()) {
                try {
                    return nodeToString((LogicalExpression) src.getSources().get(0), src);
                } catch (FrontendException e) {
                    LOG.error(e);
                    throw new RuntimeException(e);
                }
            }
            return "";
        }
    }
}
