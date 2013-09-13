package com.netflix.lipstick;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Graph;
import org.apache.hadoop.hive.ql.plan.api.Operator;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.Task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.lipstick.adaptors.HiveJsonAdaptor;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;
import com.netflix.lipstick.model.operators.elements.SchemaElement;

public class HivePlanGenerator {

    /** The p2j plan. */
    protected P2jPlan p2jPlan;

    /**
     * Instantiates a new p2j plan generator. Generates reverseMap and p2jPlan.
     *
     * @param lp
     *            the logical plan
     * @throws IOException
     * @throws FrontendException
     *             the frontend exception
     */
    public HivePlanGenerator(QueryPlan queryPlan) throws IOException {
        p2jPlan = new P2jPlan(null);
        Map<String, String> reverseMap = Maps.newHashMap();
        Map<String, P2jLogicalRelationalOperator> nodeMap = Maps.newHashMap();
        Map<String, String> opNameToSchemaMap = Maps.newHashMap();
        fillSchemaMap(queryPlan.getRootTasks(), opNameToSchemaMap);

        for (Stage stage : queryPlan.getQuery().getStageList()) {
            for (Task task : stage.getTaskList()) {
                //Attach at the operator level within a task
                List<Adjacency> aList = task.getOperatorGraph().getAdjacencyList();
                for (Operator op : task.getOperatorList()) {
                    P2jLogicalRelationalOperator p2j = (new HiveJsonAdaptor(op)).getToP2jOperator();
                    p2j.setSuccessors(getSuccessors(p2j.getAlias(), aList));
                    p2j.setPredecessors(new ArrayList<String>());

                    nodeMap.put(p2j.getUid(), p2j);
                    reverseMap.put(op.getOperatorId(), p2j.getUid());
                    String taskType = "UNKNOWN";
                    if (task.getTaskType().toString().equals("MAP")) {
                        taskType = "MAPPER";
                    } else if (task.getTaskType().toString().equals("REDUCE")) {
                        taskType = "REDUCER";
                    }

                    //Should we replace the stageId with jobId from the execution plan??
                    p2j.setMapReduce("scope-" + stage.getStageId().replace("-", ""), taskType);
                    p2j.setSchema(processSchema(opNameToSchemaMap.get(op.getOperatorId())));
                    //Temporary until we alter how p2j.setSchemaString() works...
                    try {
                        Field f = p2j.getClass().getDeclaredField("schemaString");
                        f.setAccessible(true);
                        f.set(p2j, opNameToSchemaMap.get(op.getOperatorId()));
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            //Attach at the task level within a stage
            if (stage.getTaskListSize() == 2) {
                attachSinksToSources(stage.getTaskList().get(0), stage.getTaskList().get(1), nodeMap, reverseMap);
            }
        }
        //Attach at the stage level within a query
        if(queryPlan.getQuery().getStageListSize() >= 2) {
            attachStages(queryPlan.getQuery().getStageList(), queryPlan.getQuery().getStageGraph().getAdjacencyList(), nodeMap, reverseMap);
        }



        p2jPlan = new P2jPlan(nodeMap);
    }

    /**
     * Gets the p2j plan.
     *
     * @return the p2j plan
     */
    public P2jPlan getP2jPlan() {
        return p2jPlan;
    }

    protected void attachStages(List<Stage> sList, List<Adjacency> aList, Map<String, P2jLogicalRelationalOperator> nodeMap, Map<String, String> reverseMap) {
        Map<String, Stage> stageNameMap = Maps.newHashMap();

        for(Stage s : sList) {
            stageNameMap.put(s.getStageId(), s);
        }

        //Traverse each level of stages connecting the leaves of each parent to the roots of each child
        List<String> currStages = getStageRootNames(sList, aList);

        while(currStages.size() > 0) {
            List<String> nextLevelOfStages = Lists.newArrayList();
            for(String stageName : currStages) {
                Stage parentStage = stageNameMap.get(stageName);
                List<String> parentLeafOps = getLeafOpNamesForStage(parentStage);

                for(Adjacency adj : aList) {
                    if(adj.getNode().equals(stageName) && adj.getChildren() != null) {

                        for(String childStageName : adj.getChildren()) {
                            Stage childStage = stageNameMap.get(childStageName);

                            //We have the child roots and parent leaves, connect them
                            for(String childRootOp : getRootOpNamesForStage(childStage)) {
                                for(String parentLeafOp : parentLeafOps) {
                                    nodeMap.get(reverseMap.get(parentLeafOp)).getSuccessors().add(childRootOp);
                                }
                            }

                            nextLevelOfStages.add(childStageName);
                        }
                    }
                }
            }

            currStages = nextLevelOfStages;
        }

    }

    protected void attachSinksToSources(Task sinkTask,
                                        Task sourceTask,
                                        Map<String, P2jLogicalRelationalOperator> nodeMap, Map<String, String> reverseMap) {

        List<Adjacency> aList = sinkTask.getOperatorGraph().getAdjacencyList();
        for (String sinkOperator : getSinks(sinkTask.getOperatorList(), aList)) {
            List<String> successorList = nodeMap.get(reverseMap.get(sinkOperator)).getSuccessors();
            if (sourceTask != null && sourceTask.getOperatorGraph() != null) {
                Graph opGraph = sourceTask.getOperatorGraph();
                List<String> sources = (opGraph.getRoots() != null) ? opGraph.getRoots() : getSources(sourceTask.getOperatorList(), opGraph.getAdjacencyList());

                for (String source : sources) {
                    successorList.add(source);
                }
            }

            nodeMap.get(reverseMap.get(sinkOperator)).setSuccessors(successorList);
        }
    }

    protected List<String> getSinks(List<Operator> opList, List<Adjacency> aList) {
        List<String> sinkList = Lists.newArrayList();
        for (Operator op : opList) {
            if (getSuccessors(op.getOperatorId(), aList).size() == 0) {
                sinkList.add(op.getOperatorId());
            }
        }

        return sinkList;
    }

    protected List<String> getSuccessors(String name, List<Adjacency> aList) {
        for (Adjacency adjacency : aList) {
            if (adjacency.getNode().equals(name)) {
                return adjacency.getChildren();
            }
        }
        return Lists.newArrayList();
    }

    protected List<String> getSources(List<Operator> opList, List<Adjacency> aList) {
        Set<String> sources = Sets.newHashSet();

        //Add all op ids to sources
        for(Operator op : opList) {
            sources.add(op.getOperatorId());
        }

        //Remove all op ids that are children
        for(Adjacency adj : aList) {
            for(String child : adj.getChildren()) {
                sources.remove(child);
            }
        }

        return Lists.newArrayList(sources);
    }

    protected List<String> getStageRootNames(List<Stage> sList, List<Adjacency> aList) {
        Set<String> roots = Sets.newHashSet();

        //Add all stage ids to roots
        for(Stage s : sList) {
            roots.add(s.getStageId());
        }

        //Remove all stage ids that are children
        for(Adjacency adj : aList) {
            for(String child : adj.getChildren()) {
                roots.remove(child);
            }
        }

        return Lists.newArrayList(roots);
    }

    protected List<String> getLeafOpNamesForStage(Stage stage) {
        Task leafTask = stage.getTaskList().get(stage.getTaskListSize() - 1);
        return getSinks(leafTask.getOperatorList(), leafTask.getOperatorGraph().getAdjacencyList());
    }

    protected List<String> getRootOpNamesForStage(Stage stage) {
        Task rootTask = stage.getTaskList().get(0);
        return getSources(rootTask.getOperatorList(), rootTask.getOperatorGraph().getAdjacencyList());
    }

    protected void fillSchemaMap(List<org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> tasks, Map<String, String> opNameToSchemaMap) {
        for(org.apache.hadoop.hive.ql.exec.Task<? extends Serializable> task : tasks) {
            for(org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> op : task.getTopOperators()) {
                fillSchemaMap(op, opNameToSchemaMap);
            }

            if(task.getReducer() != null) {
                fillSchemaMap(task.getReducer(), opNameToSchemaMap);
            }

            if(task.getChildTasks() != null) {
                fillSchemaMap(task.getChildTasks(), opNameToSchemaMap);
            }
        }
    }

    protected void fillSchemaMap(org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> op, Map<String, String> opNameToSchemaMap) {
        opNameToSchemaMap.put(op.getOperatorId(), getSchemaString(op.getSchema()));

        if(op.getChildOperators() != null) {
            for(org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> childOp : op.getChildOperators()) {
                fillSchemaMap(childOp, opNameToSchemaMap);
            }
        }
    }

    protected String getSchemaString(RowSchema rSchema) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for(ColumnInfo ci : rSchema.getSignature()) {
            if(sb.length() > 1) {
                sb.append(',');
            }

            String name = (ci.getAlias() != null && ci.getAlias().length() > 0) ? ci.getAlias() : ci.getInternalName();

            sb.append(name + ": " + ci.getType());
        }
        sb.append('}');
        return sb.toString();
    }

    protected List<SchemaElement> processSchema(String schemaStr) {
        List<SchemaElement> elements = Lists.newLinkedList();
        if(schemaStr != null && schemaStr.length() > 2) {
            schemaStr = schemaStr.substring(1, schemaStr.length() - 1);

            for(String field : schemaStr.split(",")) {
                String[] nameTypePair = field.split(":");

                elements.add(new SchemaElement(nameTypePair[0].trim(), nameTypePair[1].trim(), 0L, null));
            }
        }
        return elements;
    }
}
