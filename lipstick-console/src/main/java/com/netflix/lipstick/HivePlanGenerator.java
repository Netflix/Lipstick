package com.netflix.lipstick;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Operator;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.Task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.netflix.lipstick.adaptors.HiveJsonAdaptor;
import com.netflix.lipstick.hivetolipstick.H2LClient;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;
import com.netflix.lipstick.model.operators.elements.SchemaElement;

public class HivePlanGenerator {
    private static final Log LOG = LogFactory.getLog(H2LClient.class);

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
        //QueryPlan may have duplicate stages in 'getStageList()', dedupe them
        List<Stage> uniqueStages = getUniqueStages(queryPlan.getQuery().getStageList());

        HivePlan hp = new HivePlan(queryPlan.getQueryId());
        addStagesToPlan(hp, uniqueStages, queryPlan.getQuery().getStageGraph().getAdjacencyList());
        addTasksToStages(hp, uniqueStages);
        addOperatorsToTasks(hp, uniqueStages);

        associateSchemaWithOperators(hp, queryPlan);

        connectTasksViaOperators(hp);
        connectStagesViaOperators(hp);

        Map<String, P2jLogicalRelationalOperator> nodeMap = Maps.newHashMap();
        addOperatorsToNodeMap(hp, nodeMap);

        p2jPlan = new P2jPlan(nodeMap);

        LOG.debug("HIVE PLAN\n" + hp.toString());
    }

    /**
     * Gets the p2j plan.
     *
     * @return the p2j plan
     */
    public P2jPlan getP2jPlan() {
        return p2jPlan;
    }

    protected void addStagesToPlan(HivePlan hp, List<Stage> stages, List<Adjacency> aList) {
        Map<String, HiveStage> stageIdToHiveStageMap = Maps.newHashMap();

        Map<String, String> stageIdToStageTypeMap = Maps.newHashMap();
        for(Stage s : stages) {
            stageIdToStageTypeMap.put(s.getStageId(), s.getStageType().toString());
        }

        for(Adjacency stageAdj : aList) {
            String stageId = stageAdj.getNode();

            HiveStage stage = new HiveStage(stageId, hp, stageIdToStageTypeMap.get(stageId));
            if(!stageIdToHiveStageMap.containsKey(stageId)) {
                stageIdToHiveStageMap.put(stageId, stage);
                hp.stages.add(stage);
            } else {
                stage = stageIdToHiveStageMap.get(stageId);
            }

            for(String childStageId : stageAdj.getChildren()) {
                HiveStage childStage = new HiveStage(childStageId, hp, stageIdToStageTypeMap.get(childStageId));
                if(!stageIdToHiveStageMap.containsKey(childStageId)) {
                    stageIdToHiveStageMap.put(childStageId, childStage);
                    hp.stages.add(childStage);
                } else {
                    childStage = stageIdToHiveStageMap.get(childStageId);
                }
                stage.childStages.add(childStage);
            }
        }
    }

    protected void addTasksToStages(HivePlan hp, List<Stage> stages) {
        for(Stage s : stages) {
            HiveStage hs = hp.getStageById(s.getStageId());
            HiveTask mapTask = null;
            HiveTask reduceTask = null;
            for(Task t : s.getTaskList()) {
                if(hs.getTaskById(t.getTaskId()) == null) {
                    HiveTask ht = new HiveTask(t.getTaskId(), hp, hs, t.getTaskType().toString());
                    hs.tasks.add(ht);

                    if(ht.taskType.equals("MAP")) {
                        if(mapTask != null) {
                            LOG.error("MORE THAN ONE MAP TASK IN STAGE [" + s.getStageId() + "]");
                        }

                        mapTask = ht;
                    } else if(ht.taskType.equals("REDUCE")) {
                        if(reduceTask != null) {
                            LOG.error("MORE THAN ONE REDUCE TASK IN STAGE [" + s.getStageId() + "]");
                        }

                        reduceTask = ht;
                    }
                }
            }

            if(mapTask != null && reduceTask != null) {
                mapTask.childTasks.add(reduceTask);
            }
        }
    }

    protected void addOperatorsToTasks(HivePlan hp, List<Stage> stages) {
        for(Stage s : stages) {
            HiveStage hs = hp.getStageById(s.getStageId());
            for(Task t : s.getTaskList()) {
                HiveTask ht = hs.getTaskById(t.getTaskId());

                if(t.getOperatorGraph() != null) {
                    addOperatorsToTask(ht, t.getOperatorList(), t.getOperatorGraph().getAdjacencyList());
                }
            }
        }
    }

    protected void addOperatorsToTask(HiveTask ht, List<Operator> opList, List<Adjacency> aList) {
        Map<String, HiveOp> opIdToHiveOpMap = Maps.newHashMap();
        Map<String, String> opIdToOpTypeMap = Maps.newHashMap();

        for(Operator op : opList) {
            opIdToOpTypeMap.put(op.getOperatorId(), op.getOperatorType().toString());
        }

        for(Adjacency opAdj : aList) {
            String opId = opAdj.getNode();

            HiveOp op = null;
            if(!opIdToHiveOpMap.containsKey(opId)) {
                op = new HiveOp(opId, ht.parentPlan, ht.parentStage, ht, opIdToOpTypeMap.get(opId));
                opIdToHiveOpMap.put(opId, op);
                ht.ops.add(op);
            } else {
                op = opIdToHiveOpMap.get(opId);
            }

            for(String childOpId : opAdj.getChildren()) {
                HiveOp childOp = null;
                if(!opIdToHiveOpMap.containsKey(childOpId)) {
                    childOp = new HiveOp(childOpId, ht.parentPlan, ht.parentStage, ht, opIdToOpTypeMap.get(childOpId));
                    opIdToHiveOpMap.put(childOpId, childOp);
                    ht.ops.add(childOp);
                } else {
                    childOp = opIdToHiveOpMap.get(childOpId);
                }
                op.childOps.add(childOp);
            }
        }

        for(HiveOp op : ht.ops) {
            String childOpIds = "";
            for(HiveOp childOp : op.childOps) {
                if(childOpIds.length() > 0) {
                    childOpIds += ", ";
                }
                childOpIds += childOp.getId();
            }
        }
    }

    protected void associateSchemaWithOperators(HivePlan hp, QueryPlan qp) {
        Map<String, org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> stageIdToExecTaskMap = buildStageIdToExecTaskMap(qp);

        for(String stageId : stageIdToExecTaskMap.keySet()) {
            org.apache.hadoop.hive.ql.exec.Task<? extends Serializable> task = stageIdToExecTaskMap.get(stageId);
            HiveStage hs = hp.getStageById(stageId);

            Map<String, HiveOp> hiveOpsForStage = Maps.newHashMap();
            for(HiveTask ht : hs.tasks) {
                for(HiveOp ho : ht.ops) {
                    hiveOpsForStage.put(ho.name, ho);
                }
            }

            for(org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc> execOp : getAllOperators(task)) {
                if(hiveOpsForStage.containsKey(execOp.getOperatorId())) {
                    hiveOpsForStage.get(execOp.getOperatorId()).schemaString = getSchemaString(execOp.getSchema());
                } else {
                    LOG.error("Unable to find HiveOp with name [" + execOp.getOperatorId() + "] for stage [" + stageId + "]");
                }
            }
        }
    }

    protected void connectTasksViaOperators(HivePlan hp) {
        for(HiveStage hs : hp.stages) {
            if(hs.tasks.size() != 2) {
                continue;
            }

            Set<HiveOp> leafOpsOfParent = Sets.newHashSet();
            Set<HiveOp> rootOpsOfChild = Sets.newHashSet();
            for(HiveTask ht : hs.getRootTasks()) {
                leafOpsOfParent = ht.getLeafOps();
            }
            for(HiveTask ht : hs.getLeafTasks()) {
                rootOpsOfChild = ht.getRootOps();
            }

            if(!leafOpsOfParent.isEmpty() && !rootOpsOfChild.isEmpty()) {
                connectOperators(leafOpsOfParent, rootOpsOfChild);
            }
        }
    }

    protected void connectStagesViaOperators(HivePlan hp) {
        for(HiveStage parentStage : hp.stages) {
            if(parentStage.stageType.equals("CONDITIONAL")) {
                continue;
            }

            Set<HiveStage> childrenToAttach = parentStage.getClosestMapRedChildren();
            if(!childrenToAttach.isEmpty()) {
                Set<HiveOp> leafOpsOfParent = Sets.newHashSet();
                Set<HiveOp> rootOpsOfChild = Sets.newHashSet();

                for(HiveTask ht : parentStage.getLeafTasks()) {
                    for(HiveOp ho : ht.getLeafOps()) {
                        leafOpsOfParent.add(ho);
                    }
                }

                for(HiveStage childStage : childrenToAttach) {
                    for(HiveTask ht : childStage.getRootTasks()) {
                        for(HiveOp ho : ht.getRootOps()) {
                            rootOpsOfChild.add(ho);
                        }
                    }
                }

                connectOperators(leafOpsOfParent, rootOpsOfChild);
            }
        }
    }

    protected void connectOperators(Set<HiveOp> from, Set<HiveOp> to) {
        for(HiveOp fromOp : from) {
            for(HiveOp toOp : to) {
                fromOp.childOps.add(toOp);
            }
        }
    }

    protected void addOperatorsToNodeMap(HivePlan hp, Map<String, P2jLogicalRelationalOperator> nodeMap) {
        for(HiveStage hs : hp.stages) {
            for(HiveTask ht : hs.tasks) {
                for(HiveOp ho : ht.ops) {
                    P2jLogicalRelationalOperator p2j = (new HiveJsonAdaptor(ho)).getToP2jOperator();
                    //Temporary until we alter how p2j.setSchemaString() works...
                    try {
                        Field f = p2j.getClass().getDeclaredField("schemaString");
                        f.setAccessible(true);
                        f.set(p2j, ho.schemaString);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    nodeMap.put(ho.getId(), p2j);
                }
            }
        }
    }

    protected List<Stage> getUniqueStages(List<Stage> stages) {
        List<Stage> uniqueStages = Lists.newArrayList();
        Set<String> usedStageIds = Sets.newHashSet();

        for(Stage stage : stages) {
            if(!usedStageIds.contains(stage.getStageId())) {
                uniqueStages.add(stage);
                usedStageIds.add(stage.getStageId());
            }
        }
        return uniqueStages;
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
        List<SchemaElement> elements = Lists.newArrayList();
        if(schemaStr != null && schemaStr.length() > 2) {
            schemaStr = schemaStr.substring(1, schemaStr.length() - 1);

            for(String field : schemaStr.split(",")) {
                String[] nameTypePair = field.split(":");
                if(nameTypePair.length >= 2) {
                    elements.add(new SchemaElement(nameTypePair[0].trim(), nameTypePair[1].trim(), 0L, null));
                } else {
                    LOG.warn("Unable to get name and type from [" + schemaStr + "] schema may not be correct!");
                }
            }
        }
        return elements;
    }

    protected String generateUniqueOperatorId(Operator op, Stage s) {
        return op.getOperatorId() + "_" + extractStageNumber(s);
    }

    protected String extractStageNumber(Stage s) {
        String stageId = s.getStageId();
        return stageId.substring(stageId.indexOf('-'), + 1);
    }

    protected Map<String, org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> buildStageIdToExecTaskMap(QueryPlan queryPlan) {
        Map<String, org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> stageIdToExecTaskMap = Maps.newHashMap();
        try {
            Set<String> stageIds = Sets.newHashSet();
            for(Stage s : queryPlan.getQueryPlan().getStageList()) {
                stageIds.add(s.getStageId());
            }

            for(org.apache.hadoop.hive.ql.exec.Task<? extends Serializable> task : getAllTasks(queryPlan.getRootTasks(), null)) {
                if(stageIds.contains(task.getId())) {
                    stageIdToExecTaskMap.put(task.getId(), task);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return stageIdToExecTaskMap;
    }

    protected List<org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> getAllTasks(Collection<org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> thisLevelTasks,
                                                                                            List<org.apache.hadoop.hive.ql.exec.Task<? extends Serializable>> allTasks) {
        if(allTasks == null) {
            allTasks = Lists.newArrayList();
        }

        for(org.apache.hadoop.hive.ql.exec.Task<? extends Serializable> task : thisLevelTasks) {
            allTasks.add(task);

            if(task.getChildTasks() != null) {
                getAllTasks(task.getChildTasks(), allTasks);
            }
        }

        return allTasks;
    }

    protected List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> getAllOperators(org.apache.hadoop.hive.ql.exec.Task<? extends Serializable> task) {
        List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> topOps = new ArrayList<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>>(task.getTopOperators());
        //The reducer task is not accessible through the 'getTopOperators()' tree, add it
        if(task.getReducer() != null) {
            topOps.add(task.getReducer());
        }
        return getAllOperators(topOps, null);
    }

    protected List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> getAllOperators(Collection<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> thisLevelOps,
                                                                                                    List<org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc>> allOps) {
        if(allOps == null) {
            allOps = Lists.newArrayList();
        }

        for(org.apache.hadoop.hive.ql.exec.Operator<? extends OperatorDesc> op : thisLevelOps) {
            allOps.add(op);
            if(op.getChildOperators() != null) {
                getAllOperators(op.getChildOperators(), allOps);
            }
        }

        return allOps;
    }

    public static class HivePlan {
        private final String name;
        public Set<HiveStage> stages = Sets.newHashSet();

        public HivePlan(String name) {
            this.name = name;
        }

        public String getId() {
            return name;
        }

        public HiveStage getStageById(String stageId) {
            for(HiveStage hs : stages) {
                if(hs.name.equals(stageId)) {
                    return hs;
                }
            }
            return null;
        }

        public Set<HiveStage> getRootStages() {
            Set<HiveStage> roots = Sets.newHashSet();

            for(HiveStage stage : stages) {
                roots.add(stage);
            }

            for(HiveStage stage : stages) {
                for(HiveStage childStage : stage.childStages) {
                    roots.remove(childStage);
                }
            }

            return roots;
        }

        public Set<HiveStage> getLeafStages() {
            Set<HiveStage> leaves = Sets.newHashSet();

            for(HiveStage stage : stages) {
                if(stage.childStages.isEmpty()) {
                    leaves.add(stage);
                }
            }

            return leaves;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Plan: " + name);
            for(HiveStage hs : getRootStages()) {
                sb.append("\n\t" + hs.toString());
            }
            return sb.toString();
        }
    }

    public static class HiveStage {
        private final String name;
        public final String stageType;
        public final HivePlan parentPlan;
        public Set<HiveStage> childStages = Sets.newHashSet();
        public Set<HiveTask> tasks = Sets.newHashSet();

        public HiveStage(String name, HivePlan parentPlan, String stageType) {
            this.name = name;
            this.parentPlan = parentPlan;
            this.stageType = stageType;
        }

        public String getId() {
            return name;
        }

        public HiveTask getTaskById(String taskId) {
            for(HiveTask ht : tasks) {
                if(ht.name.equals(taskId)) {
                    return ht;
                }
            }
            return null;
        }

        public Set<HiveTask> getRootTasks() {
            Set<HiveTask> roots = Sets.newHashSet();

            for(HiveTask task : tasks) {
                roots.add(task);
            }

            for(HiveTask task : tasks) {
                for(HiveTask childTask : task.getAllChildren()) {
                    roots.remove(childTask);
                }
            }

            return roots;
        }

        public Set<HiveTask> getLeafTasks() {
            Set<HiveTask> leaves = Sets.newHashSet();

            for(HiveTask task : tasks) {
                if(task.childTasks.isEmpty()) {
                    leaves.add(task);
                }
            }

            return leaves;
        }

        public Set<HiveStage> getAllChildren() {
            Set<HiveStage> allChildren = Sets.newHashSet();

            for(HiveStage child : childStages) {
                allChildren.add(child);
                for(HiveStage subChild : child.getAllChildren()) {
                    allChildren.add(subChild);
                }
            }

            return allChildren;
        }

        public Set<HiveStage> getClosestMapRedChildren() {
            Set<HiveStage> closestMapRedChildren = Sets.newHashSet();

            for(HiveStage child : childStages) {
                if(!child.stageType.equals("MAPRED")) {
                    for(HiveStage subClosest : child.getClosestMapRedChildren()) {
                        closestMapRedChildren.add(subClosest);
                    }
                } else {
                    closestMapRedChildren.add(child);
                }
            }

            return closestMapRedChildren;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Stage: " + name);
            for(HiveTask task : getRootTasks()) {
                sb.append("\n\t\t" + task.toString());
            }
            for(HiveStage hs : childStages) {
                sb.append("\n\t" + hs.toString());
            }
            return sb.toString();
        }
    }

    public static class HiveTask {
        private final String name;
        public final HivePlan parentPlan;
        public final HiveStage parentStage;
        public final String taskType;
        public Set<HiveTask> childTasks = Sets.newHashSet();
        public Set<HiveOp> ops = Sets.newHashSet();

        public HiveTask(String name, HivePlan parentPlan, HiveStage parentStage, String taskType) {
            this.name = name;
            this.parentPlan = parentPlan;
            this.parentStage = parentStage;
            this.taskType = taskType;
        }

        public String getId() {
            return name;
        }

        public Set<HiveOp> getRootOps() {
            Set<HiveOp> roots = Sets.newHashSet();

            for(HiveOp op : ops) {
                roots.add(op);
            }

            for(HiveOp op : ops) {
                for(HiveOp childOp : op.getAllChildren()) {
                    roots.remove(childOp);
                }
            }

            return roots;
        }

        public Set<HiveOp> getLeafOps() {
            Set<HiveOp> leaves = Sets.newHashSet();

            for(HiveOp op : ops) {
                if(op.childOps.isEmpty()) {
                    leaves.add(op);
                }
            }

            return leaves;
        }

        public Set<HiveTask> getAllChildren() {
            Set<HiveTask> allChildren = Sets.newHashSet();

            for(HiveTask child : childTasks) {
                allChildren.add(child);
                for(HiveTask subChild : child.getAllChildren()) {
                    allChildren.add(subChild);
                }
            }

            return allChildren;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Task: " + name);
            for(HiveOp op : getRootOps()) {
                sb.append("\n\t\t\t" + op.toString());
            }
            for(HiveTask childTask : childTasks) {
                sb.append("\n\t\t" + childTask.toString());
            }
            return sb.toString();
        }
    }

    public static class HiveOp {
        private final String name;
        public final HivePlan parentPlan;
        public final HiveStage parentStage;
        public final HiveTask parentTask;
        public final String opType;
        public String schemaString;
        public Set<HiveOp> childOps = Sets.newHashSet();

        public HiveOp(String name, HivePlan parentPlan, HiveStage parentStage, HiveTask parentTask, String opType) {
            this.name = name;
            this.parentPlan = parentPlan;
            this.parentStage = parentStage;
            this.parentTask = parentTask;
            this.opType = opType;
        }

        public String getId() {
            return name + "_S" + getParentStageNumber();
        }

        private String getParentStageNumber() {
            return parentStage.getId().substring(parentStage.getId().indexOf('-') + 1);
        }

        public Set<HiveOp> getAllChildren() {
            Set<HiveOp> allChildren = Sets.newHashSet();

            for(HiveOp child : childOps) {
                allChildren.add(child);
                for(HiveOp subChild : child.getAllChildren()) {
                    allChildren.add(subChild);
                }
            }

            return allChildren;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Op: " + getId() + ", Schema: " + schemaString);
            for(HiveOp op : childOps) {
                sb.append("\n\t\t\t" + op.toString());
            }
            return sb.toString();
        }
    }
}
