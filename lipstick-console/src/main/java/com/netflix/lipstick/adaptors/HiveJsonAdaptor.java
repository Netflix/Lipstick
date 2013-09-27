package com.netflix.lipstick.adaptors;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.lipstick.HivePlanGenerator.HiveOp;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

import org.apache.hadoop.hive.ql.plan.api.Operator;

public class HiveJsonAdaptor {
    protected final P2jLogicalRelationalOperator p2j;

    public HiveJsonAdaptor(Operator op) {
        this(op, new P2jLogicalRelationalOperator());
    }

    public HiveJsonAdaptor(HiveOp ho) {
        this(ho, new P2jLogicalRelationalOperator());
    }

    protected HiveJsonAdaptor(Operator op, P2jLogicalRelationalOperator p2j) {
        this.p2j = p2j;
        p2j.setSchemaString(null);
        p2j.setOperator("HO" + op.getOperatorType().toString());
        p2j.setAlias(op.getOperatorId());
        List<String> emptyList = Lists.newArrayList();
        p2j.setLocation(0, "default", emptyList);
        p2j.setUid(op.getOperatorId());
        p2j.setMapReduce("1", "UNKNOWN");
    }

    protected HiveJsonAdaptor(HiveOp op, P2jLogicalRelationalOperator p2j) {
        this.p2j = p2j;
        p2j.setSchemaString(null);
        p2j.setOperator("HO" + op.opType);
        p2j.setAlias(op.getId());
        List<String> emptyList = Lists.newArrayList();
        p2j.setLocation(0, "default", emptyList);
        p2j.setUid(op.getId());
        String taskType = op.parentTask.taskType;
        if (op.parentTask.taskType.equals("MAP")) {
            taskType = "MAPPER";
        } else if (op.parentTask.taskType.equals("REDUCE")) {
            taskType = "REDUCER";
        }

        p2j.setMapReduce("scope-" + op.parentStage.getId().replace("-", ""), taskType);
        List<String> successors = Lists.newArrayList();
        for(HiveOp childOp : op.childOps) {
            successors.add(childOp.getId());
        }
        p2j.setSuccessors(successors);
        p2j.setPredecessors(new ArrayList<String>());
    }

    public P2jLogicalRelationalOperator getToP2jOperator() {
        return p2j;
    }
}
