package com.netflix.lipstick.adaptors;

import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

import org.apache.hadoop.hive.ql.plan.api.Operator;

public class HiveJsonAdaptor {
    protected final P2jLogicalRelationalOperator p2j;

    public HiveJsonAdaptor(Operator op) {
        this(op, new P2jLogicalRelationalOperator());
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

    public P2jLogicalRelationalOperator getToP2jOperator() {
        return p2j;
    }
}
