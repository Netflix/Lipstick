package com.netflix.lipstick.adaptors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.data.DataType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

/**
 * Translates a PhysicalPlan to a map of p2jLogicalRelationalOperators.
 * 
 * @author jacobp
 *
 */
public class POJsonAdaptor {
	
	private String jobId;
	private Long startId;
	private PhysicalPlan pp;
	private Map<PhysicalOperator, String> reverseMap;
	private Map<String, P2jLogicalRelationalOperator> plan;
	
	public POJsonAdaptor(String jobId, Long startId, PhysicalPlan pp) {
		this.jobId = jobId;
		this.startId = startId;
		this.pp = pp;
		this.reverseMap = generateReverseMap();
		this.plan = translate();
	}
	
	public Map<String, P2jLogicalRelationalOperator> getPlan() {
		return plan;
	}
	
	protected Map<String, P2jLogicalRelationalOperator> translate() {
		Map<String, P2jLogicalRelationalOperator> result = Maps.newHashMap();
		for (PhysicalOperator op : pp) {
			String id = reverseMap.get(op);
			result.put(id, translateOperator(id, op));
		}
 		return result;
	}
	
	protected Map<PhysicalOperator, String> generateReverseMap() {
		Map<PhysicalOperator, String> map = Maps.newLinkedHashMap();
        Long counter = startId;
        for (PhysicalOperator op : pp) {
        	map.put(op, counter.toString());
            counter++;
        }
        return map;
	}
	
	public static String getTypeString(byte type) {
	    String result = null;
	    switch (type) {	        
	    case DataType.TUPLE:
	        result = "tuple()";
	        break;
	    case DataType.BAG:
	        result = "bag{}";
	        break;
	    default:
	        result = DataType.findTypeName(type);
	        break;
	    }
	    return result;
	}
	
	public static String translateInnerPlan(PhysicalPlan plan, Set<String> aliases) {
	    StringBuilder result = new StringBuilder();
	    if (plan.getLeaves().size() > 0) {
	        PhysicalOperator op = plan.getLeaves().get(0);
	        String alias = null;
	        if (op instanceof POUserFunc) {	            
	            alias = ((POUserFunc)op).getFunc().getClass().getName();
	        } else {
	            alias = op.getClass().getSimpleName();
	        }
	        alias = alias.replaceAll("\\.|\\$", "_");
	        if (!aliases.add(alias)) {
	            int i = 0;
	            while (!aliases.add(alias+i)) {
	                i++;
	            }
	            alias = alias+i;
	        }
	        result.append(alias);
	        result.append(":");
	        result.append(getTypeString(op.getResultType()));
	    }
	    return result.toString();
	}
	
	public static P2jLogicalRelationalOperator translateOp(String jobId, String id, PhysicalOperator op, List<String> successors, List<String> predecessors) {
	    P2jLogicalRelationalOperator p2jOp = new P2jLogicalRelationalOperator();
		p2jOp.setUid(id);
		p2jOp.setAlias(op.getAlias() != null ? op.getAlias() : "{tez-inserted-op}");		
		p2jOp.setOperator(op.getClass().getSimpleName());
		p2jOp.setMapReduce(jobId, "TezVertexInserted");
		p2jOp.setLocation(new P2jLogicalRelationalOperator.Location(0, "", new ArrayList<String>()));
		if (op instanceof POForEach) {
		    List<PhysicalPlan> innerPlans = ((POForEach)op).getInputPlans();
		    StringBuilder sb = new StringBuilder();
		    sb.append("(");
		    Iterator<PhysicalPlan> it = innerPlans.iterator();
		    Set<String> aliases = Sets.newHashSet();
		    while (it.hasNext()) {
		        PhysicalPlan innerPlan = it.next();
		        sb.append(translateInnerPlan(innerPlan, aliases));
		        if (it.hasNext()) {
		            sb.append(",");
		        }
		    }
		    sb.append(")");
		    p2jOp.setSchemaString(sb.toString());
		}
		
		p2jOp.setSuccessors(successors);
		p2jOp.setPredecessors(predecessors);
		
		return p2jOp;
	}
	
	protected P2jLogicalRelationalOperator translateOperator(String id, PhysicalOperator op) {
	    List<String> successors = Lists.newArrayList();
		List<String> predecessors = Lists.newArrayList();
		if (pp.getSuccessors(op) != null) {
			for (PhysicalOperator po : pp.getSuccessors(op)) {
				successors.add(reverseMap.get(po));
			}
		}
		if (pp.getPredecessors(op) != null) {
			for (PhysicalOperator po : pp.getPredecessors(op)) {
				predecessors.add(reverseMap.get(po));
			}
		}
		return translateOp(jobId, id, op, successors, predecessors);
	}
	
	public List<String> getSinks() {
		List<String> sinks = Lists.newArrayList();
		for (PhysicalOperator op : pp.getLeaves()) {			
			sinks.add(reverseMap.get(op));
		}
		return sinks;
	}
	
	public List<String> getSources() {
		List<String> sources = Lists.newArrayList();
		for (PhysicalOperator op : pp.getRoots()) {			
			sources.add(reverseMap.get(op));
		}
		return sources;
	}
}
