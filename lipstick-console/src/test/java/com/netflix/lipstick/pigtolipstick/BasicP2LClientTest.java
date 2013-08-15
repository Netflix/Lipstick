

package com.netflix.lipstick.pigtolipstick;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

import org.apache.pig.LipstickPigServer;
import org.apache.pig.impl.PigContext;
import com.netflix.lipstick.P2jPlanGenerator;
import com.netflix.lipstick.pigstatus.PigStatusClient;
import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.newplan.Operator;

public class BasicP2LClientTest {

    /* Create Mock P2jPlanGenerator objects with methods stubbed out to
       let the BasicP2LClient.createPlan() method complete, and
       pass them to the client via the setter method. */
    public void setMockPlanGenerators(BasicP2LClient client) {
        P2jPlan pln = mock(P2jPlan.class);
        P2jPlanGenerator unopt_plan = mock(P2jPlanGenerator.class);
        when(unopt_plan.getP2jPlan()).thenReturn(pln);
        P2jPlanGenerator opt_plan = mock(P2jPlanGenerator.class);
        when(opt_plan.getP2jPlan()).thenReturn(pln);
        client.setPlanGenerators(unopt_plan, opt_plan);
    }

    public MROperPlan makeMockMROperPlan() {
        MROperPlan plan = mock(MROperPlan.class);
        when(plan.iterator()).thenReturn(new ArrayList<MapReduceOper>().iterator());
        return plan;
    }

    /* Create a Mock PigContext object with methods stubbed out to
        let the BasicP2LClient.createPlan() method complete */
    public void addMockPigContext(BasicP2LClient client) {
        HExecutionEngine exec_engine = mock(HExecutionEngine.class);
        when(exec_engine.getLogToPhyMap()).thenReturn(new HashMap<Operator, PhysicalOperator>());

        PigContext ctx = mock(PigContext.class);
        when(ctx.getExecutionEngine()).thenReturn(exec_engine);
        when(ctx.getProperties()).thenReturn(new Properties());
        client.setPigContext(ctx);
    }

    public void addMockPigServer(BasicP2LClient client) {
        LipstickPigServer server = mock(LipstickPigServer.class);
        client.setPigServer(server);
    }


    @Test
    public void testCreatePlanClientSaved() throws Exception {
        PigStatusClient status_client = mock(PigStatusClient.class);        
        BasicP2LClient client = new BasicP2LClient(status_client);

        addMockPigContext(client);
        addMockPigServer(client);
        setMockPlanGenerators(client);

        client.createPlan(makeMockMROperPlan());

        /* the client.savePlan() method should have been called */
        verify(status_client).savePlan(any(P2jPlanPackage.class));        
    }

    @Test
    public void testCreatePlanNoPigServerJustContext() throws Exception {
        PigStatusClient status_client = mock(PigStatusClient.class);        
        BasicP2LClient client = new BasicP2LClient(status_client);
        addMockPigContext(client);
        setMockPlanGenerators(client);

        client.createPlan(makeMockMROperPlan());

        /* the client.savePlan() method should have been called */
        verify(status_client).savePlan(any(P2jPlanPackage.class));        
    }
}
