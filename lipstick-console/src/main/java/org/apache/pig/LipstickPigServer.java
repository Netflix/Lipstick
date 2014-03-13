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
package org.apache.pig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import com.netflix.lipstick.P2jPlanGenerator;
import com.netflix.lipstick.listeners.LipstickPPNL;

/**
 * PigServer with extensions to support additional notifications to LipstickPPNL.
 *
 * @author jmagnusson
 *
 */
public class LipstickPigServer extends PigServer {

    protected Map<PhysicalOperator, Operator> p2lMap;
    protected LogicalPlan p2jLogicalPlan;
    protected LogicalPlan p2jOptimizedLogicalPlan;
    protected PhysicalPlan p2jPhysicalPlan;
    protected MROperPlan p2jMapReducePlan;
    protected LipstickPPNL ppnl = null;
    protected P2jPlanGenerator optimizedPlanGenerator;
    protected P2jPlanGenerator unoptimizedPlanGenerator;

    /**
     * Constructs a LipstickPigServer with the given execType and properties.
     * Initializes any LipstickPPNLs that the ScriptState is aware of.
     *
     * @param execType
     * @param properties
     * @throws ExecException
     */
    public LipstickPigServer(ExecType execType, Properties properties) throws ExecException {
        super(execType, properties);
        init();
    }

    /**
     * Constructs a LipstickPigServer with the given execType.
     * Initializes any LipstickPPNLs that the ScriptState is aware of.
     * @param execType
     * @throws ExecException
     */
    public LipstickPigServer(ExecType execType) throws ExecException {
        super(execType);
        init();
    }

    /**
     * Constructs a LipstickPigServer with the given context and connect settings.
     * Initializes any LipstickPPNLs that the ScriptState is aware of.
     * @param context
     * @param connect
     * @throws ExecException
     */
    public LipstickPigServer(PigContext context, boolean connect) throws ExecException {
        super(context, connect);
        init();
    }

    /**
     * Constructs a LipstickPigServer with the given context.
     * Initializes any LipstickPPNLs that the ScriptState is aware of.
     * @param context
     * @throws ExecException
     */
    public LipstickPigServer(PigContext context) throws ExecException {
        super(context);
        init();
    }

    /**
     * Constructs a LipstickPigServer with the given execTypeString.
     * Initializes any LipstickPPNLs that the ScriptState is aware of.
     * @param execTypeString
     * @throws ExecException
     * @throws IOException
     */
    public LipstickPigServer(String execTypeString) throws IOException {
        super(execTypeString);
        init();
    }

    /**
     * Initializes any LipstickPPNLs that the ScriptState is aware of.
     */
    protected void init() {
        List<PigProgressNotificationListener> listeners = ScriptState.get().getAllListeners();
        for (PigProgressNotificationListener l : listeners) {
            if (l instanceof LipstickPPNL) {
                ppnl = (LipstickPPNL) l;
                ppnl.setPigServer(this);
            }
        }
    }

    /**
     * Launches the given PhysicalPlan as well as sets the plan generators
     * for the LipstickPPNL(s).
     */
    @Override
    protected PigStats launchPlan(LogicalPlan lp, String jobName) throws ExecException, FrontendException {
        if (ppnl != null) {
            try {
                // Get preoptimized plan
                unoptimizedPlanGenerator = new P2jPlanGenerator(lp);

                // Get optimized plan by compiling it with the appropriate execution engine
                ((HExecutionEngine)getPigContext().getExecutionEngine()).compile(lp, getPigContext().getProperties());
                optimizedPlanGenerator = new P2jPlanGenerator(lp);
                
                ppnl.setPlanGenerators(unoptimizedPlanGenerator, optimizedPlanGenerator);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return super.launchPlan(lp, jobName);
    }

    /**
     * Returns the LogicalPlan contained in the current DAG with the given alias.
     *
     * @param alias
     * @return
     * @throws IOException
     */
    public LogicalPlan getLP(String alias) throws IOException {
        return getCurrentDAG().getPlan(alias);
    }

    public List<String> getScriptCache() {
        return getCurrentDAG().getScriptCache();
    }
}
