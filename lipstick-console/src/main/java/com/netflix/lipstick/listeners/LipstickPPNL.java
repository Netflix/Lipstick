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
package com.netflix.lipstick.listeners;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LipstickPigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

import com.google.common.collect.Lists;
import com.netflix.lipstick.P2jPlanGenerator;
import com.netflix.lipstick.pigtolipstick.BasicP2LClient;
import com.netflix.lipstick.pigtolipstick.P2LClient;

/**
 * Lipstick Pig Progress notification listener.
 *
 * Manages initialization of lipstick clients and routing events to active
 * clients.
 *
 * @author jmagnusson
 * @author nbates
 *
 */
public class LipstickPPNL implements PigProgressNotificationListener {
    private static final Log LOG = LogFactory.getLog(LipstickPPNL.class);

    protected static final String LIPSTICK_UUID_PROP_NAME = "lipstick.uuid.prop.name";
    protected static final String LIPSTICK_UUID_PROP_DEFAULT = "lipstick.uuid";

    protected static final String LIPSTICK_URL_PROP = "lipstick.server.url";

    protected LipstickPigServer ps;
    protected PigContext context;
    protected List<P2LClient> clients = Lists.newLinkedList();
    protected List<PPNLErrorHandler> errorHandlers = Lists.newLinkedList();

    /**
     * Initialize a new LipstickPPNL object.
     */
    public LipstickPPNL() {
        LOG.info("--- Init TBPPNL ---");
    }

    public void addErrorHandler(PPNLErrorHandler errHandler) {
        errorHandlers.add(errHandler);
    }

    /**
     * Check if any clients are active.
     *
     * @return true, if at least one active client has been initialized
     */
    protected boolean clientIsActive() {
        if (clients != null && !clients.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Sets a reference to the pig server.
     *
     * @param ps
     *            the pig server
     */
    public void setPigServer(LipstickPigServer ps) {
        this.ps = ps;
        setPigContext(ps.getPigContext());
    }

    /**
     * Sets a reference to the pig context. Used if running
     * without a LipstickPigServer.
     *
     * @param ps
     *            the pig server
     */
    public void setPigContext(PigContext context) {
        this.context = context;
    }

    /**
     * Sets the plan generators. Initializes Lipstick clients if they have not
     * already been initialized.
     *
     * @param unoptimizedPlanGenerator
     *            the unoptimized plan generator
     * @param optimizedPlanGenerator
     *            the optimized plan generator
     */
    public void setPlanGenerators(P2jPlanGenerator unoptimizedPlanGenerator, P2jPlanGenerator optimizedPlanGenerator) {
        try {
            // this is the first time we can grab a conf from pig context so
            // initClients here
            initClients();

            if (clientIsActive()) {
                Properties props = context.getProperties();
                String uuidPropName = props.getProperty(LIPSTICK_UUID_PROP_NAME, LIPSTICK_UUID_PROP_DEFAULT);
                String uuid = props.getProperty(uuidPropName);
                if ((uuid == null) || uuid.isEmpty()) {
                    uuid = UUID.randomUUID().toString();
                    props.put(uuidPropName, uuid);
                }
                LOG.info("UUID: " + uuid);
                LOG.info(clients);
                for (P2LClient client : clients) {
                    client.setPlanGenerators(unoptimizedPlanGenerator, optimizedPlanGenerator);
                    client.setPigServer(ps);
                    client.setPigContext(context);
                    client.setPlanId(uuid);
                }

            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handlePlanGeneratorsError(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * initialPlanNotification(java.lang.String,
     * org.apache.pig.backend.hadoop.executionengine
     * .mapReduceLayer.plans.MROperPlan)
     */
    @Override
    public void initialPlanNotification(String scriptId, MROperPlan plan) {
        try {
            if (clientIsActive()) {
                for (P2LClient client : clients) {
                    client.createPlan(plan);
                }
            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handleInitialPlanNotificationError(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * launchStartedNotification(java.lang.String, int)
     */
    @Override
    public void launchStartedNotification(String scriptId, int numJobsToLaunch) {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * jobsSubmittedNotification(java.lang.String, int)
     */
    @Override
    public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * jobStartedNotification(java.lang.String, java.lang.String)
     */
    @Override
    public void jobStartedNotification(String scriptId, String assignedJobId) {
        try {
            if (clientIsActive()) {
                for (P2LClient client : clients) {
                    client.jobStarted(assignedJobId);
                }
            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handleJobStartedNotificationError(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * jobFinishedNotification(java.lang.String,
     * org.apache.pig.tools.pigstats.JobStats)
     */
    @Override
    public void jobFinishedNotification(String scriptId, JobStats jobStats) {
        try {
            if (clientIsActive()) {
                for (P2LClient client : clients) {
                    client.jobFinished(jobStats);
                }
            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handleJobFinishedNotificationError(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * jobFailedNotification(java.lang.String,
     * org.apache.pig.tools.pigstats.JobStats)
     */
    @Override
    public void jobFailedNotification(String scriptId, JobStats jobStats) {
        try {
            if (clientIsActive()) {
                for (P2LClient client : clients) {
                    client.jobFailed(jobStats);
                }
            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handleJobFailedNotificationError(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * outputCompletedNotification(java.lang.String,
     * org.apache.pig.tools.pigstats.OutputStats)
     */
    @Override
    public void outputCompletedNotification(String scriptId, OutputStats outputStats) {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * progressUpdatedNotification(java.lang.String, int)
     */
    @Override
    public void progressUpdatedNotification(String scriptId, int progress) {
        try {
            if (clientIsActive()) {
                for (P2LClient client : clients) {
                    client.updateProgress(progress);
                }
            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handleProgressUpdatedNotificationError(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.tools.pigstats.PigProgressNotificationListener#
     * launchCompletedNotification(java.lang.String, int)
     */
    @Override
    public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
        try {
            if (clientIsActive()) {
                for (P2LClient client : clients) {
                    client.planCompleted();
                }
            }
        } catch (Exception e) {
            LOG.error("Caught unexpected exception", e);
            for (PPNLErrorHandler errHandler : errorHandlers) {
                errHandler.handleLaunchCompletedNotificationError(e);
            }
        }
    }

    /**
     * Initialize the clients from properties in the pig context.
     */
    protected void initClients() {
        // Make sure client list is empty before initializing.
        // For example, this prevents initailizing multiple times when
        // executing multiple runs in a grunt shell session.
        Properties props = ps.getPigContext().getProperties();
        if (clients.isEmpty() && props.containsKey(LIPSTICK_URL_PROP)) {
            // Initialize the client
            clients.add(new BasicP2LClient(props.getProperty(LIPSTICK_URL_PROP)));
        }
    }
}
