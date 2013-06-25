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
package com.netflix.lipstick.model;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.netflix.lipstick.model.P2jPlanStatus.StatusText;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;


public class PersistTest {
    EntityManagerFactory emf;
    EntityManager em;

    @BeforeClass
    public void beforeTests() {
        emf = Persistence.createEntityManagerFactory("pu");
        em = emf.createEntityManager();
    }

    @AfterClass
    public void afterTests() {
        em.clear();
        em.close();
    }

    @Test
    public void testP2jCounters() throws Exception {
        P2jCounters counters = new P2jCounters();
        counters.setId(0);
        counters.setCounters(new HashMap<String, Long>());

        P2jCounters counters2 = persistAndFind(counters);

        compare(counters, counters2);
    }

    @Test
    public void testP2jJobStatus() throws Exception {
        P2jJobStatus jobStatus = new P2jJobStatus();
        jobStatus.setCounters(new HashMap<String, P2jCounters>());
        jobStatus.setId(0);
        jobStatus.setIsComplete(true);
        jobStatus.setIsSuccessful(true);
        jobStatus.setJobId("someJobId");
        jobStatus.setJobName("someJobName");
        jobStatus.setMapProgress(1.0f);
        jobStatus.setReduceProgress(2.0f);
        jobStatus.setScope("someScope");
        jobStatus.setTotalMappers(5);
        jobStatus.setTotalReducers(10);
        jobStatus.setTrackingUrl("someTrackingUrl");

        P2jJobStatus jobStatus2 = persistAndFind(jobStatus);

        compare(jobStatus, jobStatus2);
    }

    @Test
    public void testP2jPlan() throws Exception {
        P2jPlan plan = getP2jPlan();
        P2jPlan plan2 = persistAndFind(plan);

        compare(plan, plan2);
    }

    @Test
    public void testP2jPlanPackage() throws Exception {
        P2jPlanPackage planPackage = new P2jPlanPackage();
        planPackage.setJobName("someJobName");
        planPackage.setOptimized(persistAndFind(getP2jPlan()));
        planPackage.setSampleOutputMap(new HashMap<String, P2jSampleOutputList>());
        planPackage.setScripts(persistAndFind(getP2jScripts()));
        planPackage.setStatus(persistAndFind(getP2jPlanStatus()));
        planPackage.setUnoptimized(persistAndFind(getP2jPlan()));
        planPackage.setUserName("someUserName");
        planPackage.setUuid("someUuid");

        P2jPlanPackage planPackage2 = persistAndFind(planPackage);

        compare(planPackage, planPackage2);
    }

    @Test
    public void testP2jPlanStatus() throws Exception {
        P2jPlanStatus planStatus = getP2jPlanStatus();
        P2jPlanStatus planStatus2 = persistAndFind(planStatus);

        compare(planStatus, planStatus2);
    }

    @Test
    public void testP2jSampleOutput() throws Exception {
        P2jSampleOutput sampleOutput = getP2jSampleOutput();
        P2jSampleOutput sampleOutput2 = persistAndFind(sampleOutput);

        compare(sampleOutput, sampleOutput2);
    }

    @Test
    public void testP2jSampleOutputList() throws Exception {
        P2jSampleOutputList sampleOutputList = new P2jSampleOutputList();
        sampleOutputList.setSampleOutputList(Lists.newArrayList(getP2jSampleOutput(), getP2jSampleOutput(), getP2jSampleOutput()));

        P2jSampleOutputList sampleOutputList2 = persistAndFind(sampleOutputList);

        compare(sampleOutputList, sampleOutputList2);
    }

    @Test
    public void testP2jScripts() throws Exception {
        P2jScripts scripts = getP2jScripts();
        P2jScripts scripts2 = persistAndFind(scripts);

        compare(scripts, scripts2);
    }

    private void compare(Object a, Object b) throws JsonGenerationException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        String strA = mapper.writeValueAsString(a);
        String strB = mapper.writeValueAsString(b);

        Assert.assertEquals(strA, strB);
    }

    @SuppressWarnings("unchecked")
    private <T> T persistAndFind(T obj) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        EntityTransaction et = em.getTransaction();
        et.begin();
        em.persist(obj);
        em.flush();
        et.commit();
        T ret = (T) em.find(obj.getClass(), determineId(obj));
        return ret;
    }

    private Object determineId(Object obj) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        for(Method m : obj.getClass().getDeclaredMethods()) {
            for(Annotation a : m.getAnnotations()) {
                if(a instanceof javax.persistence.Id) {
                    return m.invoke(obj, new Object[0]);
                }
            }
        }

        throw new RuntimeException("Unable to determine id for object.");
    }

    private P2jPlan getP2jPlan() {
        P2jPlan plan = new P2jPlan();
        plan.setPlan(new HashMap<String, P2jLogicalRelationalOperator>());
        plan.setSvg("someSvg");
        return plan;
    }

    private P2jScripts getP2jScripts() {
        P2jScripts scripts = new P2jScripts();
        scripts.setScript("someScript");
        return scripts;
    }

    private P2jPlanStatus getP2jPlanStatus() {
        P2jPlanStatus planStatus = new P2jPlanStatus();
        planStatus.setEndTime();
        planStatus.setHeartbeatTime();
        planStatus.setJobStatusMap(new HashMap<String, P2jJobStatus>());
        planStatus.setProgress(5);
        planStatus.setStartTime();
        planStatus.setStatusText(StatusText.finished);
        return planStatus;
    }

    private P2jSampleOutput getP2jSampleOutput() {
        P2jSampleOutput sampleOutput = new P2jSampleOutput();
        sampleOutput.setSampleOutput("someSampleOutput");
        sampleOutput.setSchemaString("someSchemaString");
        return sampleOutput;
    }
}
