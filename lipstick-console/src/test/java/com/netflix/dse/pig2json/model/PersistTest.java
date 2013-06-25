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
package com.netflix.dse.pig2json.model;

import java.io.IOException;
import java.io.StringWriter;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlanStatus;

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
    public void persistPlan() throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        P2jPlanPackage p = mapper.readValue(PersistTest.class.getResourceAsStream("/test.json"), P2jPlanPackage.class);

        EntityTransaction et = em.getTransaction();
        et.begin();
        em.persist(p);
        em.flush();
        et.commit();
        P2jPlanPackage p2 = em.find(P2jPlanPackage.class, p.getId());

        String j = mapper.writeValueAsString(p);
        String j2 = mapper.writeValueAsString(p2);
        Assert.assertEquals(j2, j);
    }

    @Test
    public void persistStatus() throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        IOUtils.copy(PersistTest.class.getResourceAsStream("/status.json"), writer, "UTF-8");
        P2jPlanStatus p = mapper.readValue(PersistTest.class.getResourceAsStream("/status.json"), P2jPlanStatus.class);

        EntityTransaction et = em.getTransaction();
        et.begin();
        em.persist(p);
        em.flush();
        et.commit();
        P2jPlanStatus p2 = em.find(P2jPlanStatus.class, p.getId());

        String j = mapper.writeValueAsString(p);
        String j2 = mapper.writeValueAsString(p2);
        Assert.assertEquals(j2, j);
    }
}