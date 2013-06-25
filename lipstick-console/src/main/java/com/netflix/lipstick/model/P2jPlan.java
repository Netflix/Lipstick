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

import java.util.Map;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.OneToMany;

import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

/**
 * Container for a logical plan.
 *
 * @author jmagnusson
 *
 */
@Entity
public class P2jPlan {

    private long id;
    private Map<String, P2jLogicalRelationalOperator> plan = null;
    private String svg = null;

    /**
     * Construct an empty P2jPlan object.
     */
    public P2jPlan() {
    }

    /**
     * Construct a new P2jPlan object and set the plan.
     * @param plan
     */
    public P2jPlan(Map<String, P2jLogicalRelationalOperator> plan) {
        super();
        setPlan(plan);
    }

    /**
     * Construct a new P2jPlan object and set both the plan and svg representation of the plan.
     *
     * @param plan
     * @param svg
     */
    public P2jPlan(Map<String, P2jLogicalRelationalOperator> plan, String svg) {
        super();
        setPlan(plan);
        setSvg(svg);
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    /**
     * Get the logical plan.
     *
     * @return a representation of the logical plan as a map of uid to P2jLogicalRelationalOperator.
     */
    @OneToMany(cascade = CascadeType.ALL)
    public Map<String, P2jLogicalRelationalOperator> getPlan() {
        return plan;
    }

    /**
     * Get the graphical representation of the plan in svg format.
     *
     * @return a string containing the svg representation of the logical plan
     */
    @Lob
    public String getSvg() {
        return svg;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setPlan(Map<String, P2jLogicalRelationalOperator> plan) {
        this.plan = plan;
    }

    public void setSvg(String svg) {
        this.svg = svg;
    }
}
