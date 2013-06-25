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
package com.netflix.lipstick.model.operators.elements;

import java.util.List;

import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * Lipstick model object representing a join expression.
 *
 * @author jmagnusson
 *
 */
@Entity
public class JoinExpression {
    private List<String> fields;
    private long id;

    /**
     * Construct an empty JoinExpression object.
     */
    public JoinExpression() {
    }

    /**
     * Construct a new JoinExpression object and set fields attribute.
     * @param fields the list of fields being joined
     */
    public JoinExpression(List<String> fields) {
        this.fields = fields;
    }

    @ElementCollection
    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
