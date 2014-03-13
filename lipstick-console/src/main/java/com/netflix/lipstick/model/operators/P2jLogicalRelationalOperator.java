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
package com.netflix.lipstick.model.operators;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Lob;
import javax.persistence.OneToMany;
import javax.persistence.Transient;

import org.apache.pig.parser.ParserException;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.lipstick.model.Utils;
import com.netflix.lipstick.model.operators.elements.JoinExpression;
import com.netflix.lipstick.model.operators.elements.SchemaElement;

/**
 * Base Lipstick model object for logical operators.
 *
 * @author jmagnusson
 *
 */
@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class P2jLogicalRelationalOperator {

    @Embeddable
    public static class Join {
        private Map<String, JoinExpression> expression;
        private String strategy;
        private String type;

        /**
         * Creates a default Join object.
         */
        public Join() {
        }

        /**
         * Creates a Join object with the given strategy, type, and expression map.
         *
         * @param strategy
         * @param type
         * @param expression
         */
        public Join(String strategy, String type, Map<String, List<String>> expression) {
            this.strategy = strategy;
            this.type = type;
            this.expression = Maps.newHashMap();
            for (Entry<String, List<String>> e : expression.entrySet()) {
                this.expression.put(e.getKey(), new JoinExpression(e.getValue()));
            }
        }

        @OneToMany(cascade = CascadeType.ALL)
        public Map<String, JoinExpression> getExpression() {
            return expression;
        }

        public void setExpression(Map<String, JoinExpression> expression) {
            this.expression = expression;
        }

        public String getStrategy() {
            return strategy;
        }

        public void setStrategy(String strategy) {
            this.strategy = strategy;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    @Embeddable
    public static class Location {
        private String filename = null;
        private Integer line = null;
        private List<String> macro = null;

        /**
         * Creates a default Location object.
         */
        public Location() {
        }

        /**
         * Creates a Location object with the given line, filename, and macro information.
         *
         * @param line
         * @param filename
         * @param macro
         */
        public Location(Integer line, String filename, List<String> macro) {
            this.line = line;
            this.filename = filename;
            this.macro = macro;
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public Integer getLine() {
            return line;
        }

        public void setLine(Integer line) {
            this.line = line;
        }

        @ElementCollection
        @CollectionTable(name = "StringCollection")
        public List<String> getMacro() {
            return macro;
        }

        public void setMacro(List<String> macro) {
            this.macro = macro;
        }
    }

    @Embeddable
    public static class MRStage {
        private String jobId = null;
        private String stepType = null;

        /**
         * Creates a default MRStage object.
         */
        public MRStage() {
        }

        /**
         * Creates a MRStage object with the given jobId and stepType.
         *
         * @param jobId
         * @param stepType
         */
        public MRStage(String jobId, String stepType) {
            this.jobId = jobId;
            this.stepType = stepType;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getStepType() {
            return stepType;
        }

        public void setStepType(String stepType) {
            this.stepType = stepType;
        }
    }

    private String alias;
    private long id;
    private Location location;
    private MRStage mapReduce;
    private String operator;
    private List<String> predecessors;
    private List<SchemaElement> schema;
    private String schemaString;
    private List<String> successors;
    private String uid;

    public String getAlias() {
        return alias;
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    public Location getLocation() {
        return location;
    }

    public MRStage getMapReduce() {
        return mapReduce;
    }

    public String getOperator() {
        return operator;
    }

    @ElementCollection
    public List<String> getPredecessors() {
        return predecessors;
    }

    @Transient
    public List<SchemaElement> getSchema() {
        return schema;
    }

    @Lob
    public String getSchemaString() {
        return schemaString;
    }

    @ElementCollection
    public List<String> getSuccessors() {
        return successors;
    }

    public String getUid() {
        return uid;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setId(long id) {
        this.id = id;
    }

    /**
     * Creates a Location object from the line, filename, and macro.  Assigns it as the
     * P2jLogicalRelationalOperator's location field.
     *
     * @param line
     * @param filename
     * @param macro
     */
    public void setLocation(int line, String filename, List<String> macro) {
        setLocation(new Location(line, filename, macro));
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public void setMapReduce(MRStage mapReduce) {
        this.mapReduce = mapReduce;
    }

    /**
     * Creates a MRStage object from the jobId and stepType.  Assigns it as the
     * P2jLogicalRelationalOperator's mapReduce field.
     *
     * @param jobId
     * @param stepType
     */
    public void setMapReduce(String jobId, String stepType) {
        setMapReduce(new MRStage(jobId, stepType));
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void setPredecessors(List<String> predecessors) {
        this.predecessors = predecessors;
    }

    /**
     * Sets the P2jLogicalRelationalOperator's schema to the passed in schema,
     * or to any empty list if the passed in schema is null.
     *
     * @param schema
     */
    public void setSchema(List<SchemaElement> schema) {
        if (schema == null) {
            schema = Lists.newArrayList();
        }
        this.schema = schema;
    }

    /**
     * Sets the P2jLogicalRelationalOperator's schemaString.
     *
     * @param schemaString
     */
    public void setSchemaString(String schemaString) {
        if (schemaString != null) {
            try {
                setSchema(Utils.processSchema(schemaString));
            } catch (ParserException e) {
                e.printStackTrace();
            }
        } else {
            setSchema(null);
        }
        this.schemaString = schemaString.replace(".", "_");
    }

    public void setSuccessors(List<String> successors) {
        this.successors = successors;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

}
