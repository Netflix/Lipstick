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

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Lipstick model object representing an individual field in a schema.
 *
 * @author jmagnusson
 *
 */
public class SchemaElement {
    private String alias = null;
    private String type = null;
    private List<SchemaElement> schemaElements = null;


    /**
     * Construct an empty SchemaElement.
     */
    public SchemaElement() {
    }

    /**
     * Construct a new schema element.
     *
     * @param alias element alias
     * @param type element type
     * @param uid uid of this element
     * @param schemaElements a schema embedded in this element, expressed as a list of SchemaElements
     */
    public SchemaElement(String alias, String type, Long uid, List<SchemaElement> schemaElements) {
        this.alias = alias;
        this.type = type;
        this.schemaElements = schemaElements;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("schema")
    public List<SchemaElement> getSchemaElements() {
        return schemaElements;
    }

    @JsonProperty("schema")
    public void setSchemaElements(List<SchemaElement> schemaElements) {
        this.schemaElements = schemaElements;
    }

}
