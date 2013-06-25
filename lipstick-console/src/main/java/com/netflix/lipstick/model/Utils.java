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

import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.ParserException;

import com.google.common.collect.Lists;
import com.netflix.lipstick.model.operators.elements.SchemaElement;

/**
 * Utilities related to the lipstick model.
 *
 * @author jmagnusson
 */
public final class Utils {

    /** Mapping of type id to string type. */
    protected static final Map<Byte, String> TYPESMAP = DataType.genTypeToNameMap();

    private Utils() { }

    /**
     * Produces a list of Lipstick SchemaElements from a string representation of a schema.
     *
     * @param schemaString the schema string
     * @return a list of schema elements
     * @throws ParserException the parser exception
     */
    public static List<SchemaElement> processSchema(String schemaString) throws ParserException {
        String tempString = schemaString.replace(".", "_");
        tempString = tempString.substring(1, tempString.length() - 1);
        Schema temp = org.apache.pig.impl.util.Utils.getSchemaFromString(tempString);
        return processSchema(Util.translateSchema(temp));
    }

    /**
     * Produces a list of Lipstick SchemaElements given a LogicalSchema.
     *
     * @param src the LogicalSchema
     * @return a list of SchemaElements
     */
    public static List<SchemaElement> processSchema(LogicalSchema src) {

        if (src == null) {
            return null;
        }

        List<SchemaElement> schemaList = Lists.newArrayList();
        List<LogicalFieldSchema> fields = src.getFields();
        for (LogicalFieldSchema f : fields) {
            SchemaElement ele = new SchemaElement();
            if (f.alias != null) {
                ele.setAlias(f.alias.toString());
            }
            ele.setType(TYPESMAP.get(f.type));
            if (f.schema != null) {
                ele.setSchemaElements(processSchema(f.schema));
            }
            schemaList.add(ele);
        }
        return schemaList;
    }

}
