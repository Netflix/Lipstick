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

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * Model object for pig scripts.
 *
 * @author jmagnusson
 *
 */
@Entity
public class P2jScripts {
    private long id;
    private String script;

    /**
     * Create an empty P2jScripts.
     */
    public P2jScripts() {
    }

    /**
     * Create a new P2jScripts and set script.
     *
     * @param script a pig script
     */
    public P2jScripts(String script) {
        this.script = script;
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    @Lob
    public String getScript() {
        return script;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setScript(String script) {
        this.script = script;
    }

}
