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

import javax.persistence.Column;
import javax.persistence.Entity;

/**
 * Lipstick model object for LOStore operator.
 *
 * @author jmagnusson
 *
 */
@Entity
public class P2jLOStore extends P2jLogicalRelationalOperator {
    @Column(length = 2048)
    public String getStorageLocation() {
        return storageLocation;
    }

    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    public String getStorageFunction() {
        return storageFunction;
    }

    public void setStorageFunction(String storageFunction) {
        this.storageFunction = storageFunction;
    }

    private String storageLocation;
    private String storageFunction;
}
