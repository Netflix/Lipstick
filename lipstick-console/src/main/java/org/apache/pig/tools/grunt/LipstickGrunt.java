/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.tools.grunt;

import java.io.BufferedReader;

import org.apache.pig.LipstickPigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;

public class LipstickGrunt extends Grunt {

    public LipstickGrunt(BufferedReader in, PigContext pigContext) throws ExecException {
        super(in, pigContext);
        this.pig = new LipstickPigServer(pigContext);
        if (this.in != null) {
            parser = new GruntParser(this.in);
            parser.setParams(pig);
        }
    }
}