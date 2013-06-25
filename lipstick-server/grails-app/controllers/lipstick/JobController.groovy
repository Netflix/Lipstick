/*
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
package lipstick

import grails.converters.*

class JobController {
    def planService

    def index() {
        if (request.method == 'GET') {
            //GET is handled separately because we use jackson for the plans
            if (!params.id) {
                def res = planService.list(params)
                render res as JSON
                return
            }
            def ret =  planService.get(params)
            if (!ret) {
                response.status = 404
                def res = ["error": String.format("plan (%s) not found", params.id)]
                render res as JSON
                return
            }
            render(contentType: 'application/json', text: ret)
        } else {
            render _process(params, request, response) as JSON
        }
    }
	
	def addSampleOutput() {
		def ret = planService.addSampleOutput(params.id, params.jobId, request.reader.text)
        if (ret.error) {
            response.status = 500
        }
        render ret as JSON
	}

    def _process = { params, request, response ->
        switch(request.method){
            case "POST":
                def ret = planService.save(params, request.reader.text)
                if (ret.error) {
                    response.status = 500
                }
                return ret
            case "PUT":
                if (!params.id) {
                    response.status = 400
                    def res =  ["error": "a uuid must be specified"]
                    return res
                }
                def ret = planService.update(params, request.reader.text)
                if (!ret) {
                    response.status = 404
                    def res = ["error": String.format("plan (%s) not found", params.id)]
                    return res
                }
                return ret
            case "DELETE":
                response.status = 405
                return ["error":"service does not support delete currently"]
                //return planService.delete(params.id)
        }
    }
}
