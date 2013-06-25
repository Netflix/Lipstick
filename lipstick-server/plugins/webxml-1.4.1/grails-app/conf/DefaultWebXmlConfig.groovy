/**
 * Copyright 2008 Roger Cass (roger.cass@byu.net)
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
 *
 * Default configuration file for WebXml plugin.
 *
 * To override values, create a file called YOUR-APP/grails-app/conf/WebXmlConfig.groovy
 */
webxml {
	filterChainProxyDelegator.add = false
	filterChainProxyDelegator.targetBeanName = "filterChainProxyDelegate"
	filterChainProxyDelegator.urlPattern = "/*"
	filterChainProxyDelegator.filterName = "filterChainProxyDelegator"
	filterChainProxyDelegator.className = "org.springframework.web.filter.DelegatingFilterProxy"

	listener.add = false
	//listener.classNames = ["org.springframework.web.context.request.RequestContextListener"]

	contextparams = [sample: 'Sample Value']

//	sessionConfig.sessionTimeout = 30
}
