import grails.util.Environment
import groovy.xml.StreamingMarkupBuilder
import groovy.xml.XmlUtil

import org.slf4j.Logger
import org.slf4j.LoggerFactory

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
 * History
 *   1.0 Roger Cass, Filter/BeanMapping
 *   1.1 ericacm - gmail.com, Listener
 *   1.2 Bob Schulze al.lias - web.de, Context Parameters
 *   1.3 Burt Beckwith; added new feature to order filter-mapping elements
 *   1.4 Stefano Gualdi; added patch from Daniel Bower to support session-timeout setting
 */
class WebxmlGrailsPlugin {

	private static final String DEFAULT_CONFIG_FILE = "DefaultWebXmlConfig"
	private static final String APP_CONFIG_FILE     = "WebXmlConfig"

	private Logger log = LoggerFactory.getLogger('grails.plugin.webxml.WebxmlGrailsPlugin')

	def version = "1.4.1"
	def grailsVersion = '1.2 > *'
	def author = "Roger Cass"
	def authorEmail = "roger.cass@byu.net"
	def title = "WebXmlConfig"
	def description = 'Add additional Features to your web.xml, such as Filters, Config Listeners or Context Parameter definitions'
	def documentation = "http://grails.org/plugin/webxml"

	def license = 'APACHE'
	def issueManagement = [system: 'JIRA', url: 'http://jira.grails.org/browse/GPWEBXML']
	def scm = [url: 'http://plugins.grails.org/grails-webxml/']
	def developers = [
		[name: "Eric Pederson",  email: "ericacm@gmail.com"],
		[name: "Bob Schulze",    email: "al.lias@gmx.de"],
		[name: "Burt Beckwith",  email: "beckwithb@vmware.com"],
		[name: "Stefano Gualdi", email: "stefano.gualdi@gmail.com"]
	]

	def doWithWebDescriptor = { xml ->
		def config = getConfig()
		if (!config) {
			return
		}

		if (config.filterChainProxyDelegator.add) {
			def contextParam = xml."context-param"
			contextParam[contextParam.size() - 1] + {
				'filter' {
					'filter-name'(config.filterChainProxyDelegator.filterName)
					'filter-class'(config.filterChainProxyDelegator.className)
					'init-param' {
						'param-name'('targetBeanName')
						'param-value'(config.filterChainProxyDelegator.targetBeanName)
					}
				}
			}

			def filter = xml."filter"
			filter[filter.size() - 1] + {
				'filter-mapping' {
					'filter-name'(config.filterChainProxyDelegator.filterName)
					'url-pattern'(config.filterChainProxyDelegator.urlPattern)
				}
			}
		}

		if (config.listener.add) {
			def listenerNode = xml."listener"

			for (String className in config.listener.classNames) {
				listenerNode[listenerNode.size() - 1] + {
					listener {
						'listener-class'(className)
					}
				}
			}
		}

		// add possibility for context params. As with the other features, the generated result
		// is a bit thin, it could contain the descriptions field too, a context.xml also would
		// be a good idea...  (bs)
		if (config.contextparams) {
			config.contextparams.each { String name, String value ->
				def contextParam = xml."context-param"
				contextParam[contextParam.size() - 1] + {
					'context-param' {
						'param-name'(name)
						'param-value'(value)
					}
				}
			}
		}

		//session Timeout
		if (config.sessionConfig.sessionTimeout instanceof Integer) {
			def contextParam = xml."context-param"
			contextParam[contextParam.size() - 1] + {
				'session-config'{
					'session-timeout'(config.sessionConfig.sessionTimeout)
				}
			}
		}

		if (log.isTraceEnabled()) {
			log.trace new StreamingMarkupBuilder().bind { out << xml }
		}
	}

	private getConfig() {
		GroovyClassLoader loader = new GroovyClassLoader(getClass().classLoader)

		def config
		try {
			def defaultConfigFile = loader.loadClass(DEFAULT_CONFIG_FILE)
			log.info "Loading default config file: $defaultConfigFile"
			config = new ConfigSlurper(Environment.current.name).parse(defaultConfigFile)

			try {
				def appConfigFile = loader.loadClass(APP_CONFIG_FILE)
				log.info "Found application config file: $appConfigFile"
				def appConfig = new ConfigSlurper(Environment.current.name).parse(appConfigFile)
				if (appConfig) {
					log.info "Merging application config file: $appConfigFile"
					config = config.merge(appConfig)
				}
			}
			catch (ClassNotFoundException e) {
				log.warn "Did not find application config file: $APP_CONFIG_FILE"
			}
		}
		catch (ClassNotFoundException e) {
			log.error "Did not find default config file: $DEFAULT_CONFIG_FILE"
		}

		config?.webxml
	}
}
