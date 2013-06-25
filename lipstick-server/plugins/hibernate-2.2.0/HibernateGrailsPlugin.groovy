/*
 * Copyright 2004-2005 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.codehaus.groovy.grails.commons.AnnotationDomainClassArtefactHandler
import org.codehaus.groovy.grails.plugins.orm.hibernate.HibernatePluginSupport

/**
 * Handles the configuration of Hibernate within Grails.
 *
 * @author Graeme Rocher
 * @since 0.4
 */
class HibernateGrailsPlugin {
    def author = "Graeme Rocher"
    def title = "Hibernate for Grails"
    def description = "Provides integration between Grails and Hibernate through GORM"

    def grailsVersion = "2.2 > *"
    def version = "2.2.0"
    def documentation = "http://grails.org/doc/$version"
    def observe = ['domainClass']

    def dependsOn = [dataSource: "2.2 > *",
                     i18n: "2.2 > *",
                     core: "2.2 > *",
                     domainClass: "2.2 > *"]

    def loadAfter = ['controllers', 'domainClass']

    def watchedResources = ["file:./grails-app/conf/hibernate/**.xml"]

    def artefacts = [new AnnotationDomainClassArtefactHandler()]

    def doWithSpring = HibernatePluginSupport.doWithSpring

    def doWithDynamicMethods = HibernatePluginSupport.doWithDynamicMethods

    def onChange = HibernatePluginSupport.onChange
}
