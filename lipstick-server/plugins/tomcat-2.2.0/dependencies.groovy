tomcatVersion = "7.0.30"

grails.project.work.dir = 'target'

grails.project.dependency.resolution = {

    inherits "global"
    log "warn"

    repositories {
        grailsCentral()
    }

    dependencies {
        runtime("org.apache.tomcat:tomcat-catalina-ant:$tomcatVersion") {
            excludes 'tomcat-catalina', 'tomcat-coyote'
        }
        compile "org.apache.tomcat.embed:tomcat-embed-core:$tomcatVersion"
        runtime "org.apache.tomcat.embed:tomcat-embed-jasper:$tomcatVersion"
        runtime "org.apache.tomcat.embed:tomcat-embed-logging-log4j:$tomcatVersion"
        runtime "org.apache.tomcat.embed:tomcat-embed-logging-juli:$tomcatVersion"

		// needed for JSP compilation
		runtime "org.eclipse.jdt.core.compiler:ecj:3.7.2"
		
        compile("org.grails:grails-plugin-tomcat:$grailsVersion") {
            excludes group: "org.grails", name: "grails-core"
            excludes group: "org.grails", name: "grails-bootstrap"
            excludes group: "org.grails", name: "grails-web"

        }
    }

    plugins {
        build(':release:2.2.0', ':rest-client-builder:1.0.3') {
            export = false
        }
    }
}
