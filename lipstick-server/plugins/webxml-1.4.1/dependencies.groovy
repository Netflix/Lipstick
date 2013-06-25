grails.project.work.dir = 'target'
grails.project.docs.output.dir = 'docs/manual'

grails.project.dependency.resolution = {

	inherits 'global'
	log 'warn'

	repositories {
		grailsPlugins()
		grailsHome()
		grailsCentral()
	}

	dependencies {}

	plugins {
		build(':release:1.0.0.RC3') {
			export = false
		}
	}
}

