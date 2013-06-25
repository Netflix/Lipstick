target(createWebXmlConfig: 'Creates WebXmlConfig.groovy') {

	String destination = "$basedir/grails-app/conf/WebXmlConfig.groovy"
	if (new File(destination).exists()) {
		println '\ngrails-app/conf/WebXmlConfig.groovy exists, not overwriting\n'
		return
	}

	ant.copy file:   "$webxmlPluginDir/src/samples/_WebXmlConfig.groovy",
				tofile: destination
	println '\nCreated grails-app/conf/WebXmlConfig.groovy\n'
}

setDefaultTarget 'createWebXmlConfig'
