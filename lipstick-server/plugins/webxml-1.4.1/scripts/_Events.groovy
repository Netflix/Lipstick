import grails.util.GrailsUtil
import groovy.xml.DOMBuilder
import groovy.xml.XmlUtil
import groovy.xml.dom.DOMCategory

/**
 * Re-write the web.xml to order the servlet filters the way we need
 */
eventWebXmlEnd = { String filename ->
	try {
		fixWebXml()
	}
	catch (e) {
		GrailsUtil.deepSanitize e
		e.printStackTrace()
	}
}

private void fixWebXml() {

	def wxml = DOMBuilder.parse(new StringReader(webXmlFile.text)).documentElement

	def FilterManager = classLoader.loadClass('grails.plugin.webxml.FilterManager')
	def filterManager = FilterManager.newInstance()

	for (plugin in pluginManager.allPlugins) {
		plugin.instance.properties.webXmlFilterOrder.each { k, v ->
			filterManager.registerWebXmlFilterPosition k, v
		}
	}

	def sorted = new TreeMap()
	def defaultPositionNames = []
	sorted[FilterManager.DEFAULT_POSITION] = defaultPositionNames

	def orderedNames = []
	filterManager.filterOrder.each { k, v ->
		// invert the map; new map key is int (order) and value is list of names for that order
		def list = sorted[v]
		if (!list) {
			list = []
		}
		list << k
		orderedNames << k
		sorted[v] = list
	}

	for (String name in findFilterMappingNames(wxml)) {
		if (!orderedNames.contains(name)) {
			defaultPositionNames << name
		}
	}
	def orderedFilterNames = (sorted.values() as List).flatten()

	sortFilterMappingNodes(wxml, orderedFilterNames)

	webXmlFile.withWriter { it << XmlUtil.serialize(wxml) }
}

private Set<String> findFilterMappingNames(dom) {
	Set names = []

	use (DOMCategory) {
		def mappingNodes = dom.'filter-mapping'
		mappingNodes.each { n ->
			names << n.'filter-name'.text()
		}
	}

	names
}

private void sortFilterMappingNodes(dom, orderedFilterNames) {
	def sortedMappingNodes = []
	def followingNode

	use (DOMCategory) {
		def mappingNodes = dom.'filter-mapping'
		if (mappingNodes.size()) {
			followingNode = mappingNodes[-1].nextSibling

			Set doneFilters = []
			orderedFilterNames.each { f ->
				mappingNodes.each { n ->
					def filterName = n.'filter-name'.text()
					if (!(filterName in doneFilters)) {
						if (filterName == f || (f == '*' && !orderedFilterNames.contains(filterName))) {
							sortedMappingNodes << n
							doneFilters << n
						}
					}
				}
			}

			mappingNodes.each { dom.removeChild(it) }
		}
	}

	sortedMappingNodes.each { dom.insertBefore(it, followingNode) }
}
