package grails.plugin.webxml

class FilterManager {

	static final int DEFAULT_POSITION = 500

	static final int CHAR_ENCODING_POSITION = 100
	static final int GRAILS_WEB_REQUEST_POSITION = 1000
	static final int RELOAD_POSITION = 2000
	static final int SITEMESH_POSITION = 3000
	static final int URL_MAPPING_POSITION = 4000

	Map<String, Integer> filterOrder = [
		charEncodingFilter: CHAR_ENCODING_POSITION,
		grailsWebRequest:   GRAILS_WEB_REQUEST_POSITION,
		reloadFilter:       RELOAD_POSITION,
		sitemesh:           SITEMESH_POSITION,
		urlMapping:         URL_MAPPING_POSITION]

	void registerWebXmlFilterPosition(String name, int position) {
		filterOrder[name] = position
	}
}
