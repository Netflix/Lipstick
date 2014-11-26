package com.netflix.lipstick.template;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;

public class Template {
    
    private static final Log LOG = LogFactory.getLog(Template.class);
    
    public String name;
    public String view;
    public String template;
    
    public Template() {    
    }
    
    public Template(String name) {
        this.name = name;        
    }
    
    public Template name(String name) {
        this.name = name;
        return this;
    }
    
    public Template view(String view) {
        this.view = view;
        return this;
    }
    
    public Template template(String template) {
        this.template = template;
        return this;
    }
    
    protected String readFile(String uri) throws IOException {
        return Files.toString(new File(uri), Charset.defaultCharset());
    }
    
    public Template loadView(String viewUri) throws IOException {
        this.view = readFile(viewUri);
        return this;
    }
    
    public Template loadTemplate(String templateUri) throws IOException {
        this.template = readFile(templateUri);
        return this;
    }
    
    public static Template fromJson(String json) {
        try {
            Template t = (new ObjectMapper()).readValue(json, Template.class);
            return t;
        } catch (IOException e) {
            LOG.error("Error deserializing Template", e);
        }
        return null;
    }
    
    public String toString() {
        String result = null;
        try {
            result = (new ObjectMapper()).writeValueAsString(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
