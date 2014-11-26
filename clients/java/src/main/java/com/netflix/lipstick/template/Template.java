package com.netflix.lipstick.template;

public class Template {
    public String name;
    public String view;
    public String template;
    
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
    
    public Template loadView(String viewUri) {
        // read file
        return this;
    }
    
    public Template loadTemplate(String templateUri) {
        // read file
        return this;
    }
}
