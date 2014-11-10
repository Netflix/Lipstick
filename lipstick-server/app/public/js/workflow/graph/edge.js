define(['knockout', 'lib/mustache', '../templates'],
       function(ko, Mustache, templates) {
           function Edge(data) {
               var self = this;
               self.type = ko.observable(data.type);
               self.u = ko.observable(data.u);
               self.v = ko.observable(data.v);
               self.label = ko.observable(data.label);
               self.properties = ko.observable(data.properties);
               self.title = ko.computed(function() {
                   return "<em>["+self.u()+"] -> ["+self.v()+"]</em>";
               });
               self.render = ko.computed(function() {
                   var template = templates.templates[self.type()];
                   if (template) {
                       return Mustache.render(template.template, new template.view(self.properties()));
                   }
                   return;
               });
           };
           return Edge;
       });
