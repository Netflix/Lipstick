define(['knockout', 'lib/mustache', './status', '../utils', '../templates'],
       function(ko, Mustache, Status, utils, templates) {
           
           function Node(data) {
               var self = this;
               self.status = ko.observable(new Status(data.status || {}));
               self.id = ko.observable(data.id);
               self.child = ko.observable(data.child);
               self.properties = ko.observable(data.properties);
               self.type = ko.observable(data.type);
               self.url = ko.observable(data.url);
               self.treeTable = ko.observableArray(
                   utils.sortTreeTable(utils.flatten(data.properties))
               );

               self.render = function() {
                   var template = templates.templates[self.type()];
                   if (template) {
                       return Mustache.render(template.template, new template.view(self.properties()));
                   } else {
                       return Mustache.render("<h1>{{id}}</h1>", self);
                   }
               };
           };         
           return Node;
       });
