define(['jquery'], function($) {

    var Template = function(data) {
        eval("this.view = "+data.view); // yup.
        this.name = data.name;
        this.template = data.template;
    };
    
    function WorkflowTemplates() {
        var self = this;
        self.baseUrl =  './template';
        self.templates = {};
        self.initialize = function() {
            $.ajax({
                type: 'GET',
                url:  self.baseUrl
            }).done(function(json) {
                $.each(json.templates, function (i, template) {
                    self.templates[template.name] = new Template(template) ;
                });
            }).fail(function() {
            });
        };
    };

    return new WorkflowTemplates();
});
