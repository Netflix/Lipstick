define(['knockout', './status', './stage', '../utils'], function(ko, Status, Stage, utils) {
    function NodeGroup(data) {
        var self = this;
        self.url = ko.observable(data.url);
        self.name = ko.observable(data.name || '');
        self.status = ko.observable(new Status(data.status || {}));
        self.properties = ko.observable(data.properties);
        self.children = ko.observableArray(data.children);
        self.id = ko.observable(data.id);
        self.immediateParent = ko.observable(data.immediateParent);
        self.treeTable = ko.observableArray(
            utils.sortTreeTable(utils.flatten(data.properties))
        );

        self.stages = ko.observableArray((data.stages ? data.stages : []).map(
            function(stage) {
                return new Stage(stage);
            }
        ));
        
        self.title = ko.computed(function() {
            if (self.url()) {
                // HACK - handles legacy MRJob case
                if (self.properties() && self.properties().jobId)
                    return '<a href="'+self.url()+'">'+self.properties().jobId+'</a>';
                else {
                    return '<a href="'+self.url()+'">'+self.name()+'</a>';
                }
            } else if (self.properties() && self.properties().jobId) {
                return self.properties().jobId;
            } else {
                return self.name();
            }
        });
     
    };
    return NodeGroup;
});
