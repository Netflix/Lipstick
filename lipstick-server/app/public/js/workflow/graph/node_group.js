define(['knockout', './status', '../utils'], function(ko, Status, utils) {
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

        self.title = ko.computed(function() {
            if (self.url()) {
                // HACK - handles legacy MRJob case
                if (self.properties().jobId)
                    return '<a href="'+self.url()+'">'+self.properties().jobId+'</a>';
                else {
                    return '<a href="'+self.url()+'">'+self.name()+'</a>';
                }
            } else {
                return self.name();
            }
        });
    };
    return NodeGroup;
});
