define(['knockout', './status', '../utils'], function(ko, Status, utils) {
    function NodeGroup(data) {
        var self = this;
        self.name = ko.observable(data.name || '');
        self.status = ko.observable(new Status(data.status || {}));
        self.properties = ko.observable(data.properties);
        self.children = ko.observableArray(data.children);
        self.id = ko.observable(data.id);
        self.immediateParent = ko.observable(data.immediateParent);
        self.treeTable = ko.observableArray(utils.flatten(data.properties));
    };
    return NodeGroup;
});
