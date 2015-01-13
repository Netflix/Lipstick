define(['knockout', './status'], function(ko, Status) {
    function Stage(data) {
        var self = this;
        self.name = ko.observable(data.name || '');    
        self.status = ko.observable(new Status(data.status || {}));
    };
    return Stage;
});
