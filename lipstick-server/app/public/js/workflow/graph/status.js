define(['knockout'], function(ko) {
    function Status(data) {
        var self = this;
        self.progress = ko.observable(data.progress ? data.progress : 0);    
        self.startTime = ko.observable(data.startTime);
        self.heartbeatTime = ko.observable(data.heartbeatTime);
        self.endTime = ko.observable(data.endTime);
        self.statusText = ko.observable(data.statusText);
        
        self.progressText = ko.computed(function() {
            if (self.progress()) {
                return self.progress()+"%";
            } else {
                return;
            }
        });
        self.progressCss = ko.computed(function() {
            switch(self.statusText())
            {
            case "finished":
                return 'progress-bar-success';
            case "failed":
                return 'progress-bar-danger';
            case "terminated":
                return 'progress-bar-warning';
            default:
                return 'progress-bar-striped';
            }
        });
    };
    return Status;
});
