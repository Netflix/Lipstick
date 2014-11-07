define(['knockout'], function(ko) {
    function Status(data) {
        var self = this;
        self.progress = ko.observable(data.progress ? data.progress : 0);    
        self.startTime = ko.observable(data.startTime);
        self.heartbeatTime = ko.observable(data.heartbeatTime);
        self.endTime = ko.observable(data.endTime);
        self.statusText = ko.observable(data.statusText);

        self.timeString = function(ms) {
            var d = new Date(ms);
            return d.toLocaleDateString()+' '+d.toLocaleTimeString();
        };

        self.startTimeString = ko.computed(function() {
            return self.timeString(self.startTime());
        });

        self.endTimeString = ko.computed(function() {
            if (self.endTime() && self.endTime() > 0) {
                return self.timeString(self.endTime());
            } else {
                return;
            }
        });

        self.heartbeatTimeString = ko.computed(function() {
            return self.timeString(self.heartbeatTime());
        });
        
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
        self.updateWith = function(data) {
            if (data) {
                if (data.progress) {self.progress(data.progress);}
                if (data.startTime) {self.startTime(data.startTime);}
                if (data.endTime) {self.endTime(data.endTime);}
                if (data.heartbeatTime) {self.heartbeatTime(data.heartbeatTime);}
                if (data.statusText) {self.statusText(data.statusText);}
            }
        };
    };
    return Status;
});
