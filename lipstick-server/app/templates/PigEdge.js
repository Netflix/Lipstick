function PigEdge(properties) {
    var self = this;
    self.properties = properties;    
    self.hasSampleData = function() {
        if (self.properties.sampleOutput) {
            return true;
        } 
        return false;
    };

    self.hasSchema = function() {
        if (self.properties.schema) {
            return true;
        }
        return false;
    };

    self.sampleOutput = function() {
        var sol = self.properties.sampleOutput;
        var result = sol.map(function(so, i) {
            return {
                data: so.split("\n").map(function(record, j) {
                    return {fields: record.split("\u0001")};
                })
            };
        });
        return result;
    };

    self.schema = properties.schema;
}
