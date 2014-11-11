function PigNode(properties) {
    var self = this;
    self.properties = properties;
    self.operation = properties.operation;
    
    self.additional_info = function() {
        if (self.properties.macro) {
            return " (MACRO: "+self.properties.macro+")";
        }
        if (self.properties.limit) {
            return " ("+self.properties.limit+")";
        }
        if (self.properties.join &&
            self.properties.join.strategy &&
            self.properties.join.type) {
            var strategy = self.properties.join.strategy;
            var type = self.properties.join.type;
            return " ("+type+", "+strategy+")";
        }
    };

    self.step_type_color = function() {
        if (self.properties.step_type) {
            switch (self.properties.step_type.toLowerCase()) {
            case 'mapper':
                return '#3299BB';
            case 'reducer':
                return '#FF9900';
            case 'tez':
                return '#F5D04C';
            default:
                return '#BF0A0D';
            }
        } else {
            return '#BF0A0D';
        }
    };

    self.jg = function() {
        var j;
        if (self.properties.join) {
            j = self.properties.join;
        } else if (self.properties.group) {
            j = self.properties.group;
        }
        return j;
    }
                         
    self.join = function() {        
        var j = self.jg();
        if (j && j.by) {
            var relations = j.by.map(function(rel){ return rel.alias; });
            var fields = [];
            var to = j.by[0].fields.length;
            for (var i = 0; i < to; i++) {
                fields[i] = [];
                for (var k = 0; k < j.by.length; k++) {
                    var rel = j.by[k];
                    fields[i].push(rel.fields[i]);
                }
            }
            
            var result = {
                relations: relations,
                by: fields.map(function(field_list) { return {fields: field_list}; })
            };
            return result;
        }
        return;
    };
    
    self.colspan = function() {
        var j = self.jg();
        if (j && j.by) {
            return j.by.length;
        } else {
            return 2;
        }
    };
    
    self.alias = properties.alias;
    self.expression = properties.expression;
    self.storage_location = properties.storage_location;
    self.storage_function = properties.storage_function;
    
    self.schema = function() {
        if (self.properties.schema_equals_pred) {
            return;
        }            
        if (self.properties.schema) {            
            switch (self.operation) {
                case 'SPLIT':
                case 'FILTER':
                case 'DISTINCT':
                case 'GROUP':
                case 'COGROUP':
                case 'JOIN':
                case 'LIMIT':
                        return;
                default:
                        return {columns: self.properties.schema};
            }
        } else {
            return;
        }
    };
}
