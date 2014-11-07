define(['jquery'], function($) {
    var Utils = {
        
        //
        // Given an object, flattens it into a list to use
        // for a treetable. Importantly preserves parent-child
        // relationships with ids.
        //
        // eg.
        //
        //  flatten({a: {b: {c: [1,2,3]}}}) results in
        //  [
        //    {id:1, leaf:null, name:"a",parent:0},
        //    {id:2, leaf:null, name:"b",parent:1},
        //    {id:3, leaf:null, name:"c",parent:2},
        //    {id:4, leaf:1, name:0, parent:3},
        //    {id:6, leaf:2, name:1, parent:3},
        //    {id:8, leaf:3, name:2, parent:3}
        //  ]
        //
        flatten: function(object) {
            var collection = [];
            var id = 0;
            var self = this;
            
            self.visit = function(node, predecessor) {                
                if (Object(node) !== node) { // base type
                    self.visitBaseType(node, predecessor);
                } else if (Array.isArray(node)) {
                    self.visitArrayType(node, predecessor);
                } else {
                    self.visitObject(node, predecessor);
                } 
            };
            
            self.visitBaseType = function(node, predecessor) {
                id += 1;
                if (predecessor !== null) {
                    predecessor.leaf = node;
                    if (node === false) {predecessor.leaf = "false";}
                    if (node === null) {predecessor.visible = false;}
                }
            };

            self.visitArrayType = function(node, predecessor) {
                if (node.length > 0) {
                    for (var i = 0, l = node.length; i < l; i++) {
                        id += 1;
                        var row = {name: i, id: id, parent: predecessor.id, leaf: null, visible: true};
                        collection.push(row);
                        self.visit(node[i], row);
                    }
                } else {
                    if (predecessor !== null) {predecessor.visible = false;}
                }
            };

            self.visitObject = function(node, predecessor) {
                var successors = 0;
                for (var p in node) {
                    if (node.hasOwnProperty(p)) {
                        successors += 1;
                        id += 1;
                        var row = {name: p, id: id, parent: predecessor.id, leaf: null, visible: true};                            
                        collection.push(row);
                        self.visit(node[p], row); 
                    }
                }
                if (successors < 1 && predecessor !== null) {predecessor.visible = false;}
            };

            self.visit(object, {id:0});
            Utils.sortTreeTable(collection);
            return collection;
        },

        sortTreeTable: function(treetable) {
            var topNodes = [];
            var result = $.grep(treetable, function(e) {
                if (e.parent === 0 && e.leaf !== null) {
                    topNodes.push(e);
                    return false;
                }
                return true;
            });
            $.each(topNodes, function(i, e) {
                result.unshift(e);
            });
            return result;
        }
    };
    return Utils;
});
