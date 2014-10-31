define(function() {
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
        flatten: function(obj) {
            var result = [];
            var id = 0;
            function recurse(cur, parentId, parent) {
                if (Object(cur) !== cur) {
                    id += 1;
                    if (parent !== null) {
                        parent.leaf = cur;
                    }
                } else if (Array.isArray(cur)) {
                    for (var i = 0, l = cur.length; i < l; i++) {
                        id += 1;
                        var row = {name: i, id: id, parent: parentId, leaf: null};
                        result.push(row);
                        recurse(cur[i], id, row);
                    }
                } else {            
                    for (var p in cur) {
                        if (cur.hasOwnProperty(p)) {
                            id += 1;
                            var row = {name: p, id: id, parent: parentId, leaf: null};
                            result.push(row);
                            recurse(cur[p], id, row); 
                        }
                    }
                }
            }
            recurse(obj, id, null);
            return result;
        }
    };
    return Utils;
});
