requirejs.config({
    baseUrl: 'js',
    paths: {
        lib: 'lib',
        bootstrap: '../bootstrap/js/bootstrap.3.1.1.min',
        jquery: 'jquery-1.8.2.min',
        transit: 'jquery.transit.min',
        knockout: 'lib/knockout-3.0.0',
        treetable: 'lib/jquery.treetable',
        d3: 'lib/d3.v3.min',
    },
    shim : {
        'backbone-min' : {
            deps : [ 'lodash.min', 'jquery' ],
            exports : 'Backbone'
        }
    }
});

requirejs(
    ['jquery', 'backbone-min', 'knockout',
     'workflow/templates', 'workflow/viewmodel'],
    function($, Backbone, ko, templates, viewmodel) {

        templates.initialize();
        
        var getJobInfo = function(uuid) {
            window.uuid = uuid;
            viewmodel.initialize(uuid);
            ko.applyBindings(viewmodel);
        };
        
        var TestRouter = Backbone.Router.extend({
            routes: {
                "job/:uuid": "getJobInfo",
                ":page/:uuid": "drawPageWithInfo"
            },
            getJobInfo: function(uuid) {
                getJobInfo(uuid);                       
                window.location.hash = '#graph/'+uuid;
            },
            drawPageWithInfo: function(page, uuid) {
                if (window.uuid != uuid) {
                    getJobInfo(uuid);
                }
                scrollTo(0,0);
                $('div .progress').show();
                $('div.page').hide();
                $('div.page#'+page).show();
                $('#graph-menu').width($('#graph-type button').width() * 2.45);
            }
        });
        router = new TestRouter();
        Backbone.history.start();
    }
);
