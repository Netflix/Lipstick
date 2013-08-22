/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/** tossboss-modal.js
 * Responsible for the initialization of objects and the page.
 * 
 * LISTENS FOR EVENTS:
 * - loadGraphModel.tossboss-graph-model
 * 
 * TIGGERS EVENTS:
 */

;Modal = {
    options: {
        modalDefaults: {
            title: '',
            html: '<img class="center" src="./images/loading.gif"/>'
        }
    },
    /**
     * Start all custom event listeners.
     */
    startListeners: function() {
        // On getting graph data from GraphModel, show menus.
        $(document).on('loadGraphModel.tossboss-graph-model', function(event) {
            $('.zoom-menu').show();
            $('.graph-type-menu').show();
        });
    },
    /**
     * Initialize the Modal object.
     */
    initialize: function() {
        Modal.startListeners();
    },
    /**
     * Add and display a modal.
     * Example: displayModal(options)
     * options: {
     *     title: '',
     *     html: ''
     * }
     *
     * options.title = modal title
     * options.html = html markup for modal body
     *
     * @param {Object} options The options for the modal
     */
    displayModal: function(options) {
        var opts = $.extend({}, Modal.options.modalDefaults, options);
        $('body').prepend(_.template(Templates.modalTmpl, opts, {variable:'data'}));
        $('#myModal').css({
            'width': function () {
                return ($(document).width() * .9) + 'px';
            },
            'margin-left': function () {
                return -($(this).width() / 2);
            }
        });
        $('#myModal').modal('toggle');
        $('#myModal').on('hidden', function(event) {
            $('#myModal').remove();
        });
    }
};
