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
/** tossboss-drawers.js
 * Responsible for drawing and maintaining the drawers.
 *
 * TRIGGERS EVENTS:
 * - closeDrawer.tossboss-drawer
 * - openDrawer.tossboss-drawer
 */

;Drawer = {
    options: {
        handleSize: 0,
        containerSize: 0,
        drawerSize: 0,
        navbarSize: 0,
        allHandleSel:    '#drawers .handle',
        topHandleSel:    '#top-drawer > .handle',
        rightHandleSel:  '#right-drawer > .handle',
        bottomHandleSel: '#bottom-drawer > .handle',
        leftHandleSel:   '#left-drawer > .handle',
        allContainerSel:    '#drawers .container',
        topContainerSel:    '#top-drawer > .container',
        rightContainerSel:  '#right-drawer > .container',
        bottomContainerSel: '#bottom-drawer > .container',
        leftContainerSel:   '#left-drawer > .container',
        allDrawerSel:    '#drawers > .drawer',
        topDrawerSel:    '#drawers > #top-drawer',
        rightDrawerSel:  '#drawers > #right-drawer',
        bottomDrawerSel: '#drawers > #bottom-drawer',
        leftDrawerSel:   '#drawers > #left-drawer',
        pageSel: '.page',
        objDefaults: {
            drawer: 'right-drawer',
            groupName: '',
            objectName: '',
            title: '',
            html: 'Hello World!',
        },
        drawersHtml: ' \
<div id="drawers"> \
    <div id="right-drawer" class="drawer toggle-transition"> \
        <div class="handle active"></div> \
        <div class="container"></div> \
    </div> \
    <div id="bottom-drawer" class="drawer toggle-transition"> \
        <div class="handle active"></div> \
        <div class="container"></div> \
    </div> \
    <div id="left-drawer" class="drawer toggle-transition"> \
        <div class="handle active"></div> \
        <div class="container"></div> \
    </div> \
</div> \
'
    },
    /**
     * Initialize the Drawer object.
     *
     * @param {Number} top If 1, activate top drawer
     * @param {Number} right If 1, activate right drawer
     * @param {Number} bottom If 1, activate bottom drawer
     * @param {Number} left If 1, activate left drawer
     */
    initialize: function(top, right, bottom, left) {
        // Add drawers HTML to page and get drawer sizes.
        $('body').prepend(Drawer.options.drawersHtml);
        Drawer.options.handleSize    = $(Drawer.options.rightHandleSel).outerWidth();
        Drawer.options.containerSize = $(Drawer.options.rightContainerSel).outerWidth();
        Drawer.options.drawerSize    = Drawer.options.handleSize + Drawer.options.containerSize;
        Drawer.options.navbarSize    = $('.navbar').outerHeight();
        // Activate drawers.
        if (top === 0) { $(Drawer.options.topHandleSel).removeClass('active'); }
        if (right === 0) { $(Drawer.options.rightHandleSel).removeClass('active'); }
        if (bottom === 0) { $(Drawer.options.bottomHandleSel).removeClass('active'); }
        if (left === 0) { $(Drawer.options.leftHandleSel).removeClass('active'); }
        // Bind events.
        $(Drawer.options.allHandleSel+'.active').on('click', function(e) {
            Drawer.toggleDrawer(e.currentTarget.parentElement.id);
        });
        // If any drawer is activated, bind event on window resize.
        if (top === 1 || right === 1 || bottom === 1 || left === 1) {
            $(window).resize(function(e) {
                var horizontalDrawerDifference = 0;
                var pageHorizontalPadding = parseFloat($(Drawer.options.pageSel).css('padding-left'))
                                          + parseFloat($(Drawer.options.pageSel).css('padding-right'));
                var pageVerticalPadding   = parseFloat($(Drawer.options.pageSel).css('padding-top'))
                                          + parseFloat($(Drawer.options.pageSel).css('padding-bottom'));
                var newPageHeight = window.innerHeight
                                  - pageVerticalPadding
                                  - Drawer.options.navbarSize
                                  - Drawer.options.handleSize;
                var newPageWidth  = window.innerWidth
                                  - pageHorizontalPadding
                                  - (Drawer.options.handleSize * 2);
                if ($(Drawer.options.topDrawerSel).hasClass('toggled')) {
                    newPageHeight -= Drawer.options.containerSize;
                }
                if ($(Drawer.options.bottomDrawerSel).hasClass('toggled')) {
                    newPageHeight -= Drawer.options.containerSize;
                }
                if ($(Drawer.options.leftDrawerSel).hasClass('toggled')) {
                    newPageWidth -= Drawer.options.containerSize;
                    horizontalDrawerDifference += Drawer.options.drawerSize;
                }
                if ($(Drawer.options.rightDrawerSel).hasClass('toggled')) {
                    newPageWidth -= Drawer.options.containerSize;
                    horizontalDrawerDifference += Drawer.options.drawerSize;
                }
                $(Drawer.options.topDrawerSel).css('width', (window.innerWidth - horizontalDrawerDifference)+'px');
                $(Drawer.options.bottomDrawerSel).css('width', (window.innerWidth - horizontalDrawerDifference)+'px');
                $(Drawer.options.pageSel).height(newPageHeight);
                $(Drawer.options.pageSel).width(newPageWidth);
            });
        }
        $(Drawer.options.allHandleSel+'.active').on('mouseenter', function(event) {
            $(this).addClass('mouseover');
        });
        $(Drawer.options.allHandleSel+'.active').on('mouseleave click', function(event) {
            $(this).removeClass('mouseover');
        });
    },
    /**
     * Set the intial display of drawers (opened or closed) at page load.
     *
     * @param {Number} top If 1, open top drawer at page load
     * @param {Number} right If 1, open right drawer at page load
     * @param {Number} bottom If 1, open bottom drawer at page load
     * @param {Number} left If 1, open left drawer at page load
     */
    initDisplay: function(top, right, bottom, left) {
        if (top === 1) { Drawer.toggleDrawer('top-drawer'); }
        if (right === 1) { Drawer.toggleDrawer('right-drawer'); }
        if (bottom === 1) { Drawer.toggleDrawer('bottom-drawer'); }
        if (left === 1) { Drawer.toggleDrawer('left-drawer'); }
    },
    /**
     * Get drawer selector information for a given drawer.
     *
     * @param {String} drawer The drawer to get information for ('top-drawer', 'right-drawer', 'bottom-drawer', or 'left-drawer')
     * @return {Object} Returns object containing selectors
     */
    getInfo: function(drawer) {
        drawer = drawer.toLowerCase();
        if (drawer === 'top-drawer') {
            return { 'handleSel':    Drawer.options.topHandleSel,
                     'containerSel': Drawer.options.topContainerSel,
                     'drawerSel':    Drawer.options.topDrawerSel,
                     'location':     'top' };
        }
        else if (drawer === 'right-drawer') {
            return { 'handleSel':    Drawer.options.rightHandleSel,
                     'containerSel': Drawer.options.rightContainerSel,
                     'drawerSel':    Drawer.options.rightDrawerSel,
                     'location':     'right' };
        }
        else if (drawer === 'bottom-drawer') {
            return { 'handleSel':    Drawer.options.bottomHandleSel,
                     'containerSel': Drawer.options.bottomContainerSel,
                     'drawerSel':    Drawer.options.bottomDrawerSel,
                     'location':     'bottom' };
        }
        else if (drawer === 'left-drawer') {
            return { 'handleSel':    Drawer.options.leftHandleSel,
                     'containerSel': Drawer.options.leftContainerSel,
                     'drawerSel':    Drawer.options.leftDrawerSel,
                     'location':     'left' };
        }
    },
    /**
     * Toggle (open or close) the drawer.
     *
     * @param {String} drawer The drawer to toggle ('top-drawer', 'right-drawer', 'bottom-drawer', or 'left-drawer')
     */
    toggleDrawer: function(drawer) {
        var page_container_height = $(Drawer.options.pageSel).height();
        var page_container_width  = $(Drawer.options.pageSel).width();
        var toggleDivs = Drawer.getInfo(drawer);
        $(toggleDivs.drawerSel).toggleClass('toggled');
        // Adjust the page div's height.
        if (toggleDivs.location == 'top' || toggleDivs.location == 'bottom') {
            if ($(toggleDivs.drawerSel).hasClass('toggled')) {
                $(Drawer.options.pageSel).height(page_container_height - Drawer.options.containerSize);
                if (toggleDivs.location == 'top') { $(Drawer.options.pageSel).css('top',Drawer.options.drawerSize+'px'); }
            }
            else {
                $(Drawer.options.pageSel).height(page_container_height + Drawer.options.containerSize);
                if (toggleDivs.location == 'top') { $(Drawer.options.pageSel).css('top',Drawer.options.navbarSize+'px'); }
            }
        }
        // Adjust the page div's width.
        else if (toggleDivs.location == 'left' || toggleDivs.location == 'right') {
            var bottomDrawerWidth = parseFloat($(Drawer.options.bottomDrawerSel).css('width'));
            if ($(toggleDivs.drawerSel).hasClass('toggled')) {
                $(Drawer.options.pageSel).width(page_container_width - Drawer.options.containerSize);
                if (toggleDivs.location == 'left') {
                    $(Drawer.options.pageSel).css('left',Drawer.options.drawerSize+'px');
                    $(Drawer.options.bottomDrawerSel).css('left',Drawer.options.drawerSize+'px');
                }
                // Adjust bottom drawer.
                bottomDrawerWidth -= Drawer.options.drawerSize;
                $(Drawer.options.bottomDrawerSel).css('width', bottomDrawerWidth+'px');
            }
            else {
                $(Drawer.options.pageSel).width(page_container_width + Drawer.options.containerSize);
                if (toggleDivs.location == 'left') {
                    $(Drawer.options.pageSel).css('left',Drawer.options.handleSize+'px');
                    $(Drawer.options.bottomDrawerSel).css('left','0px');
                }
                // Adjust bottom drawer.
                bottomDrawerWidth += Drawer.options.drawerSize;
                $(Drawer.options.bottomDrawerSel).css('width', bottomDrawerWidth+'px');
            }
        }
    },
    /**
     * Open drawer.
     *
     * @param {String} drawer The drawer to open ('top-drawer', 'right-drawer', 'bottom-drawer', or 'left-drawer')
     */
    openDrawer: function(drawer) {
        drawer = drawer.toLowerCase();
        var drawerSelects = Drawer.getInfo(drawer);
        if (drawerSelects) {
            if (!($(drawerSelects.drawerSel).hasClass('toggled'))) {
                Drawer.toggleDrawer(drawer);
            }
        }
        $(document).trigger('openDrawer.tossboss-drawer', [drawer]);
    },
    /**
     * Close drawer.
     *
     * @param {String} drawer The drawer to close ('top-drawer', 'right-drawer', 'bottom-drawer', or 'left-drawer')
     */
    closeDrawer: function(drawer) {
        drawer = drawer.toLowerCase();
        var drawerSelects = Drawer.getInfo(drawer);
        if (drawerSelects) {
            if ($(drawerSelects.drawerSel).hasClass('toggled')) {
                Drawer.toggleDrawer(drawer);
            }
        }
        $(document).trigger('closeDrawer.tossboss-drawer', [drawer]);
    },
    /**
     * Clear drawer contents.
     *
     * @param {String} drawer The drawer to clear ('top-drawer', 'right-drawer', 'bottom-drawer', or 'left-drawer')
     */
    clearDrawer: function(drawer) {
        var drawerSelects = Drawer.getInfo(drawer.toLowerCase());
        $(drawerSelects.containerSel).remove();
    }
};
