$(function () { 
    $('#side-menu').metisMenu();
});

//Loads the correct sidebar on window load,
//collapses the sidebar on window resize.
// Sets the min-height of #page-wrapper to window size
$(function() {
    $(window).bind("load resize", function() {
        topOffset = 50;
        width = (this.window.innerWidth > 0) ? this.window.innerWidth : this.screen.width;
        if (width < 768) {
            $('div.navbar-collapse').addClass('collapse');
            topOffset = 100; // 2-row-menu
        } else {
            $('div.navbar-collapse').removeClass('collapse');
        }

        height = ((this.window.innerHeight > 0) ? this.window.innerHeight : this.screen.height) - 1;
        height = height - topOffset;
        if (height < 1) height = 1;
        if (height > topOffset) {
            $("#page-wrapper").css("min-height", (height) + "px");
        }
    });

    var url = window.location;
    var element = $('ul.nav a').filter(function() {
        return this.href == url || url.href.indexOf(this.href) == 0;
    }).addClass('active').parent().parent().addClass('in').parent();
    if (element.is('li')) {
        element.addClass('active');
    }
});

// This jQuery plugin will gather the comments within
// the current jQuery collection, returning all the
// comments in a new jQuery collection.
//
// NOTE: Comments are wrapped in DIV tags.

jQuery.fn.comments = function( blnDeep ){
    var blnDeep = (blnDeep || false);
    var jComments = $( [] );

    // Loop over each node to search its children for
    // comment nodes and element nodes (if deep search).
    this.each(
        function( intI, objNode ){
            var objChildNode = objNode.firstChild;
            var strParentID = $( this ).attr( "id" );

            // Keep looping over the top-level children
            // while we have a node to examine.
            while (objChildNode){

                // Check to see if this node is a comment.
                if (objChildNode.nodeType === 8){

                    // We found a comment node. Add it to
                    // the nodes collection wrapped in a
                    // DIV (as we may have HTML).
                    jComments = jComments.add(
                        "<div rel='" + strParentID + "'>" +
                        objChildNode.nodeValue +
                        "</div>"
                        );

                } else if (
                    blnDeep &&
                    (objChildNode.nodeType === 1)
                    ) {

                    // Traverse this node deeply.
                    jComments = jComments.add(
                        $( objChildNode ).comments( true )
                        );

                }

                // Move to the next sibling.
                objChildNode = objChildNode.nextSibling;

            }

        }
        );

    // Return the jQuery comments collection.
    return( jComments );
}
