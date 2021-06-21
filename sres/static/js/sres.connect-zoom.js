// Searcher
$(document).on('keyup', '#connection_filter', function(){
    var term = $(this).val();
    if (term) {
        term = term.toLowerCase();
        $('.sres-card-container').each(function(){
            let haystack = [];
            haystack.push($(this).find('.sres-zoom-meetings option:selected').text());
            haystack.push($(this).find('.sres-import-destination-select option:selected').text());
            if (haystack.join(' ').toLowerCase().indexOf(term) !== -1) {
                $(this).removeClass('d-none');
            } else {
                $(this).addClass('d-none');
            }
        });
    } else {
        $('.sres-card-container').removeClass('d-none');
    }
});

/**
    Connections manipulation
**/
// Restore connections if available
$(document).ready(function(){
    if (sresZoomConnections.length > 0) {
        for (let i = 0; i < sresZoomConnections.length; i++) {
            let config = sresZoomConnections[i];
            //console.log('triggering new connection', i, config);
            $('.sres-connection-new').trigger('click', { existingConnection: {
                columnDestinationUuid: config.column_destination_uuid,
                sourceZoomMeetingIdentifiers: config.source_zoom_meetings,
                sresUsername: config.sres_username,
                i: i + 1
            }});
        }
    }
});
// Add a connection
$(document).on('click', '.sres-connection-new', function(event, eventArgs){
    let template = Handlebars.compile(document.getElementById("connection-mapper-template").innerHTML);
    let counter = null;
    let addingExistingConnection = (typeof eventArgs !== 'undefined' && typeof eventArgs.existingConnection !== 'undefined' && typeof eventArgs.existingConnection.i !== 'undefined');
    let currentUserUsername = ENV['SRES_USER'].username;
    if (addingExistingConnection) {
        counter = eventArgs.existingConnection.i;
    } else {
        sresZoomConnections.push({});
        counter = sresZoomConnections.length;
    }
    let html = template({
        counter: counter,
        destination_columns: ENV['EXISTING_COLUMNS'],
        zoom_meetings: ENV['ZOOM_MEETINGS'],
        save_disabled: true,
        new_connection: true,
        connection_type: 'past_meeting_participants'
    });
    // put it into DOM
    $('#connections_container').prepend(html);
    // trigger some UI
    let connectionContainer = $(".sres-connection-configuration-card[data-sres-counter=" + counter + "]");
	$(".sres-connection-configuration-card[data-sres-counter=" + counter + "] select.sres-zoom-meetings")
		.chosen({'width': '100%', search_contains: true});
    // update if existing config
    if (addingExistingConnection) {
        // updating existing
        //console.log('eventArgs.existingConnection', eventArgs.existingConnection);
        connectionContainer.find('.sres-import-action-choose[value=existing]').trigger('click');
        connectionContainer.find('.sres-import-destination-select').val(eventArgs.existingConnection.columnDestinationUuid);
        connectionContainer.find('.sres-zoom-meetings').val(eventArgs.existingConnection.sourceZoomMeetingIdentifiers).trigger('chosen:updated');
        connectionContainer.find('button.sres-connection-sync-now').prop('disabled', false);
        connectionContainer.find('.sres-refresh-now-warning').addClass('d-none');
        let meetingsFromAnotherUser = false;
        let meetingUuidToUsername = {};
        for (let j = 0; j < ENV['ZOOM_MEETINGS'].length; j++) {
            meetingUuidToUsername[ENV['ZOOM_MEETINGS'][j].uuid] = ENV['ZOOM_MEETINGS'][j].sres_username;
        }
        for (let k = 0; k < eventArgs.existingConnection.sourceZoomMeetingIdentifiers.length; k++) {
            let meetingIdentifier = eventArgs.existingConnection.sourceZoomMeetingIdentifiers[k];
            if (meetingUuidToUsername[meetingIdentifier] != currentUserUsername) {
                meetingsFromAnotherUser = true;
                break;
            }
        }
        if (meetingsFromAnotherUser) {
            connectionContainer.find('.sres-connection-by-another-user-warning').removeClass('d-none');
        }
    } else {
        connectionContainer[0].scrollIntoView({behavior: 'smooth'});
    }
});
// Delete a connection
$(document).on('click', '.sres-connection-delete', function(){
    let counter = $(this).parents('[data-sres-counter]').attr('data-sres-counter');
    $("input[name='connection_workflow_state_" + counter + "']").val('deleted');
    $("div.sres-connection-configuration-card[data-sres-counter=" + counter + "]").parents('.sres-card-container').addClass('d-none');
});
// Choose a column action
$(document).on('change', '.sres-import-action-choose', function(){
    let counter = $(this).parents('[data-sres-counter]').attr('data-sres-counter');
    //console.log('counter', counter);
    $("#connection_column_destination_" + counter).addClass('d-none');
    $("#connection_column_new_name_" + counter).addClass('d-none');
    switch ($(this).val()) {
        case 'existing':
            $("#connection_column_destination_" + counter).removeClass('d-none');
            break;
        case 'new':
            $("#connection_column_new_name_" + counter).removeClass('d-none');
            break;
    }
});

// Force sync
$(document).on('change click', '.sres-connection-configuration-card .card-body button, .sres-connection-configuration-card .card-body input, .sres-connection-configuration-card .card-body select', function(){
    $(this).parents('.sres-connection-configuration-card')
        .find('.sres-refresh-now button').prop('disabled', true).end()
        .find('.sres-refresh-now-warning').removeClass('d-none').end();
});
$(document).on('click', 'button.sres-connection-sync-now', function(){
    let spinner = $(this).find('.sres-refresh-now-spinner').addClass('spinning');
    window.setTimeout(function(){
        spinner.removeClass('spinning');
    }, 3000);
    $.ajax({
        url: ENV['FORCE_REFRESH_ENDPOINT']
            .replace('__connection_type__', $(this).attr('data-sres-connection-type'))
            .replace('__connection_index__', $(this).parents('[data-sres-counter]').attr('data-sres-counter')),
        method: 'GET',
        success: function(data){
            $.notify({message: 'Sync requested.'},{type: 'success'});
        },
        error: function(data){
            $.notify({message: 'Request failed.'},{type: 'danger'});
        }
    });
});


$(document).on('click', 'a.sres-action-buttons-jump', function(event){
   event.preventDefault();
   $('#update_connection')[0].scrollIntoView({behavior: 'smooth'});
   $('#update_connection').addClass('animated bounce infinite');
   window.setTimeout(function(){
       $('#update_connection').removeClass('animated bounce infinite');
   }, 3000);
});
