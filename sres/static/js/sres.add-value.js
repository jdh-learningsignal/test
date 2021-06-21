/**
    Focus
**/
$(document).ready(function(){
    $("[data-sres-field]").first().focus();
});

/**
    Search student 
**/
var searchStudentLastKeycode = 0;
var searchStudentLastTerm = '';
$(document).ready(function(){
    $("#search_student_term").autocomplete(
        {
            minLength: 2,
            autoWidth: false,
            autoselect: true
        }, 
        {
            name: 'search-results',
            source: function(query, callback){
                $.ajaxq.abort('search-student-term');
                $.ajaxq('search-student-term', {
                    url: $("#search_student_term").attr('data-sres-search-endpoint'),
                    data: { term: query },
                    type: 'GET',
                    dataType: 'json',
                    success: function(data){
                        // See if enter was pressed
                        if (searchStudentLastKeycode == 13 && data.search_results.length == 1 && data.term == searchStudentLastTerm) {
                            //console.log('GOGOGO', data.term, query, data.search_results[0]);
                            window.location = $("#search_student_term").attr('data-sres-column-root-url').replace('__identifier__', data.search_results[0]['sid']);
                            return false;
                        }
                        // Otherwise continue to show
                        //console.log('search result', data);
                        callback(data['search_results'].map(function(x){
                            return x;
                        }));
                    }
                });
            },
            templates: {
                suggestion: function(suggestion, answer){
                    //console.log(suggestion, answer);
                    let spanClass = suggestion['studentstatus'] == 'active' ? '' : 'text-muted font-italic';
                    let displayName = suggestion['fullname'];
                    let sid = suggestion['display_sid'];
                    let inactive = suggestion['studentstatus'] == 'inactive';
                    let template = Handlebars.compile(document.getElementById("search_student_result_line_template").innerHTML);
                    return template({
                        sid: sid,
                        spanClass: spanClass,
                        displayName: displayName,
                        inactive: inactive
                    })
                },
                empty: function(){
                    return '<span>No students found</span>';
                }
            }
        }
    ).on('autocomplete:selected', function(event, suggestion, dataset, context) {
        console.log(event, suggestion, dataset, context);
        window.location = $("#search_student_term").attr('data-sres-column-root-url').replace('__identifier__', suggestion['sid']);
    }).on('keypress', function(event){
        //console.log(event.key);
        searchStudentLastKeycode = event.keyCode;
        searchStudentLastTerm = $(this).val();
    });
});

/**
    Clear data
**/
$(document).on('click', '.sres-add-value-clear-data', function(){
    sendData(
        $(this).attr('data-sres-identifier'),
        "",
        null,
        $(this).attr('data-sres-table-uuid'),
        $(this).attr('data-sres-column-uuid')
    );
});

/**
    Save data
**/
function sendDataSend(identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback) {
    //console.log('sending data', identifier, dataToSend);
    // get report index if operating in multiple report mode
    let multipleReportTogglerContainer = $(".sres-report-toggler-container[data-sres-columnuuid='" + columnuuid + "'][data-sres-identifier='" + identifier + "']");
    let multipleReportCurrentIndex = null;
    if (multipleReportTogglerContainer) {
        multipleReportCurrentIndex = multipleReportTogglerContainer.attr('data-sres-report-toggler-current-index');
    }
    // emit datasaving event
    $(document).trigger('sres:datasaving', {
        'identifier': identifier,
        'dataToSend': dataToSend,
        'tableuuid': tableuuid,
        'columnuuid': columnuuid
    });
    // send data via ajax
    $.ajax({
        url: ENV['set_data_endpoint'].replace('__column_uuid__', columnuuid),
        method: 'POST',
        data: { 
            'table_uuid': ENV['table_uuid'],
            'column_uuid': columnuuid, 
            'identifier': identifier,
            'ignore_notify': false, 
            'ignore_apply_to_others': false, 
            'report_index': multipleReportCurrentIndex,
            'data': dataToSend
        },
        success: function(data){
            data = JSON.parse(data);
            //console.log('data', data, typeof data.success);
            if (data.success == true || data.success == 'true') {
                // Remove backup
                deleteBackup(tableuuid, columnuuid, identifier);
                // Focus on find student box
                if (ENV.hasOwnProperty('FOCUS_SEARCH_STUDENT_INPUT_AFTER_SAVE') && !ENV['FOCUS_SEARCH_STUDENT_INPUT_AFTER_SAVE']){
                    // pass
                } else {
                    if ($("#search_student_term")) {
                        $("#search_student_term").focus();
                    }
                }
                // Refresh quickinfo if needed
                if (ENV['reload_quickinfo_upon_saving'] == 'true' && !isOfflineModeActive()) {
                    let originalHtml = $('.sres-quickinfo-container').html();
                    $('.sres-quickinfo-container').append('<div style="left:50%; position:absolute; margin-top:-' + ($('.sres-quickinfo-container').outerHeight() / 2) + 'px;"><span class="fa fa-circle-notch spinning sres-quickinfo-loading"></span></div>');
                    updateQuickInfo('single', identifier, originalHtml);
                }
                // Update others
                if (typeof successCallback !== 'undefined') {
                    successCallback(identifier, dataToSend, tableuuid, columnuuid, data);
                }
                genericSuccessCallback(identifier, dataToSend, tableuuid, columnuuid, data);
            } else {
                genericErrorCallback(identifier, dataToSend, tableuuid, columnuuid);
            }
        },
        error: function(data) {
            var err = data;
            //console.log('in error', data, updateTarget, tableuuid, columnuuid, identifier, backupExists(tableuuid, columnuuid, identifier));
            updateTarget.addClass('bg-danger');
            setTimeout(function(){ updateTarget.removeClass('bg-danger'); }, 2000);
            genericErrorCallback(identifier, dataToSend, tableuuid, columnuuid, err);
        }
    });
}
function sendData(identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback) {
    saveBackup(dataToSend, tableuuid, columnuuid, identifier);
    if (isOfflineModeActive()) {
        if (checkForBackups(identifier)) {
        } else {
            // Problem saving backup...
            alert('There may have been a problem saving the offline backup data. Please try again.');
        }
    }
    //console.log('calling if conneted...');
    callIfConnected('bs3', sendDataSend, identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback);
    return false;
}
$(document).on('sres:datasaving', function(event, args) {
    //console.log('sres:datasaving', event, args);
    $btn = $('button.sres-addvalue-btn-save[data-sres-identifier="' + args['identifier'] + '"][data-sres-columnuuid=' + args['columnuuid'] + ']');
    $btn.append('<span class="' + SRES_SAVE_BTN_SAVING_CLASS + '"></span>');
});
$(document).on('sres:datasaved', function(event, args) {
    //console.log('sres:datasaved', event, args);
    $btn = $('button.sres-addvalue-btn-save[data-sres-identifier="' + args['identifier'] + '"][data-sres-columnuuid=' + args['columnuuid'] + ']');
    $btn.find('.sres-addvalue-btn-save-status').remove();
    $btn.addClass('btn-success').append('<span class="' + SRES_SAVE_BTN_SAVED_CLASS + '"></span>');
    window.setTimeout(function(){
        $btn.removeClass('btn-success').find('.sres-addvalue-btn-save-status').remove();
    }, 2000);
});
$(document).on('sres:datasaveerror', function(event, args) {
    //console.log('sres:datasaveerror', event, args);
    $btn = $('button.sres-addvalue-btn-save[data-sres-identifier="' + args['identifier'] + '"][data-sres-columnuuid=' + args['columnuuid'] + ']');
    $btn.find('.sres-addvalue-btn-save-status').remove();
    $btn.addClass('btn-danger animated shake').append('<span class="' + SRES_SAVE_BTN_ERROR_CLASS + '"></span>');
    window.setTimeout(function(){
        $btn.removeClass('btn-danger animated shake').find('.sres-addvalue-btn-save-status').remove();
    }, 2000);
});

/**
    Backup handling
**/
function checkForBackups(identifier) {
    var backupsExist = false;
    var tableuuid = ENV['table_uuid'];
    var columnuuid = ENV['column_uuid'];
    var identifier = ENV['identifier'];
    //console.log('backupexists', backupExists(tableuuid, columnuuid, identifier));
    if (backupExists(tableuuid, columnuuid, identifier)) {
        $('#backup_available_notification_container').removeClass('d-none');
        backupsExist = true;
    } else {
        $('#backup_available_notification_container').addClass('d-none');
    }
    // Trigger other backup retrieval handlers
    $(document).trigger('sres:audiorecordingshowbackups');
    // Toggle stuff depending on offline/offline
    if (isOfflineModeActive()){
        $('button.sres-backup-available-save').addClass('d-none');
    } else {
        $('button.sres-backup-available-save').removeClass('d-none');
    }
    return backupsExist;
}
$(document).ready(function(){
    checkForBackups();
});
$(document).on('click', '.sres-backup-available-save, .sres-backup-available-view, .sres-backup-available-delete', function(){
    var identifier = ENV['identifier'];
    var tableuuid = ENV['table_uuid'];
    var columnuuid = ENV['column_uuid'];
    var backedUpData = getBackup(tableuuid, columnuuid, identifier);
    //console.log('backedUpData', backedUpData);
    if ($(this).hasClass('sres-backup-available-save')) {
        // Mimic data send
        sendData(identifier, backedUpData, null, tableuuid, columnuuid);
    } else if ($(this).hasClass('sres-backup-available-view')) {
        populateInputFields('div.sres-input-container[data-sres-identifier="' + identifier + '"]', backedUpData);
    } else if ($(this).hasClass('sres-backup-available-delete')) {
        // Delete the backup
        deleteBackup(tableuuid, columnuuid, identifier);
        checkForBackups();
    }
});
