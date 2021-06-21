/**
    dataToSend / sendData interceptor
**/
var latestDataToSend = null;
function setDataToSend(identifierIgnored, dataToSend, updateTargetIgnored, tableuuid, columnuuid){
    latestDataToSend = dataToSend;
    if (latestDataToSend !== null) {
        $('#find_student_container').removeClass('d-none');
        $('#bulk_identifiers').focus();
    } else {
        $('#find_student_container').addClass('d-none');
    }
    // Update input fields
    populateInputFields("div.sres-input-container[data-sres-identifier]", latestDataToSend);
}

/**
    Timestamper
**/
function updateTimestamp() {
    $('#set_data_current_timestamp').text(moment().format("YYYY-MM-DD HH:mm:ss"));
    setTimeout(updateTimestamp, 500);
};

/**
    Process identifiers
**/
var bulkCount = 0;
var syncCurrentlyProcessing = false;
$(document).on('keypress', '#bulk_identifiers', function(e){
    if (((e.keyCode ? e.keyCode : e.which) == 13) && (!syncCurrentlyProcessing)) {
        processBulkInputInitiate();
    }
});
$(document).on('click', '#bulk_process_identifiers_now', function(){
    processBulkInputInitiate();
});
function processBulkInputInitiate() {
    callIfConnected('bs3', processBulkInput);
    return false;
}
function processBulkInput() {
    switch (ENV['column_type']) {
        case 'submission':
        case 'signinout':
        case 'signinoutmemory':
            latestDataToSend = moment().format("YYYY-MM-DD HH:mm:ss")
            break;
    }
    if ($('#bulk_identifiers').val().length != 0 ) {
        var resultSource = document.getElementById("bulk_processing_result_template").innerHTML;
        var resultTemplate = Handlebars.compile(resultSource);
        syncCurrentlyProcessing = true;
        $('#bulk_identifiers').attr('disabled','disabled');
        var lines = $('#bulk_identifiers').val().split('\n');
        for (var i = 0; i < 1 /*i.e. process one line at a time instead of lines.length*/; i++) {
            if (lines[i].length != 0) {
                bulkCount++;
                var Identifier = lines[i];
                var Data = latestDataToSend;
                let context = {
                    id: bulkCount,
                    identifier: Identifier,
                    statusText: 'Saving data...'
                }
                $('#bulk_processing_results_container').prepend(resultTemplate(context));
                // submit via ajax
                $.ajax({
                    url: ENV['set_data_endpoint'].replace('__column_uuid__', ENV['column_uuid']),
                    method: 'POST',
                    data: {
                        table_uuid: ENV['table_uuid'],
                        column_uuid: ENV['column_uuid'], 
                        identifier: Identifier,
                        ignore_notify: false, 
                        ignore_apply_to_others: false, 
                        data: Data,
                        quick_info: 'bulk',
                        echo: JSON.stringify({
                            bulkCount: bulkCount
                        })
                    },
                    success: function(data) {
                        data = JSON.parse(data);
                        processBulkInputResult(data, true);
                        processBulkInput();
                    },
                    error: function(data) {
                        console.error(data);
                        processBulkInputResult(data, false);
                        processBulkInput();
                    }
                });
            }
        }
        $('#bulk_identifiers').val(lines.slice(1).join('\n')).removeAttr('disabled');
        syncCurrentlyProcessing = false;
        $('#bulk_identifiers').focus();
    };
};
function processBulkInputResult(data, success) {
    //console.log('raw data', data);
    console.log(data);
    let resultContainer = $('.sres-bulk-processing-result[data-sres-id=' + data.echo.bulkCount + ']');
    let messages = [];
    if (data.messages) {
        data.messages.forEach(function(message){
            messages.push('<span class="badge badge-' + message[1] + '">' + message[0] + '</span>');
        });
    }
    if (success) {
        if (data.success == true) {
             resultContainer.find('span.sres-bulk-processing-success').removeClass('d-none');
        }
        if (data.apply_to_others.success == true) {
             resultContainer.find('span.sres-bulk-processing-apply-to-others').removeClass('d-none');
        }
        if (data.notify_email.success == true) {
             resultContainer.find('span.sres-bulk-processing-notify-email').removeClass('d-none');
        }
        if (data.quick_info) {
             resultContainer.find('span.sres-bulk-processing-response').html(data.quick_info);
        }
        setTimeout(function(){
            resultContainer.addClass('d-none');
        }, 5000);
    } else {
        resultContainer.find('span.sres-bulk-processing-error').removeClass('d-none');
        resultContainer.find('span.sres-bulk-processing-response')
            .removeClass('d-none')
            .html(data.statusText + ' ' + data.status);
    }
    resultContainer.find('span.sres-bulk-processing-messages').html(messages.join(''));
    $("textarea#bulk_identifiers").focus();
}
$(document).on('click', '#bulk_process_identifiers_hide_all', function(){
    $('.sres-bulk-processing-result').addClass('d-none');
});
$(document).on('click', '#bulk_process_identifiers_show_all', function(){
    $('.sres-bulk-processing-result').removeClass('d-none');
});

/**
    Backup handling
**/
function checkForBackups(identifier) {
    var backupsExist = false;
    var tableuuid = ENV['table_uuid'];
    var columnuuid = ENV['column_uuid'];
    var identifier = ENV['identifier'];
    console.log('backupexists', backupExists(tableuuid, columnuuid, identifier));
    if (backupExists(tableuuid, columnuuid, identifier)) {
        $('#backup_available_notification_container').removeClass('d-none');
        backupsExist = true;
    } else {
        $('#backup_available_notification_container').addClass('d-none');
    }
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
