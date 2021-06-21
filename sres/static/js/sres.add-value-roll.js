$(document).ready(function(){
    checkForBackups();
	autosize($("#apply_data_to_all_modal textarea[data-sres-field]"));
});

/**
    Data saving
**/
function sendData(identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback, errorCallback) {
    saveBackup(dataToSend, tableuuid, columnuuid, identifier);
    if (isOfflineModeActive()) {
        if (checkForBackups(identifier)) {
            applyDataToAllInputsUpdated = false;
            $("#apply_data_to_all_modal").modal('hide');
        } else {
            // Problem saving backup...
            alert('There may have been a problem saving the offline backup data. Please try again.');
        }
    }
    //console.log('calling if conneted...');
    callIfConnected('bs3', sendDataSend, identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback, errorCallback);
    return false;
}
function sendDataSend(identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback, errorCallback) {
    $(document).trigger('sres:datasaving', {
        'identifier': identifier,
        'dataToSend': dataToSend,
        'tableuuid': tableuuid,
        'columnuuid': columnuuid
    });
    //console.log('sending data', identifier, dataToSend);
    $.ajax({
        url: ENV['set_data_endpoint'],
        method: 'POST',
        data: { 
            'table_uuid': ENV['table_uuid'],
            'column_uuid': ENV['column_uuid'], 
            'identifier': identifier,
            'ignore_notify': false, 
            'ignore_apply_to_others': false, 
            'data': dataToSend
        },
        success: function(data){
            try {
                data = JSON.parse(data);
            } catch(e) {
                console.error('Could not parse data');
                data = {
                    success: false,
                    status: 400
                }
            }
            //console.log('data', data);
            if (data.success == true || data.success == 'true') {
                // Remove backup
                deleteBackup(tableuuid, columnuuid, identifier);
                /** Update data display **/
                // Parse apply to others if necessary
                //console.log(data);
                let identifiersToUpdate = [identifier];
                if (Object.getOwnPropertyNames(data.apply_to_others).length > 0 && (data.apply_to_others.success || data.apply_to_others.success == 'true')) {
                    for (let i = 0; i < data.apply_to_others.other_targets.length; i++) {
                        identifiersToUpdate.push(data.apply_to_others.other_targets[i].sid);
                    }
                }
                for (var i = 0; i < identifiersToUpdate.length; i++) {
                    try {
                        let targetCell = $('td[id="data_' + identifiersToUpdate[i] + '"]');
                        if (typeof data.load_existing_data_mode !== 'undefined' && data.load_existing_data_mode == 'fresh') {
                            // making a fresh entry every time so clear the data
                            targetCell.data("sres-data", '');
                            table.cell(targetCell).data('');
                        } else {
                            if (ENV['column_type'] == 'image') {
                                table.cell(targetCell).data('<img src="' + data.data.url + '">');
                            } else {
                                let displayData = $('<span>').text(data.data.saved.substr(0, ENV['truncate_data_display_after'])).html();
                                table.cell(targetCell).data(
                                    '<span class="' + (data.data.saved.length > 0 ? "fa fa-save" : "") + '" aria-hidden="true"></span> '
                                    + '<span class="' + (displayData.length == ENV['truncate_data_display_after'] ? 'sres-data-display-fadeout' : '') + '">'
                                    + displayData
                                    + '</span>'
                                );
                                targetCell.data("sres-data", data.data.saved);
                            }
                            // update other inline if necessary
                            populateInputFields(
                                ".sres-input-container[data-sres-identifier='" + identifiersToUpdate[i] + "']",
                                data.data.saved,
                                identifiersToUpdate[i],
                                true
                            );
                        }
                        // Update the appearance of the popout editor button
                        $(document).trigger('sres:popouteditordataupdated', {
                            identifier: identifiersToUpdate[i]
                        });
                        // update quickinfo
                        if (ENV['reload_quickinfo_upon_saving'] == 'true' && !isOfflineModeActive()) {
                            let $quickInfoTd = $('tr[data-sres-identifier="' + identifiersToUpdate[i] + '"] td[data-sres-role=quickinfo]');
                            let originalHtml = $quickInfoTd.html();
                            $quickInfoTd
                                .data('sres-original-html', originalHtml)
                                .html('<span class="fa fa-circle-notch spinning"></span>');
                            updateQuickInfo('roll', identifiersToUpdate[i], originalHtml);
                        }
                    } catch(e) {
                        console.error('error in row with identifier ' + identifiersToUpdate[i], e);
                    }
                }
                table.draw('page');
                // Update updateTarget
                for (var i = 0; i < identifiersToUpdate.length; i++) {
                    let updateTds = $('tr[data-sres-identifier="' + identifiersToUpdate[i] + '"] td');
                    updateTds.addClass('bg-success-light');
                    setTimeout(function(){
                        updateTds.removeClass('bg-success-light'); 
                    }, 2000);
                }
                // Update others
                if (typeof successCallback != 'undefined' && successCallback !== null) {
                    successCallback(identifier, dataToSend, tableuuid, columnuuid, data);
                } else {
                    genericSuccessCallback(identifier, dataToSend, tableuuid, columnuuid, data);
                }
            } else {
                if (typeof errorCallback != 'undefined' && errorCallback !== null) {
                    errorCallback(identifier, dataToSend, tableuuid, columnuuid);
                } else {
                    genericErrorCallback(identifier, dataToSend, tableuuid, columnuuid, data);
                }
            }
        },
        error: function(data) {
            var err = data;
            //console.log('in error', data, updateTarget, tableuuid, columnuuid, identifier, backupExists(tableuuid, columnuuid, identifier));
            updateTarget.addClass('bg-danger-light');
            setTimeout(function(){ updateTarget.removeClass('bg-danger-light'); }, 2000);
            if (typeof errorCallback != 'undefined' && errorCallback !== null) {
                errorCallback(identifier, dataToSend, tableuuid, columnuuid, err);
            } else {
                genericErrorCallback(identifier, dataToSend, tableuuid, columnuuid, err);
            }
        }
    });
}

/**
    Popout editor trigger button
**/
$(document).on('sres:popouteditordataupdated', function(event, args) {
    //console.log('sres:popouteditordataupdated', event, args);
    if (ENV['popout_change_edit_button_colour'] != 'false') {
        let dataExists = false;
        let data = $('td[data-sres-role=data-store][data-sres-identifier="' + args.identifier + '"]').data("sres-data");
        try {
            if (JSON.parse(data).length) {
                dataExists = true;
            }
        } catch(e) {
            if (data && data.length) {
                dataExists = true;
            }
        }
        $button = $('tr[data-sres-identifier="' + args.identifier + '"] button.sres-trigger-editor-popout');
        let dataExistsClass = 'btn-outline-primary';
        let dataExistsAdditionalSpan = '';
        let dataNotExistsClass = 'btn-outline-primary';
        switch (ENV['popout_change_edit_button_colour']) {
            case 'false':
                // don't do anything
                break;
            case 'bluegreentick':
                dataExistsAdditionalSpan = '<span class="fa fa-check-circle ml-1" aria-hidden="true" aria-label="Data entered"></span>';
            case 'bluegreen':
                dataExistsClass = 'btn-outline-success';
                break;
            case 'bluegrey':
                dataNotExistsClass = 'btn-outline-primary';
                dataExistsClass = 'btn-outline-secondary';
                break;
            case 'bluefillgreenoutline':
                dataNotExistsClass = 'btn-primary';
                dataExistsClass = 'btn-outline-success';
                break;
        }
        $button.removeClass('btn-outline-primary'); // remove the default
        if (dataExists) {
            $button.addClass(dataExistsClass);
            $button.append(dataExistsAdditionalSpan);
        } else {
            $button.addClass(dataNotExistsClass);
        }
    }
});
$(document).ready(function(){
    if (ENV['popout_change_edit_button_colour'] != 'false') {
        $("#roll_table tr").each(function(){
            let identifier = $(this).attr('data-sres-identifier');
            $(document).trigger('sres:popouteditordataupdated', {
                identifier: identifier
            });
        });
    }
});

/**
    Data saving status
**/
$(document).on('sres:datasaving', function(event, args) {
    //console.log('sres:datasaving', event, args);
    $btn = $('button.sres-addvalue-btn-save[data-sres-identifier="' + args['identifier'] + '"][data-sres-columnuuid=' + args['columnuuid'] + ']');
    $btn.append('<span class="' + SRES_SAVE_BTN_SAVING_CLASS + '"></span>');
});
$(document).on('sres:datasaved', function(event, args) {
    //console.log('sres:datasaved', event, args);
    let $btn = $('button.sres-addvalue-btn-save[data-sres-identifier="' + args['identifier'] + '"][data-sres-columnuuid=' + args['columnuuid'] + ']');
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
    Bulk actions
**/
function applyDataToAllApply(identifier, dataToSend, updateTarget, tableuuid, columnuuid) {
    var target_identifier = $("#apply_data_to_all_modal_target_identifier").val();
    if (target_identifier == '') {
        // Operating in apply-to-all mode
        // Change up display
        $("#apply_data_to_all_modal_progress_container").removeClass('d-none');
        $("#apply_data_to_all_modal_input").addClass('d-none');
        $("#apply_data_to_all_modal_footer").removeClass('d-none');
        // Iterate through filtered rows
        var numberOfRows = table.rows({search:'applied'}).nodes().length;
        var currentRow = 0;
        var successRows = 0;
        table.rows({search:'applied'}).nodes().to$().each(function(){
            currentRow++
            var identifier = $(this).attr('data-sres-identifier');
            //$("#apply_data_to_all_modal_progress_queued").css('width', (currentRow / numberOfRows * 100.0) + '%');
            sendData(
                identifier, 
                dataToSend, 
                $(this), 
                tableuuid, 
                columnuuid, 
                function(identifier, dataToSend, tableuuid, columnuuid) {
                    successRows++
                    $("#apply_data_to_all_modal_progress_success")
                        .css('width', (successRows / numberOfRows * 100.0) + '%')
                        .text(successRows + ' of ' + numberOfRows);
                    checkForBackups();
                }
            );
        });
    } else {
        // Operating in single popout editor mode
        updateTarget = $('#roll_table tr[data-sres-identifier="' + target_identifier + '"] td');
        //console.log('updateTarget', updateTarget);
        sendData(
            target_identifier, 
            dataToSend, 
            updateTarget, 
            tableuuid, 
            columnuuid, 
            function(identifier, dataToSend, tableuuid, columnuuid, data) {
                // OK
                applyDataToAllInputsUpdated = false; // User wants to save so don't need to warn
                $("#apply_data_to_all_modal").modal('hide');
                genericSuccessCallback(identifier, dataToSend, tableuuid, columnuuid, data);
            },
            function(identifier, dataToSend, tableuuid, columnuuid, err) {
                // Error
                applyDataToAllInputsUpdated = false; // User wants to save so don't need to warn
                $("#apply_data_to_all_modal").modal('hide');
                genericErrorCallback(identifier, dataToSend, tableuuid, columnuuid, err);
            }
        );
    }
}
// Detect changes and prompt user if closing without saving
var applyDataToAllInputsUpdated = false;
$(document).on("propertychange change click keyup input paste", "#apply_data_to_all_modal .modal-body input, #apply_data_to_all_modal .modal-body select, #apply_data_to_all_modal .modal-body button:not([data-toggle=collapse])", function(){
    applyDataToAllInputsUpdated = true;
});
$(document).on("change propertychange keyup paste", "#apply_data_to_all_modal .modal-body textarea", function(){
    applyDataToAllInputsUpdated = true;
});
$(document).on('hide.bs.modal', "#apply_data_to_all_modal", function(event){
    if (applyDataToAllInputsUpdated) {
        if (confirm('There may be some unsaved changes. Are you sure you wish to close this dialog?')) {
            // Continue
        } else {
            event.preventDefault();
            event.stopPropagation();
        }
    }
});
// Show apply data to all modal
$(document).on('click', "#apply_data_to_all", function(event){
    event.stopPropagation();
    // Clear data
    clearInputFields();
    applyDataToAllInputsUpdated = false;
    // Show modal
    $("#apply_data_to_all_modal_input").removeClass('d-none');
    $("#apply_data_to_all_modal_progress_container").addClass('d-none');
    $("#apply_data_to_all_modal_progress_queued").css('width', '0%');
    $("#apply_data_to_all_modal_progress_success").css('width', '0%');
    $("#apply_data_to_all_modal_footer").addClass('d-none');
    $("#apply_data_to_all_modal .modal-title").html('Apply to all');
    $("#apply_data_to_all_modal_target_identifier").val('');
    $("#apply_data_to_all_modal").modal('show');
    $("#apply_data_to_all_modal textarea[data-sres-field]").each(function(){
        $(this).attr('rows', 3);
    });
});
$(document).on('click', "#apply_data_to_all_modal_refresh", function(){
    location.reload();
});
		
/**
    Popout editing
**/
// Popout editor for individual students
$(document).on('click', ".sres-trigger-editor-popout", function(event, data){
    let identifier = $(this).closest('tr[data-sres-identifier]').attr('data-sres-identifier');
    // Adapt modal
    $("#apply_data_to_all_modal_target_identifier").val(identifier);
    $("#apply_data_to_all_modal_progress_container").addClass('d-none');
    $("#apply_data_to_all_modal_footer").addClass('d-none');
    $("#apply_data_to_all_modal").attr('data-sres-identifier', identifier);
    $("#apply_data_to_all_modal .sres-addvalue-btn-save").attr('data-sres-identifier', identifier);
    $("#apply_data_to_all_modal [data-sres-field]").attr('data-sres-identifier', identifier);
    // Popout titler
    let title = ENV['popout_title_style'];
    $tr = $(this).closest('tr[data-sres-identifier]');
    let sid = Handlebars.escapeExpression($tr.attr('data-sres-identifier'));
    let preferredName = Handlebars.escapeExpression($tr.attr('data-sres-preferred-name'));
    let surname = Handlebars.escapeExpression($tr.attr('data-sres-surname'));
    title = title.replace('$SID$', sid);
    title = title.replace('$PREFERREDNAME$', preferredName);
    title = title.replace('$SURNAME$', surname);
    $("#apply_data_to_all_modal .modal-title").html(title);
    // Populate modal with existing data and show it
    if (typeof data == 'undefined' || data == null ) {
        data = $(this).closest('tr[data-sres-identifier]').find("td[id='data_" + identifier + "']").data("sres-data");
        if (typeof data == "object") {
            data = JSON.stringify(data);
        } else {
            // leave as is
            //data = JSON.stringify(data);
        }
        console.log("sres-data", data);
    } else {
        // Just use the data passed in
    }
    populateInputFields("#apply_data_to_all_modal", data, identifier);
    $(document).trigger('sres:audiorecordingshowbackups');
    // Trigger multientry recalculations if necessary
    $("[data-sres-subfield-n][data-sres-multientry-render-calculated-value='yes']").each(function(){
        let type = $(this).find("[data-sres-field]").attr("data-sres-field");
        let args = {
            columnUuid: $(this).parents(".sres-input-container[data-sres-column-type]").attr('data-sres-columnuuid'),
            subfield: $(this).attr('data-sres-subfield-n'),
            identifier: '',
            type: type
        }
        triggerMultientryAggregation(type, args);
    });
    // Show modal
    $("#apply_data_to_all_modal").modal('show');
    $("#apply_data_to_all_modal textarea[data-sres-field]").each(function(){
        $(this).attr('rows', $(this).val().split(/\n/).length > 1 ? $(this).val().split(/\n/).length : 3);
    });
    applyDataToAllInputsUpdated = false;
});
$(document).on("shown.bs.modal", "#apply_data_to_all_modal", function(){
    // sketch
    prepareSketchables();
    // range sliders
    //renderRangeSlider(ENV.column_uuid);
});
$(document).ready(function(){
    prepareSketchables();
});

/** 
    Backups and restoration 
**/
$(document).on('click', '.sres-backup-available-save, .sres-backup-available-view, .sres-backup-available-delete', function(){
    var identifier = $(this).parents('tr').attr('data-sres-identifier');
    var tableuuid = ENV['table_uuid'];
    var columnuuid = ENV['column_uuid'];
    var backedUpData = getBackup(tableuuid, columnuuid, identifier);
    //console.log('backedUpData', backedUpData);
    if ($(this).hasClass('sres-backup-available-save')) {
        // Mimic data send
        $("#apply_data_to_all_modal_target_identifier").val(identifier);
        applyDataToAllApply(identifier, backedUpData, null, tableuuid, columnuuid);
    } else if ($(this).hasClass('sres-backup-available-view')) {
        // Show data in popout
        switch ($(this).parents('td').attr('data-sres-editor-mode')) {
            case 'multiEntry-popout':
                // Populate modal with backed up data and show it
                $('#roll_table tr[data-sres-identifier="' + identifier + '"] td button.sres-trigger-editor-popout')
                    .trigger('click', backedUpData);
                break;
            default:
            case 'multiEntry-inline':
                populateInputFields('div.sres-input-container[data-sres-identifier="' + identifier + '"]', backedUpData, identifier);
                break;
        }
    } else if ($(this).hasClass('sres-backup-available-delete')) {
        // Delete the backup
        deleteBackup(tableuuid, columnuuid, identifier);
        checkForBackups();
    }
});
$(document).on('click', 'a.sres-backup-review-all', function(){
    $('.sres-backup-available-notification')
        .filter(":visible")
        .find('.sres-backup-available-view')
        .trigger('click');
});
$(document).on('click', 'a.sres-backup-save-all', function(){
    $('.sres-backup-available-notification')
        .filter(":visible")
        .find('.sres-backup-available-save')
        .trigger('click');
});
function checkForBackups(identifier) {
    var rows;
    var backupsExist = false;
    if (typeof identifier == 'undefined' || identifier == null) {
        rows = $("#roll_table tr");
    } else {
        rows = $('#roll_table tr[data-sres-identifier="' + identifier + '"]');
    }
    var tableuuid = ENV['table_uuid'];
    var columnuuid = ENV['column_uuid'];
    rows.each(function(){
        let identifier = $(this).attr('data-sres-identifier');
        if (backupExists(tableuuid, columnuuid, identifier)) {
            $(this).find('.sres-backup-available-notification').css('display', 'inline');
            backupsExist = true;
        } else {
            $(this).find('.sres-backup-available-notification').each(function(){
                this.style.setProperty('display', 'none', 'important');
            });
        }
    });
    // Bulk backup actions button
    if (backupsExist) {
        $('.sres-backup-bulk-actions').removeClass('d-none');
    } else {
        $('.sres-backup-bulk-actions').addClass('d-none');
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
$(document).on('draw.dt', '#roll_table', function(){
    checkForBackups();
});
		
	$(document).on('click', "#group_selector_button", function() {
		let location = ENV['base_url'] + "?group=" + encodeURIComponent(JSON.stringify($("#group_selector_a").val()));
		if (ENV['sdak']) {
            location += '&sdak=' + ENV['sdak'];
        }
        window.location = location;
	});
	$(document).on('click', "a.sres-button-save-all", function() {
		$("button.sres-addvalue-sase-text-save[data-sres-mode=roll]").each(function() {
			$(this).trigger('click');
		});
		$("button[class~=sres-addvalue-save][class~=sres-addvalue-module-element]").each(function(){
			$(this).trigger('click');
		});
	});
    
/**
    Offline mode
**/
// Things to do when starting or stopping offline mode
$(document).on('sres.offlineModeStart', function(){
	$('#group_selector_a.selectpicker').prop('disabled', true).selectpicker('refresh');
	$('#group_selector_button').prop('disabled', true);
});
$(document).on('sres.offlineModeStop', function(){
	$('#group_selector_a.selectpicker').removeProp('disabled').selectpicker('refresh');
	$('#group_selector_button').removeProp('disabled');
});

/**
    Data entry column width dynamic adjustment
**/
function setDataEntryColumnWidth(width){
    let cookieKey = 'sres_column_entry_roll_' + ENV['COLUMN_UUID'] + '_data_entry_width';
    switch (width) {
        case '5vw':
        case '25vw':
        case '50vw':
        case '75vw':
            $('#roll_table thead .sres-rollview-dataentry').width(width);
            Cookies.set(cookieKey, width);
            break;
    }
}
$(document).on('click', '.sres-data-entry-width-adjust', function(){
    let adjustment = $(this).attr('data-sres-width-adjust');
    setDataEntryColumnWidth(adjustment);
});
$(document).on('init.dt', '#roll_table', function(){
    let cookieKey = 'sres_column_entry_roll_' + ENV['COLUMN_UUID'] + '_data_entry_width';
    let widthCookie = Cookies.get(cookieKey);
    if (typeof widthCookie !== 'undefined') {
        setDataEntryColumnWidth(widthCookie);
    }
});

