
/**
    Results table display
**/
var ignore_send_to = [];
var oTable = '';
$(document).ready(function() {
    oTable = $("#results_dump").DataTable({
        dom: "<'row'<'col'l><'col'f>><'row'<'col'i><'col'p>><'row'<'col'tr>><'row'<'col'B>>",
        buttons: [{
            extend: 'csvHtml5',
            text: 'Download as CSV'
        }],
        order: [[ 4, 'asc' ]]
    });
    $(document).on("click", "#results_dump > tbody > tr", function() {
        $(this).find("input:checkbox[name=confirm_send_to]").trigger('click');
    });
    $(document).on("click", "input:checkbox[name=confirm_send_to]", function(e) {
        updatePreviewEmailIgnoreSendToNotice();
        e.stopPropagation();
        var sid = $(this).val();
        if ($(this).prop('checked')) {
            ignore_send_to.splice(ignore_send_to.indexOf(sid), 1);
        } else {
            ignore_send_to.push(sid);
        }
    });
});

/**
    Preview and send functionality
**/
$(document).ready(function(){
    $('#preview_selector').chosen({width: '100%', search_contains: true});
});
$(document).on('change', '#preview_selector', function(){
    var display_sid = $(this).val();
    console.log('display_sid', display_sid);
    // Load via AJAX
    $("#preview_email_loading").collapse('show');
    $.ajax({
        url: ENV['RUN_PERSONALISED_MESSAGE_ENDPOINT'].replace('__mode__', 'preview'),
        method: 'POST',
        data: {
            'identifiers[]': display_sid
        },
        success: function(data){
            data = JSON.parse(data);
            console.log(data);
            data = data[0];
            if (data.success == 'true' || data.success == true) {
                // Show
                $("#preview_email_container").removeClass('d-none');
                // From
                if (data.contact_types.includes('email')) {
                    $("#preview_email_from").html(data.email.details.sender_name + ' &lt;' + data.email.details.sender_email + '&gt;').parents('tr').removeClass('d-none');
                } else {
                    $("#preview_email_from").parents('tr').addClass('d-none');
                }
                // Target
                let toHtml = '';
                toHtml += data.email.target.name
                if (data.contact_types.includes('email')) {
                    if (Array.isArray(data.email.target.email) || typeof data.email.target.email == 'object') {
                        toHtml += ' &lt;' + data.email.target.email.join('&gt;, &lt;') + '&gt;';
                    } else {
                        toHtml += ' &lt;' + data.email.target.email + '&gt;';
                    }
                }
                data.contact_types_display.forEach(function(contactTypeDisplay){
                    toHtml += '<span class="badge badge-info ml-2">' + contactTypeDisplay + '</span>';
                });
                $("#preview_email_to").html(toHtml);
                // Extra email targets
                ['reply_to','cc','bcc'].forEach(function(field){
                    if (data.email.details[field]) {
                        $("#preview_email_" + field).html(data.email.details[field]).parent().removeClass('d-none');
                    } else {
                        $("#preview_email_" + field).html('').parent().addClass('d-none');
                    }
                });
                // Message subject
                $("#preview_email_subject").html(data.email.subject);
                // Message HTML body
                if (data.contact_types.includes('email')) {
                    $("#preview_email_body").html(data.email.body).parents('tr').removeClass('d-none');
                } else {
                    $("#preview_email_body").html('').parents('tr').addClass('d-none');
                }
                // Plaintext message
                if (data.contact_types.includes('canvasinbox')) {
                    $("#preview_email_body_plaintext").html(data.email.body_plaintext.replace(/\n/g, '<br>')).parents('tr').removeClass('d-none');
                } else {
                    $("#preview_email_body_plaintext").html('').parents('tr').addClass('d-none');
                }
                // Attachments
                if (data.email.attachments.length > 0) {
                    $("#preview_email_attachments").html('').parent().removeClass('d-none');
                    data.email.attachments.forEach(function(attachment){
                        $("#preview_email_attachments").append('<a href="' + attachment.filename + '">' + attachment.original_filename + '</a>');
                    });
                } else {
                    $("#preview_email_attachments").parent().addClass('d-none');
                }
            } else {
                $.notify({message:"Sorry, an error occurred. " + data.message}, {type:'danger'});
            }
        },
        error: function(data){
            console.log(data);
            $.notify({message:"Sorry, an error occurred. " + data.statusText}, {type:'danger'});
        },
        complete: function(){
            setTimeout(function(){
                $("#preview_email_loading").collapse('hide');
            }, 500);
        }
    });
    /*$('div[id^=preview_email_display_]').hide();
    $('div[id=preview_email_' + display_sid + ']').show();*/
    $('div[id^=preview_sms_display_]').hide();
    $('div[id="preview_sms_' + 'display_' + display_sid + '"]').show();
    updatePreviewEmailIgnoreSendToNotice();
});
function updatePreviewEmailIgnoreSendToNotice() {
    let sid = $("#preview_selector option:selected").attr('data-sres-sid');
    if (oTable.$("input:checkbox[name=confirm_send_to][value='" + sid + "']:checked").length > 0) {
        $("#preview_email_ignoring_send_to").addClass('d-none');
    } else {
        $("#preview_email_ignoring_send_to").removeClass('d-none');
    }
}
$(document).on('click', '#preview_prev, #preview_next', function(){
    let currentIndex = $('#preview_selector').prop('selectedIndex');
    let numberOfStudents = $('#preview_selector').prop('length');
    let direction = $(this).attr('id') == 'preview_prev' ? -1 : 1;
    let newIndex = currentIndex + direction;
    if (newIndex < 0) {
        newIndex = numberOfStudents - 1;
    } else if (newIndex > numberOfStudents - 1) {
        newIndex = 0;
    }
    $('#preview_selector :nth-child(' + (newIndex + 1) + ')').prop('selected', true);
    $('#preview_selector').trigger('change').trigger('chosen:updated');
});
$(document).ready(function(){
    $('#preview_selector').trigger('change');
});

/**
    Sending
**/
var sendingStatusMaxCount = 0;
var sendingStatusCurrentCount = 0;
$(document).on('click', '#modal_send_message_confirmation_send', function(){
    /** SEND MESSAGES !! **/
    // change some ui
    $('#message_preview_container').collapse('hide');
    $('#send_message_button').prop('disabled', true);
    $('#message_send_parent_container').removeClass('d-none');
    $('#modal_send_message_confirmation').modal('hide');
    // sending status modal
    /*$("#sending_status_modal").modal('show');
    if (ENV['contact_type'].indexOf("email") !== -1) {
        $("#sending_status_container_email").removeClass("d-none");
    }
    if (ENV['contact_type'].indexOf("sms") !== -1) {
        $("#sending_status_container_sms").removeClass("d-none");
    }*/
    // set some counters
    sendingStatusMaxCount = oTable.$("input:checkbox[name=confirm_send_to]:checked").length;
    sendingStatusCurrentCount = 0;
    // iterate through students
    allIdentifiers = [];
    oTable.$("input:checkbox[name=confirm_send_to]").each(function(){
        let checked = $(this).prop('checked');
        let identifier = $(this).attr('data-sres-identifier');
        //console.log(checked, identifier);
        let statusMessage = '<div class="sres-filter-send-result" data-sres-identifier="' + identifier + '">';
        statusMessage += identifier + ': <span class="sres-filter-send-result-status">';
        //statusMessage += checked ? 'Requesting...' : 'Skipped';
        statusMessage += checked ? 'Message added to queue.' : 'Skipped.';
        statusMessage += '</span></div>';
        $('#message_send_log_container').append(statusMessage);
        if (checked) {
            //queueEmailSend([identifier]);
            allIdentifiers.push(identifier);
        }
    });
    $.ajax({
		url: ENV['RUN_PERSONALISED_MESSAGE_ENDPOINT'].replace('__mode__', 'queue'),
		method: 'POST',
        data: {
            'identifiers[]': allIdentifiers
        },
        success: function(data){
            data = JSON.parse(data);
            console.log(data);
            $('#message_send_log_queued').removeClass('d-none');
            $('#message_send_log_time_estimate').text(Math.ceil(data.identifier_count * 3 / 60 + 1));
            $('#message_send_log_email_notification').text(data.sender_email);
        },
        error: function(data){
            console.log(data);
            $('#message_send_log_container').prepend('<div class="alert alert-danger">There was an unexpected error during message queueing</div>');
        }
    });
    // add run history
    /*$.ajax({
        url: ENV['ADD_RUN_HISTORY_ENDPOINT'],
        method: 'POST'
    })*/
});
$(document).on('click', '#send_message_button', function(){
    var recipientCount = oTable.rows().count() - ignore_send_to.length;
    $("#modal_send_message_confirmation_recipient_count").html(recipientCount);
    if (recipientCount == 0) {
        $("#modal_send_message_confirmation_send").addClass('d-none');
    }
    $("#modal_send_message_confirmation").modal('show');
});

/**
	Email send
**/
function queueEmailSend(identifiers) {
    let queuedIdentifiers = identifiers;
	$.ajaxq('sres_filter_email_send', {
		url: ENV['RUN_PERSONALISED_MESSAGE_ENDPOINT'].replace('__mode__', 'send'),
		method: 'POST',
        data: {
            'identifiers[]': queuedIdentifiers
        },
		timeout: 10000,
		success: function(returnedData) {
            //console.log(returnedData, queuedIdentifiers);
            let data = [];
			try {
                data = JSON.parse(returnedData);
			} catch(e) {
                queuedIdentifiers.forEach(function(queuedIdentifier){
                    data.push({ 
                        email: { 
                            send_result: { 
                                success: false,
                                message: "Undefined error"
                            }
                        },
                        identifier: queuedIdentifier
                    });
                });
			}
            data.forEach(function(record, i){
                //console.log(record);
                if (record.email.send_result.success == 'true' || record.email.send_result.success == true) {
                    $("[class='sres-filter-send-result'][data-sres-identifier='" + record.identifier + "'] .sres-filter-send-result-status").html(record.email.send_result.target + ' <span class="badge badge-success">OK</span>');
                } else {
                    $("[class='sres-filter-send-result'][data-sres-identifier='" + record.identifier + "'] .sres-filter-send-result-status").html('<span class="badge badge-warning">Error</span> ' + record.email.send_result.messages.join(' ') + (typeof record.messages !== 'undefined' ? record.messages.join(' ') : '') );
                }
                sendingStatusCurrentCount++;
                $("#sending_status_progressbar").css('width', (sendingStatusCurrentCount / sendingStatusMaxCount * 100) + '%').html(sendingStatusCurrentCount + ' of ' + sendingStatusMaxCount);
                if (sendingStatusCurrentCount == sendingStatusMaxCount) {
                  $('#send_complete').css('display','inline')
                }
            });
		},
		error: function(data) {
			try {
				response = JSON.parse(data.responseText);
				console.log(response, data);
			} catch(e) {
				response = {
					message: ""
				}
			}
            identifiers.forEach(function(identifier, i){
                $("[class='sres-filter-send-result'][data-sres-identifier='" + identifier + "'] .sres-filter-send-result-status").html('[Unexpected error, status ' + data.status + '] ' + response.message).addClass('bg-danger');
            });
        }
	});
}

/**
	SMS send
**/
function queueSmsSend(filterUuid, identifier, mainTableUuid) {
	$.ajaxq('sres_filter_sms_send', { // TODO
		url: 'Filter.cfc?method=run_sms&filteruuid=' + filterUuid + '&identifier=' + identifier + '&main_table_uuid=' + mainTableUuid + '&mode=send',
		type: 'get',
		timeout: 10000,
		success: function(data) {
			try {
				data = JSON.parse(data);
				//console.log(data);
			} catch(e) {
				data = { 
					sms: { 
						sendresult: { 
							success: false,
							message: "Undefined error"
						}
					}
				};
			}
			if (data.sms.sendresult.success == 'true' || data.sms.sendresult.success == true) {
				$("[class='sres-filter-sms-send-result'][data-sres-identifier='" + identifier + "'] .sres-filter-sms-send-result-status").html('[Dispatched: ' + data.sms.sendresult.message + ']');
			} else {
				$("[class='sres-filter-sms-send-result'][data-sres-identifier='" + identifier + "'] .sres-filter-sms-send-result-status").html('[Error. ' + data.sms.sendresult.message + (typeof data.message !== 'undefined' ? data.message : '') + ']').addClass('bg-danger');
			}
			sendingStatusCurrentCountSMS++;
			$("#sending_status_progressbar_sms").css('width', (sendingStatusCurrentCountSMS / sendingStatusMaxCount * 100) + '%').html(sendingStatusCurrentCountSMS + ' of ' + sendingStatusMaxCount);
			if (sendingStatusCurrentCountSMS == sendingStatusMaxCount) {
                $('#send_complete_sms').removeClass("hidden");
			}
		},
		error: function(data) {
			try {
				response = JSON.parse(data.responseText);
				console.log(response, data);
			} catch(e) {
				response = {
					message: ""
				}
			}
			$("[class='sres-filter-sms-send-result'][data-sres-identifier='" + identifier + "'] .sres-filter-sms-send-result-status").html('[Unexpected error, status ' + data.status + '] ' + response.message).addClass('bg-danger');
		}
	});
}

/**
    Schedule send
**/
$(document).on('click', '#schedule_message_button', function(){
    $('#modal_schedule_message_ignorelist_notice').addClass('d-none');
    $('#modal_schedule_message_date').attr('max', moment().add(72, 'hours').format("YYYY-MM-DD"));
    $('#modal_schedule_message_date').attr('min', moment().format("YYYY-MM-DD"));
    // get ignorelist
    ignorelistIdentifiers = [];
    oTable.$("input:checkbox[name=confirm_send_to]").each(function(){
        let identifier = $(this).attr('data-sres-identifier');
        if (!$(this).prop('checked')) {
            ignorelistIdentifiers.push(identifier);
        }
    });
    if (ignorelistIdentifiers.length > 0) {
        $('#modal_schedule_message_ignorelist_notice').removeClass('d-none');
        $('#modal_schedule_message_ignorelist_count').html(ignorelistIdentifiers.length);
    }
    // show
    $('#modal_schedule_message').modal('show');
});
$(document).on('click', '#modal_schedule_message_do', function(){
    $('#modal_schedule_message_error').addClass('d-none').html('');
    // work out if allowed
    let dt = moment($('#modal_schedule_message_date').val() + ' ' + $('#modal_schedule_message_time').val());
    if (dt.isValid()) {
        if (dt.isBetween( moment().add(10, 'minutes'), moment().add(72, 'hours') )) {
            // good
            // check reminder, if set
            let reminderHours = parseFloat($('#modal_schedule_message_reminder').val());
            if (reminderHours > 0) {
                if ( (dt.diff(moment(), 'seconds') - (3600 * reminderHours) ) < 0) {
                    $('#modal_schedule_message_error')
                        .removeClass('d-none')
                        .html('Cannot set this schedule because the reminder will occur in the past.');
                    return;
                }
            }
            // call endpoint to schedule
            allIdentifiers = [];
            ignorelistIdentifiers = [];
            oTable.$("input:checkbox[name=confirm_send_to]").each(function(){
                let identifier = $(this).attr('data-sres-identifier');
                if ($(this).prop('checked')) {
                    allIdentifiers.push(identifier);
                } else {
                    ignorelistIdentifiers.push(identifier);
                }
            });
            $.ajax({
                url: ENV['RUN_PERSONALISED_MESSAGE_ENDPOINT'].replace('__mode__', 'schedule'),
                method: 'POST',
                data: {
                    'identifiers[]': allIdentifiers,
                    'ignorelist_identifiers[]': ignorelistIdentifiers,
                    'scheduled_ts_utc': dt.utc().format(),
                    'reminder_hours_advance': $('#modal_schedule_message_reminder').val()
                },
                success: function(data){
                    data = JSON.parse(data);
                    //console.log(data);
                    if (data.success) {
                        $('#modal_schedule_message_success')
                            .removeClass('d-none')
                            .html('Schedule set successfully. Please wait while we refresh this page...');
                        window.location.reload();
                    } else {
                        $('#modal_schedule_message_error')
                            .removeClass('d-none')
                            .html('There was an unexpected error scheduling this filter.');
                    }
                }
            });
        } else {
            $('#modal_schedule_message_error')
                .removeClass('d-none')
                .html('Could not set schedule. Schedules can only be set between 10 minutes and 72 hours from now.');
        }
    } else {
        $('#modal_schedule_message_error')
            .removeClass('d-none')
            .html('The date and/or time do not appear to be valid.');
    }
});





