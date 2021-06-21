SRES_SAVE_BTN_SAVING_SPAN = '<span class="fa fa-circle-notch spinning sres-addvalue-btn-save-status"></span>';
SRES_SAVE_BTN_SAVING_CLASS = 'fa fa-circle-notch spinning sres-addvalue-btn-save-status';
SRES_SAVE_BTN_SAVED_SPAN = '<span class="fa fa-check sres-addvalue-btn-save-status"></span>';

function postLogEvent(e, d, a) {
	$.ajax({
		method: 'POST',
		url: ENV['POST_LOG_EVENT_ENDPOINT'],
		data: {
			e: e,
			d: d,
			a: a,
            i: ENV['auth_user']
		}
	});
}$(document).on('mouseup', 'a', function(){
	postLogEvent('a', $(this).attr('href'), 'click');
});
$(document).ready(function(){
	postLogEvent('portal', '', 'open');
});

/**
    Input edit tracking
**/
var sresInputsDirty = [];
$(document).on('input click change', '.sres-input-container input, .sres-input-container button, .sres-input-container textarea, .sres-input-container select, .sres-input-container canvas, .sres-input-container .sres-addvalue-tinymce-simple[contenteditable=true]', function(event, eventParams){
    // Figure out when to skip
    if ($(this).hasClass('sres-ignore-dirty')) return;
    if (eventParams && eventParams.doNotProcessDirty) return;
    if ($(this).parents("[data-sres-multientry-render-calculated-value='yes']").length) return;
    // Continue
    let columnUuid = $(this).parents('.sres-input-container').attr('data-sres-columnuuid');
    if (columnUuid && sresInputsDirty.indexOf(columnUuid) == -1) {
        sresInputsDirty.push(columnUuid);
    }
});
window.onbeforeunload = function(e) {
    if (sresInputsDirty.length > 0) {
        let dialogText = 'You may have some unsaved changes. Are you sure you want to leave before saving them?';
        e.returnValue = dialogText;
        return dialogText;
    } else {
        return null;
    }
};

/**
    Data saving
**/
function sendData(identifier, dataToSend, updateTarget, tableuuid, columnuuid, successCallback) {
    // update button with spinner
    let saveBtn = $("button.sres-addvalue-btn-save[data-sres-columnuuid=" + columnuuid + "]");
    saveBtn.append(SRES_SAVE_BTN_SAVING_SPAN);
    // get report index if operating in multiple report mode
    let multipleReportTogglerContainer = $(".sres-report-toggler-container[data-sres-columnuuid='" + columnuuid + "'][data-sres-identifier='" + identifier + "']");
    let multipleReportCurrentIndex = null;
    if (multipleReportTogglerContainer) {
        multipleReportCurrentIndex = multipleReportTogglerContainer.attr('data-sres-report-toggler-current-index');
    }
    // get panel number
    let panelNumber = saveBtn.parents('[data-sres-panel-number]').attr('data-sres-panel-number');
    // send
    $.post(
        ENV['SEND_DATA_ENDPOINT'],
        {
            'c': columnuuid,
            'i': identifier,
            'd': dataToSend,
            'ri': multipleReportCurrentIndex
        },
        function(data){
            let saveBtn = $("button.sres-addvalue-btn-save[data-sres-columnuuid=" + columnuuid + "]");
            saveBtn.find('.sres-addvalue-btn-save-status').remove();
            data = JSON.parse(data);
            if (data.success == true || data.success == 'true') {
                // Notify toast
                $.notify({message:'Saved'}, {type:'success'});
                // Button flash
                saveBtn.append(SRES_SAVE_BTN_SAVED_SPAN).addClass('btn-success');
                setTimeout(function(){
                    saveBtn
                        .removeClass('btn-success')
                        .find('.sres-addvalue-btn-save-status').remove(); 
                }, 2000);
                // Update update target
                /*if (typeof updateTarget != 'undefined' && updateTarget != null) {
                    updateTarget.addClass('bg-success');
                    setTimeout(function(){ updateTarget.removeClass('bg-success'); }, 2000);
                }*/
                // Update others
                if (typeof successCallback != 'undefined') {
                    successCallback(identifier, dataToSend, tableuuid, columnuuid, data);
                }
                populateInputFields(
                    "div.sres-input-container[data-sres-identifier='" + identifier + "'][data-sres-columnuuid='" + columnuuid + "']", 
                    dataToSend, 
                    identifier
                );
                sresInputsDirty = sresInputsDirty.filter(function(e){return e !== columnuuid});
                $(document).trigger('sres:datasaved', {
                    'identifier': identifier,
                    'dataToSend': dataToSend,
                    'tableuuid': tableuuid,
                    'columnuuid': columnuuid,
                    'payload': data,
                    'panelNumber': panelNumber
                });
            } else {
                $.notify({message:data.message}, {type:'danger'});
            }
        }
    ).fail(function(err){
        let saveBtn = $("button.sres-addvalue-btn-save[data-sres-columnuuid=" + columnuuid + "]");
        saveBtn.find('.sres-addvalue-btn-save-status').remove();
        saveBtn.addClass('btn-danger animated shake')
        setTimeout(function(){
            saveBtn.removeClass('btn-danger animated shake');
        }, 4000);
        $.notify({message:'Error saving data. ' + err.responseText}, {type:'danger'});
    });
};

/**
    Feedback
**/
$(document).on('click', '.sres-portal-feedback-close', function(){
    $('.sres-portal-feedback-container').addClass('d-none');
});
$(document).on('click', '#sres_portal_feedback_sleep', function(){
    // Register the sleep command
    Cookies.set('sres_portal_feedback_sleep_' + ENV['portal_uuid'], '1', { expires: 6 });
    // Close the prompt
    $('.sres-portal-feedback-close').trigger('click');
});
$(document).on('click', 'input[name=sres_portal_feedback_options]', function(){
    let selectedValue = $(this).val();
    // post to server
    $.ajax({
        url: ENV['POST_FEEDBACK_EVENT_ENDPOINT'],
        method: 'POST',
        data: {
            vote: selectedValue
        },
        success: function(data){
            data = JSON.parse(data);
            if (data.oid) {
                $('#portal_feedback_event_oid').val(data.oid);
            }
        }
    })
    // show followups
    $('[data-sres-followup-prompt-parent-option]').collapse('hide');
    $('[data-sres-followup-prompt-parent-option=' + selectedValue + ']').collapse('show');
    $('.sres-portal-feedback-submit').collapse('show');
    $('.sres-portal-feedback-followup-data[data-sres-followup-prompt-parent-option=' + selectedValue + ']').first().focus();
});
$(document).on('click', '#sres_portal_feedback_submit', function(){
    let selectedValue = $('input:checked[name=sres_portal_feedback_options]').val();
    let feedbackEventOid = $('#portal_feedback_event_oid').val();
    let followupData = {};
    $('.sres-portal-feedback-followup-data[data-sres-followup-prompt-parent-option=' + selectedValue + ']').each(function(){
        let name = $(this).attr('data-sres-followup-prompt-name');
        followupData[name] = $(this).val();
    });
    $.ajax({
        url: ENV['POST_FEEDBACK_EVENT_ENDPOINT'],
        method: 'POST',
        data: {
            vote: selectedValue,
            oid: feedbackEventOid,
            data: JSON.stringify(followupData)
        },
        success: function(data){
            data = JSON.parse(data);
        }
    })
    $('.sres-portal-feedback-close').trigger('click');
});
$(document).ready(function(){
    if (ENV['DISABLE_FEEDBACK_REQUEST']) {
        return;
    }
    if (typeof Cookies.get('sres_portal_feedback_sleep_' + ENV['portal_uuid']) !== 'undefined') {
        return;
    }
    let intervalSinceLastFeedbackEvent = parseInt($('#days_since_last_feedback_event').val());
    if (intervalSinceLastFeedbackEvent == -1 || intervalSinceLastFeedbackEvent > 7) {
        window.setTimeout(function(){
            $('.sres-portal-feedback-container').removeClass('d-none').addClass('animated bounceInLeft');
        }, 15000);
    }
});

/**
    Collapsible panels
**/
$(document).on('sres:updatecollapsiblepanelstate', function(event, args){
    $('.sres-panel-collapse-shown, .sres-panel-collapse-collapsed').each(function(){
        $(this).children().each(function(index, element){
            let parent = $(this).parent();
            let mode = $(this).parent().attr('data-sres-collapsible-mode');
            // don't touch first child unless it's in a linked panel
            if (index == 0 && mode != 'linked') return;
            // don't touch scripts and styles
            if ($(this).prop('tagName') == 'SCRIPT' || $(this).prop('tagName') == 'STYLE') return;
            // don't touch other sets if set number is defined in args
            let setNumber = parent.attr('data-sres-collapsible-panel-set-number');
            if (typeof args !== 'undefined' && args.setNumber != setNumber) return;
            // update display
            if (parent.hasClass('sres-panel-collapse-shown')) {
                $(this).show('fast');
            } else if (parent.hasClass('sres-panel-collapse-collapsed')) {
                $(this).hide('fast');
            }
        });
    });
});
$(document).ready(function(){
    $(document).trigger('sres:updatecollapsiblepanelstate');
});
$(document).on('click', '.sres-collapsible-panel-controller-expand, .sres-collapsible-panel-controller-collapse', function(){
    let operation = $(this).hasClass('sres-collapsible-panel-controller-expand') ? 'expand' : 'collapse';
    let setNumber = $(this).attr('data-sres-collapsible-panel-set-number');
    if (operation == 'expand') {
        $(".sres-collapsible-panel-controller-expand[data-sres-collapsible-panel-set-number='" + setNumber + "']").addClass('d-none');
        $(".sres-collapsible-panel-controller-collapse[data-sres-collapsible-panel-set-number='" + setNumber + "']").removeClass('d-none');
        $(".sres-collapsible-panel[data-sres-collapsible-panel-set-number='" + setNumber + "']").addClass('sres-panel-collapse-shown').removeClass('sres-panel-collapse-collapsed');
    } else {
        $(".sres-collapsible-panel-controller-expand[data-sres-collapsible-panel-set-number='" + setNumber + "']").removeClass('d-none');
        $(".sres-collapsible-panel-controller-collapse[data-sres-collapsible-panel-set-number='" + setNumber + "']").addClass('d-none');
        $(".sres-collapsible-panel[data-sres-collapsible-panel-set-number='" + setNumber + "']").addClass('sres-panel-collapse-collapsed').removeClass('sres-panel-collapse-shown');
    }
    // update
    $(document).trigger('sres:updatecollapsiblepanelstate', {setNumber: setNumber});
});

/**
    List of students
**/
function sresToggleStudentListPane(visible) {
    if (visible) {
        $('.sres-pane').addClass('open');
    } else {
        $('.sres-pane').removeClass('open');
    }
}
$(document).on('click', '.sres-pane-close', function(){
    sresToggleStudentListPane(false);
});
$(document).on('click', '#show_student_list', function(){
    sresToggleStudentListPane(true);
    $('#student_list_container').html('');
    $('#student_list_container_loading').removeClass('d-none');
    $.ajax({
        url: ENV['GET_STUDENTS_ENDPOINT'],
        success: function(data) {
            data = JSON.parse(data);
            students = [];
            let template = Handlebars.compile(document.getElementById("student_list_item_template").innerHTML);
            let url = new URL(window.location);
            if (data.students.length > 0) {
                $('#student_list_container_loading').addClass('d-none');
                $('#student_list_container').html('');
                data.students.forEach(function(student){
                    let params = url.searchParams;
                    params.set('identifier', student.sid);
                    url.search = params.toString();
                    $('#student_list_container').append(template({
                        displayName: student.preferred_name + ' ' + student.surname,
                        sid: student.sid,
                        displaySid: student.display_sid,
                        href: url.toString()
                    }));
                });
            } else {
                $('#student_list_container').html('No students found.');
                $('#student_list_container_loading').addClass('d-none');
            }
        }
    });
});
$(document).on('click', '.sres-student-list-item', function(){
    $(this).append('<span class="fa fa-circle-notch spinning ml-2"></span>');
});

