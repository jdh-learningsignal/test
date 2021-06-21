var SRES_SAVE_BTN_SAVING_CLASS = 'fa fa-circle-notch spinning sres-addvalue-btn-save-status';
var SRES_SAVE_BTN_SAVED_CLASS = 'fa fa-check sres-addvalue-btn-save-status';
var SRES_SAVE_BTN_ERROR_CLASS = 'fa fa-exclamation-triangle sres-addvalue-btn-save-status';

function genericSuccessCallback(identifier, dataToSend, tableuuid, columnuuid, payload) {
    //console.log('in genericSuccessCallback', identifier, dataToSend, tableuuid, columnuuid, payload);
    $.notify(
        {message:'Successfully saved to server' + (typeof identifier != 'undefined' && identifier !== null ? ' for ' + Handlebars.escapeExpression(identifier) : '') +  '.'},
        {type:'success'}
    );
    if (payload.apply_to_others.success) {
        otherTargets = [];
        payload.apply_to_others.other_targets.forEach(function(otherTarget){
            otherTargets.push(otherTarget.name + ' (' + otherTarget.sid + ')');
        });
        $.notify(
            {message:'Also applied to ' + otherTargets.join(', ')},
            {type:'success'}
        );
    }
    let parentSelector = 'div.sres-input-container[data-sres-identifier="' + identifier + '"][data-sres-columnuuid=' + columnuuid + ']';
    if (typeof payload.load_existing_data_mode !== 'undefined' && payload.load_existing_data_mode == 'fresh') {
        clearInputFields(parentSelector);
        $.notify(
            { message: 'Inputs will be cleared to allow a fresh set of data to be recorded.' },
            { type: 'info' }
        );
    } else {
        populateInputFields(parentSelector, dataToSend, identifier);
    }
    checkForBackups();
    $(document).trigger('sres:datasaved', {
        'identifier': identifier,
        'dataToSend': dataToSend,
        'tableuuid': tableuuid,
        'columnuuid': columnuuid,
        'payload': payload
    });
    removeFromDirtyInputs(columnuuid, identifier);
}

function genericErrorCallback(identifier, dataToSend, tableuuid, columnuuid, err) {
    let isBackedUp = backupExists(tableuuid, columnuuid, identifier);
    let statusMessage = '';
    switch (err.status) {
        case 429:
            statusMessage = 'Too many requests to save data within a certain amount of time than allowed. Please try again later.';
            break;
        case 400:
            statusMessage = 'A security token is missing or the server could not understand the request.';
            break;
        default:
            statusMessage = (typeof err['statusText'] != 'undefined' ? ' ' + err['statusText'] + '.' : '');
    }
    $.notify(
        {
            message: 'Error while saving to server. '
                + statusMessage
                + (isBackedUp ? ' A local backup has been saved to this device. You can re-attempt the save operation later from this backup.' : '')
        },
        {
            type:'danger'
        }
    );
    checkForBackups(identifier);
    $(document).trigger('sres:datasaveerror', {
        'identifier': identifier,
        'dataToSend': dataToSend,
        'tableuuid': tableuuid,
        'columnuuid': columnuuid,
        'err': err
    });
    // check if user is logged out
    $.ajax({
        url: ENV['PING_ENDPOINT'],
        success: function(data) {
            //console.log(data);
            // all fine
        },
        error: function(err) {
            //console.error(err);
            if (err.status == 401) {
                $.notify(
                    {
                        message: 'You appear to be logged out. Your session may have timed out for security reasons. <span class="sres-addvalue-headless-login sres-clickable text-primary">Click here to open a new tab and log in again</a>.'
                    },
                    {
                        type:'danger'
                    }
                );
            }
        }
    });
}
$(document).on('click', '.sres-addvalue-headless-login', function(){
    window.open( ENV['HEADLESS_LOGIN_ENDPOINT'] );
});

/**
    Input edit tracking
**/
var sresInputsDirty = [];
$(document).on('input click change', '.sres-input-container input, .sres-input-container button, .sres-input-container textarea, .sres-input-container select, .sres-input-container canvas, .sres-input-container .sres-addvalue-tinymce-simple[contenteditable=true]', function(event, eventParams){
    //console.log('something got dirty', eventParams, $(this));
    // Figure when to skip
    if ($(this).hasClass('sres-ignore-dirty')) return;
    if (eventParams && eventParams.doNotProcessDirty) return;
    if ($(this).parents("[data-sres-multientry-render-calculated-value='yes']").length) return;
    // Continue...
    let columnuuid = $(this).parents('.sres-input-container').attr('data-sres-columnuuid');
    let identifier = $(this).parents('.sres-input-container').attr('data-sres-identifier');
    let record = { columnuuid: columnuuid, identifier: identifier };
    if (columnuuid && identifier && sresInputsDirty.findIndex(function(e){return e.columnuuid == record.columnuuid && e.identifier == record.identifier}) == -1) {
        sresInputsDirty.push(record);
    }
});
window.onbeforeunload = function(e) {
    if (sresInputsDirty.length > 0) {
        let dialogText = 'You may have some unsaved changes. Are you sure you want to leave before saving them?';
        e.returnValue = dialogText;
        sresInputsDirty.forEach(function(dirt){
            $container = $('.sres-input-container[data-sres-identifier="' + dirt.identifier + '"][data-sres-columnuuid=' + dirt.columnuuid + ']').parent();
            $container.addClass('bg-warning-light');
            window.setTimeout(function(){
                $container.removeClass('bg-warning-light');
            }, 5000);
        });
        return dialogText;
    } else {
        return null;
    }
};
function removeFromDirtyInputs(columnuuid, identifier) {
    let record = { columnuuid: columnuuid, identifier: identifier };
    let index = sresInputsDirty.findIndex(function(e){return e.columnuuid == record.columnuuid && e.identifier == record.identifier});
    if (index > -1) {
        sresInputsDirty.splice(index, 1);
    }
}

/**
    Quickinfo 
**/
function updateQuickInfo(mode, identifier, originalHtml, attemptCount, restoreOriginal){
    // Set up attemptCount
    if (typeof attemptCount == 'undefined') {
        attemptCount = 0
    }
    // Restore originalHtml? If requested, or if attemptCount too high
    if (restoreOriginal == true || attemptCount > 5) {
        switch (mode) {
            case 'roll':
                $('tr[data-sres-identifier="' + identifier + '"] td[data-sres-role=quickinfo]').html(originalHtml);
                break;
            case 'single':
                $('.sres-quickinfo-container').html(originalHtml);
                break;
        }
        return;
    }
    // Otherwise request quickinfo
    $.ajax({
        url: ENV['GET_QUICK_INFO_ENDPOINT'].replace('__mode__', mode).replace('__identifier__', identifier).replace('__wait__', 'wait'),
        method: 'GET',
        statusCode: {
            202: function() {
                // request again
                //console.log('waiting again', 1000 + (attemptCount * 1000), identifier, originalHtml, attemptCount + 1);
                setTimeout(updateQuickInfo, 1000 + (attemptCount * 1000), mode, identifier, originalHtml, attemptCount + 1);
            },
            200: function(data) {
                //console.log(data);
                data = JSON.parse(data);
                let $target = null;
                switch (mode) {
                    case 'roll':
                        $target = $('tr[data-sres-identifier="' + data.identifier + '"] td[data-sres-role=quickinfo]');
                        break;
                    case 'single':
                        $target = $('.sres-quickinfo-container');
                        break;
                }
                $target
                    .html(data.data)
                    .addClass('animated flash');
                setTimeout(function(){
                    $target.removeClass('animated flash');
                }, 1000);
            }
        },
        error: function(err) {
            // restore original
            updateQuickInfo(mode, identifier, originalHtml, null, true);
        }
    });
}

