function clearInputFields(parentSelector) {
    if (typeof parentSelector == 'undefined' || parentSelector == null) {
        parentSelector = '#apply_data_to_all_modal';
    }
    let fields = $(parentSelector + " [data-sres-field]");
    for (var i = 0; i < fields.length; i++) {
        let fieldId = $(fields[i]).attr('id');
        switch ($(fields[i]).attr('data-sres-field')) {
            case 'select':
                $(fields[i]).find("button[data-sres-value]").each(function() {
                    $(this).removeClass('btn-primary').addClass('btn-outline-primary');
                });
                break;
            case 'dropdown':
                $(fields[i]).find("select").val('').selectpicker('refresh');
                break;
            case 'regex':
            case 'regex-long':
                $(fields[i]).val('');
                break;
            case 'html-simple':
                try {
                    tinymce.get(fieldId).setContent('');
                } catch(e) {
                }
                break;
            case 'audio-recording':
                $(fields[i]).html('');
                $(fields[i]).attr('data-sres-saved-recordings', JSON.stringify([]));
                break;
            case 'sketch-small':
                let clearButton = $(fields[i]).siblings('button.sres-sketch-clear');
                let clearButtonDisabledState = clearButton.prop('disabled');
                clearButton.prop('disabled', false).trigger('click').prop('disabled', clearButtonDisabledState);
                break;
        }
    }
    $(".sres-multientry-more.collapse button").removeClass('btn-primary').addClass('btn-outline-primary');
}

function populateInputFields(parentSelector, data, identifier, doNotMakeInputsDirty) {
    if (typeof parentSelector == 'undefined' || parentSelector == null) {
        parentSelector = '#apply_data_to_all_modal';
    }
    //console.log('populateInputFields', parentSelector, data, identifier, doNotMakeInputsDirty);
    // First clear
    clearInputFields(parentSelector);
    // Then load
    var columnType = $(parentSelector).attr('data-sres-column-type');
    let columnUuid = $(parentSelector).attr('data-sres-columnuuid');
    if (typeof columnUuid == 'undefined') {
        columnUuid = ENV.column_uuid;
    }
    if (typeof identifier == 'undefined') {
        identifier = $(parentSelector).attr('data-sres-identifier');
    }
    //if ($(parentSelector).parents('td[data-sres-editor-mode]').attr('data-sres-editor-mode') == 'mark-inline' || columnType == 'teacherallocation') {
    if (columnType == 'mark' || columnType == 'teacherallocation') {
        // Special case for mark-inline
        try {
            var dataParsed = JSON.parse(data);
            data = [dataParsed, data];
        } catch(e) {
            if (typeof data == 'string') {
                data = [data, data];
            }
        }
    } else if (columnType == 'signinout' || columnType == 'signinoutmemory') {
        try {
            data = JSON.parse(data);
            if (data.hasOwnProperty('in') && !data.hasOwnProperty('out')) {
                data = [data['in']];
            } else {
                data = [''];
            }
        } catch(e) {
            data = [data];
        }
    } else {
        try {
            if (typeof data == 'number') {
                data = [data];
            } else if (typeof data == 'string' && JSON.parse(data) == parseFloat(data)) {
                data = [data];
            } else {
                data = JSON.parse(data);
            }
        } catch(e) {
            if (typeof data == 'string') {
                data = [data];
            } else {
                data = [];
            }
        }
    }
    var fields = $(parentSelector + " [data-sres-field]");
    //console.log('fields', fields, 'data', data);
    for (var i = 0; i < fields.length; i++) {
        var currentDataElement = "";
        let fieldId = $(fields[i]).attr('id');
        if (i < data.length) {
            // Try populate data
            currentDataElement = data[i];
        } else {
            // Data element doesn't exist
        }
        if (!Array.isArray(currentDataElement)) {
            // Simple data, convert to array for this next step
            currentDataElement = [data[i]];
        }
        switch ($(fields[i]).attr('data-sres-field')) {
            case 'select':
                for (var j = 0; j < currentDataElement.length; j++) {
                    //$(fields[i]).find("button[data-sres-value='" + currentDataElement[j] + "']").trigger('click');
                    $(fields[i]).find('button[data-sres-value="' + currentDataElement[j] + '"]').addClass('btn-primary').removeClass('btn-outline-primary').trigger('sres:updatemorefields');
                }
                break;
            case 'dropdown':
                for (var j = 0; j < currentDataElement.length; j++) {
                    $(fields[i]).find('select option[data-sres-value="' + currentDataElement[j] + '"]').prop('selected', true);
                }
                $(fields[i]).find("select").selectpicker('refresh').trigger( 'change', { doNotProcessDirty: doNotMakeInputsDirty } );
                break;
            case 'regex':
            case 'regex-long':
                $(fields[i]).val(data[i]);
                break;
            case 'html-simple':
                if (typeof data[i] !== 'undefined') {
                    tinymce.get(fieldId).setContent(data[i]);
                }
                break;
            case 'slider':
                //console.log('populateInputFields update slider', i, data, identifier, ENV.column_uuid);
                let rs = $(fields[i]).data('ionRangeSlider');
                if (typeof data[i] == 'undefined') {
                    //updateRangeSlider(ENV.column_uuid, i, identifier, 0);
                    updateRangeSlider(columnUuid, i, identifier, 0, true);
                } else {
                    //console.log('calling updateRangeSlider', ENV.column_uuid, i, identifier, data[i]);
                    //updateRangeSlider(ENV.column_uuid, i, identifier, data[i]);
                    updateRangeSlider(columnUuid, i, identifier, data[i], true);
                }
                break;
            case 'audio-recording':
                $(fields[i]).attr('data-sres-saved-recordings', JSON.stringify(data[i]));
                break;
            case 'sketch-small':
                $(fields[i]).val(data[i]);
                let id = $(fields[i]).siblings('.sres-sketch-container').find('.sres-sketch-area.sres-sketch-small').attr('id');
                loadDataToCanvas(id);
                break;
            case 'toggle':
                var toggleState = data[i] == '' || data[i] == null ? 'off' : 'on';
                //console.log('toggle', data[i], toggleState);
                $(fields[i])
                    .attr('data-sres-suspend-send-data', 'on')
                    .bootstrapToggle(toggleState)
                    .attr('data-sres-suspend-send-data', 'off');
        }
    }
    // Trigger any update handlers
    $(document).trigger('sres:audiorecordingsloadaudio');
}

/**
    Multiple reports toggler
**/
function multipleReportsTogglerShowReportByIndex(togglerContainer, index, forceCacheRefresh){
    
    function showMultipleReportsReport(togglerContainer, index, count, identifier, columnUuid, data) {
        // update input fields
        populateInputFields("div.sres-input-container[data-sres-identifier='" + identifier + "'][data-sres-columnuuid='" + columnUuid + "']", data, identifier, true);
        // update toggler display
        togglerContainer.attr('data-sres-report-toggler-current-index', index);
        togglerContainer.find('.sres-report-toggler-current-display').val(index);
        if (count) {
            togglerContainer.attr('data-sres-report-toggler-max-index', count);
            togglerContainer.find('.sres-report-toggler-max-display').html(count);
        }
    }
    
    let identifier = togglerContainer.attr('data-sres-identifier');
    let columnUuid = togglerContainer.attr('data-sres-columnuuid');
    
    // see if in cached
    let cachedReports = togglerContainer.data('sres-cached-reports');
    if (forceCacheRefresh && forceCacheRefresh == true) {
        cachedReports = null;
    }
    if (cachedReports && index <= cachedReports.length) {
        // probably ok to grab from cache
        showMultipleReportsReport(
            togglerContainer, 
            index, 
            null, 
            identifier, 
            columnUuid, 
            JSON.stringify(cachedReports[index - 1])
        );
    } else {
        // request from server
        togglerContainer.find('.sres-report-toggler-spinner').removeClass('d-none');
        $.ajax({
            url: ENV['GET_DATA_ENDPOINT']
                .replace('__report_index__', index)
                .replace('__column_uuid__', columnUuid)
                .replace('__identifier__', identifier) + '&return_all_reports=yes',
            method: 'GET',
            success: function(data) {
                data = JSON.parse(data);
                data = data[identifier];
                showMultipleReportsReport(
                    togglerContainer, 
                    data.multiple_reports_meta.index, 
                    data.multiple_reports_meta.count, 
                    identifier, 
                    columnUuid, 
                    data.data
                );
                togglerContainer.find('.sres-report-toggler-spinner').addClass('d-none');
                // update cache
                togglerContainer.data('sres-cached-reports', data.all_reports_data);
            }
        });
    }
}
$(document).on('sres:datasaved', function(event, args) {
    let columnUuid = args['columnuuid'];
    let identifier = args['identifier'];
    let payload = args['payload'];
    let togglerContainer = $(".sres-report-toggler-container[data-sres-columnuuid='" + columnUuid + "'][data-sres-identifier='" + identifier + "']");
    if (togglerContainer.length) {
        togglerContainer.attr('data-sres-report-toggler-current-index', payload.multiple_reports_meta.index);
        togglerContainer.attr('data-sres-report-toggler-max-index', payload.multiple_reports_meta.count);
        togglerContainer.find('.sres-report-toggler-current-display').val(payload.multiple_reports_meta.index);
        togglerContainer.find('.sres-report-toggler-max-display').html(payload.multiple_reports_meta.count);
        // call to show report
        multipleReportsTogglerShowReportByIndex(togglerContainer, payload.multiple_reports_meta.index, true);
    }
});
$(document).on('click', '.sres-report-toggler-new', function(){
    let togglerContainer = $(this).parents(".sres-report-toggler-container");
    let identifier = togglerContainer.attr('data-sres-identifier');
    let columnUuid = togglerContainer.attr('data-sres-columnuuid');
    // just clear
    clearInputFields("div.sres-input-container[data-sres-identifier='" + identifier + "'][data-sres-columnuuid='" + columnUuid + "']");
    togglerContainer.find('.sres-report-toggler-current-display').val('');
    //and reset current-index
    togglerContainer.attr('data-sres-report-toggler-current-index', '');
});
$(document).on('change keydown', '.sres-report-toggler-current-display', function(event){
    let togglerContainer = $(this).parents(".sres-report-toggler-container");
    if (event.type == 'change' || (event.type == 'keydown' && event.keyCode == 13)) {
        multipleReportsTogglerShowReportByIndex(togglerContainer, $(this).val());
    }
});
$(document).on('click', '.sres-report-toggler-delete', function(){
    // TODO
});
$(document).on('click', '.sres-report-toggler-first, .sres-report-toggler-previous, .sres-report-toggler-next, .sres-report-toggler-last', function(){
    let togglerContainer = $(this).parents(".sres-report-toggler-container");
    let identifier = togglerContainer.attr('data-sres-identifier');
    let columnUuid = togglerContainer.attr('data-sres-columnuuid');
    let newIndex = null;
    let currentIndex = togglerContainer.attr('data-sres-report-toggler-current-index');
    if (currentIndex == '') {
        currentIndex = 0;
    }
    if ($(this).hasClass('sres-report-toggler-first')) {
        newIndex = 1;
    } else if ($(this).hasClass('sres-report-toggler-previous')) {
        newIndex = parseInt(currentIndex) - 1;
        if (newIndex <= 0) {
            newIndex = 1;
        }
    } else if ($(this).hasClass('sres-report-toggler-next')) {
        newIndex = parseInt(currentIndex) + 1;
    } else if ($(this).hasClass('sres-report-toggler-last')) {
        newIndex = togglerContainer.attr('data-sres-report-toggler-max-index');;
    }
    // request and show data
    multipleReportsTogglerShowReportByIndex(togglerContainer, newIndex);
});

/**
    File upload
**/
$(document).on('click', '.sres-addvalue-file-upload, .sres-addvalue-file-upload-additional', function(event){
    let inputContainer = $(this).parents('.sres-input-container');
    let input = inputContainer.find('input.sres-addvalue-file-input');
    let hiddenInputField = inputContainer.find('[data-sres-field=file]');
    let saveButton = inputContainer.find('.sres-addvalue-file-button-save');
    let thisButton = $(this);
    let fd = new FormData();
    let files = input[0].files;
    let columnUuid = input.attr('data-sres-columnuuid');
    let identifier = input.attr('data-sres-identifier');
    let fileCountLimit = input.attr('data-sres-file-count-limit');
    let fileAllowedExtensions = input.attr('data-sres-file-allowed-extensions');
    if (fileAllowedExtensions.length) {
        fileAllowedExtensions = fileAllowedExtensions.split(',');
    } else {
        fileAllowedExtensions = [];
    }
    let fileMaxBytes = parseInt(input.attr('data-sres-file-max-bytes'));
    let elProcessing = $(this).find('.sres-addvalue-file-upload-processing');
    let uploadMode = $(this).hasClass('sres-addvalue-file-upload-additional') ? 'additional' : 'replace';
    
    // check max bytes and extensions
    let oversizeFiles = [];
    let badExtensionFiles = [];
    for (let f = 0; f < files.length; f++) {
        if (files[f].size > fileMaxBytes) {
            oversizeFiles.push(files[f].name);
        }
        if (fileAllowedExtensions.length > 0 && fileAllowedExtensions.indexOf('.' + files[f].name.split('.').pop()) == -1) {
            badExtensionFiles.push(files[f].name);
        }
    }
    if (oversizeFiles.length > 0) {
        alert('The following files are over the ' + (fileMaxBytes / 1024 / 1024) + 'mb limit: ' + oversizeFiles.join(', ') + '. The file upload will not proceed.');
        return false;
    }
    if (fileAllowedExtensions.length > 0 && badExtensionFiles.length > 0) {
        alert('The allowed extensions for files are ' + fileAllowedExtensions.join(', ') + '. The following files do not have allowed extensions: ' + badExtensionFiles.join(', ') + '. The file upload will not proceed.');
        return false;
    }
    
    // continue
    
    elProcessing.removeClass('d-none');
    
    for (let f = 0; f < files.length; f++) {
        fd.append('files', files[f]);
    }
    fd.set('c', columnUuid);
    fd.set('i', identifier);
    fd.set('t', 'file');
    
    $.ajax({
        type: 'POST',
        url: ENV['SEND_RICH_DATA_ENDPOINT'],
        data: fd,
        contentType: false,
        processData: false
    }).done(function(data){
        elProcessing.addClass('d-none');
        data = JSON.parse(data);
        data.messages.forEach(function(message){
            $.notify({message:message[0]}, {type:message[1]});
        });
        if (data.success) {
            // cosmetics
            thisButton.find('.sres-addvalue-file-upload-success').removeClass('d-none');
            thisButton.addClass('btn-success').removeClass('btn-primary');
            window.setTimeout(function(){
                thisButton.find('.sres-addvalue-file-upload-success').addClass('d-none');
                thisButton.removeClass('btn-success').addClass('btn-primary');
            }, 3000);
            // determine the new data value and store the value in the hidden input field
            switch (uploadMode) {
                case 'replace':
                    hiddenInputField.val(JSON.stringify(data.data));
                    break;
                case 'additional':
                    let currentFiles = JSON.parse(hiddenInputField.val());
                    currentFiles = currentFiles.concat(data.data);
                    currentFiles = currentFiles.slice(-fileCountLimit);
                    hiddenInputField.val(JSON.stringify(currentFiles));
                    break;
            }
            // show the files as links
            $(document).trigger('sres:updateaddvaluefilelist', {
                identifier: identifier,
                columnUuid: columnUuid
            });
            // enable the save button
            saveButton.removeClass('d-none'); //.addClass('animated bounce infinite');
            inputContainer.find('.sres-addvalue-file-unsaved-notification').removeClass('d-none');
            /*window.setTimeout(function(){
                saveButton.removeClass('animated bounce infinite');
            }, 3000);*/
        } else {
            $.notify(
                {
                    message: "Something went wrong with the file upload. Please try again later."
                },
                {
                    type: "danger"
                }
            );
        }
    });
});
$(document).on('change', '.sres-addvalue-file-input', function(event){
    //console.log($(this).val());
    let saveButton = $(this).parents('.sres-input-container').find('.sres-addvalue-file-button-save');
    if ($(this).val() == '') {
        saveButton.removeClass('d-none').addClass('animated bounce infinite');
        window.setTimeout(function(){
            saveButton.removeClass('animated bounce infinite');
        }, 3000);
    } else {
        saveButton.addClass('d-none').removeClass('animated bounce infinite');
    }
});
$(document).on('sres:updateaddvaluefilelist', function(event, args) {
    let identifier = args.identifier;
    let columnUuid = args.columnUuid;
    let inputContainer = $(".sres-input-container[data-sres-column-type=file][data-sres-columnuuid='" + columnUuid + "'][data-sres-identifier='" + identifier + "']");
    let input = inputContainer.find("[data-sres-field='file']");
    let data = input.val();
    //console.log('data', data, JSON.parse(data));
    data = JSON.parse(data);
    let files = [];
    data.forEach(function(record){
        if (typeof record == 'string') {
            files.push({
                display: record,
                url: record
            });
        } else if (typeof record == 'object') {
            if (record.original_filename && record.url) {
                files.push({
                    display: record.original_filename,
                    url: record.url
                });
            } else if (record.saved_filename && record.url) {
                files.push({
                    display: record.saved_filename,
                    url: record.url
                });
            } else if (record.url) {
                files.push({
                    display: record.url,
                    url: record.url
                });
            }
        } else {
            // unknown!!
        }
    });
    let list = inputContainer.find('.sres-addvalue-file-existing-list');
    list.html('');
    files.forEach(function(file){
        //console.log('file', file);
        let li = document.createElement('li');
        let a = document.createElement('a');
        a.href = file.url;
        a.innerHTML = file.display;
        li.innerHTML = a.outerHTML;
        list.append(li.outerHTML);
    });
    let listContainer = list.parents('.sres-addvalue-file-existing-list-container');
    listContainer.removeClass('d-none');
});

/**
    html-simple editor
**/
let addvalueHtmlSimpleConfig = {
    selector: '.sres-addvalue-tinymce-simple',
    toolbar: ['bold, italic, underline | subscript, superscript | bullist, numlist | undo, redo'],
    menubar: false,
    inline: true,
    statusbar: true,
    plugins: 'lists',
    relative_urls: false,
    remove_script_host: false,
    convert_urls: false,
    browser_spellcheck: true
}
let addvalueHtmlReadonlySimpleConfig = {
    selector: '.sres-addvalue-tinymce-simple-readonly',
    readonly: 1,
    toolbar: [],
    menubar: false,
    inline: true,
    statusbar: false,
    relative_urls: false,
    remove_script_host: false,
    convert_urls: false
}
$(document).ready(function(){
    tinymce.init(addvalueHtmlSimpleConfig);
    tinymce.init(addvalueHtmlReadonlySimpleConfig);
});

