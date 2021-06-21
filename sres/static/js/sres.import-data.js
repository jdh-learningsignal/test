$(document).on('change', '#input_file_type', function(){
    $('.sres-import-options').collapse('hide');
    if ($(this).val()) {
        $('.one-sres-import-option').css('display','none');
        $('#' + $(this).val() + '_options').css('display','block');
        $('.sres-import-options').css('display','flex');
    } else {
        $('.sres-import-options').css('display','none');
    }
});
// Picking import action
$(document).on('change', 'input.sres-import-action-choose', function(){
    var action = $(this).val();
    var $row = $(this).parents('td');
    $row.find('.sres-import-destination-config').addClass('d-none');
    switch (action) {
        case 'new':
            $row.find('.sres-import-destination-name').removeClass('d-none').focus();
            break;
        case 'existing':
            $row.find('.sres-import-destination-select').removeClass('d-none');
            break;
    }
});
// Showing multiEntry warning
$(document).on('change', '.sres-import-destination-select select', function(){
    if ($(this).find('option:selected').attr('data-sres-datatype') == 'multiEntry') {
        $(this).siblings(".sres-datatype-alert-multientry").removeClass('d-none');
    } else {
        $(this).siblings(".sres-datatype-alert-multientry").addClass('d-none');
    }
});
// Preview input data
$(document).on('click', '.sres-import-data-preview', function(){
    $(this).siblings('.sres-column-data-preview').each(function(){
        $(this).collapse('toggle');
    });
});
// Bulk set all import options
$(document).on('click', '.sres-import-action-choose-all-noimport', function(){
    $('input.sres-import-action-choose[value=noimport]').trigger('click').trigger('change');
});
$(document).on('click', '.sres-import-action-choose-all-new', function(){
    $('input.sres-import-action-choose[value=new]').trigger('click').trigger('change');
});

/**
    File import preprocessing
**/
$(document).on('submit', '#import_file_preprocess', function(event){
    let input_file_type = $('#input_file_type').val();
    if (input_file_type == 'generic_spreadsheet') {
        event.preventDefault();
        let formData = new FormData();
        formData.append('file', document.getElementById('generic_spreadsheet_file').files[0]);
        let rowSource = document.getElementById("import-row-mapper-template").innerHTML;
        let rowTemplate = Handlebars.compile(rowSource);
        $("#import_file_preprocess_button span.sres-spinner").removeClass('d-none');
        $.ajax({
            url: ENV['FILE_IMPORT_ENDPOINT'].replace('__source__', $('#input_file_type').val()).replace('__stage__', 'preprocess'),
            contentType: false,
            processData: false,
            data: formData,
            method: 'POST',
            success: function(data) {
                try {
                    data = JSON.parse(data);
                } catch(error) {
                    $.notify(
                        {
                            message:'We encountered an unexpected error in reading the response from the server. Double-check the file format, and try to process the file again.',
                            icon: 'fa fa-exclamation-triangle'
                        }, 
                        {
                            type: 'danger',
                            animate: { enter: 'animated shake' },
                            delay: 5000
                        }
                    );
                    $("#import_file_preprocess_button span.sres-spinner").addClass('d-none');
                    return false;
                }
                //console.log(data);
                $('.sres-import-mapper').collapse('show');
                $('.sres-import-activator').collapse('show');
                ENV['import_file_header_count'] = data.headers.length;
                ENV['import_file_row_count'] = data.row_count;
                ENV['import_file_system_filename'] = data.system_filename;
                // Show each import mapper row
                $('#column_sid').html('');
                $('#import_mapper_mapping_container').html('');
                dataPreview = JSON.parse(data.data_head);
                $('#column_sid').append('<option value="">Please select a column that contains a unique identifier</option>');
                data.headers.forEach(function(header, i){
                    // identifier chooser
                    $('#column_sid').append('<option value="' + i + '">' + Handlebars.Utils.escapeExpression(header) + '</option>');
                    // import mapper
                    let context = {
                        c: i,
                        column_header: header,
                        column_preview: dataPreview[header],
                        destination_columns: data.destination_columns,
                        system_columns: data.system_columns,
                        disableExisting: data.destination_columns.length > 0 ? false : true
                    }
                    $('#import_mapper_mapping_container').append(rowTemplate(context));
                });
                refreshTooltips();
                // Restore remembered_mappings
                Object.keys(data.remembered_mappings).forEach(function(i) {
                    if ($('#column_destination_' + i + ' option[value=' + data.remembered_mappings[i].target_column_uuid + ']')) {
                        $('input[name=column_import_action_' + i + '][value=existing]').parents('label.btn').trigger('click');
                        $('#column_destination_' + i).val(data.remembered_mappings[i].target_column_uuid).trigger('change');
                    }
                });
                // UI
                $('.sres-import-options-container').collapse('hide');
                $('.sres-import-mapper')[0].scrollIntoView({behavior:'smooth'});
                $("#import_file_preprocess_button span.sres-spinner").addClass('d-none');
            }
        })
    }
});

$(document).on('input', '#column_sid', function(){
    if ($(this).val()) {
        $('#column_sid_container').removeClass('alert-danger').addClass('alert-info');
        $('#column_sid_unmapped_warning').addClass('d-none');
        $('#column_sid_mapped').removeClass('d-none');
    } else {
        $('#column_sid_container').addClass('alert-danger').removeClass('alert-info');
        $('#column_sid_unmapped_warning').removeClass('d-none');
        $('#column_sid_mapped').addClass('d-none');
    }
});

/**
    Import
**/
function getNewColumns() {
    var newColumns = {};
    $('input.sres-import-action-choose').each(function() {
        if ($(this).prop('checked') && $(this).val() == 'new') {
            var i = parseInt($(this).parents('tr[data-sres-header-index]').attr('data-sres-header-index'));
            var newName = $('#column_new_column_name_' + i.toString()).val();
            //console.log(i, newName, newName.length);
            if (newName.length) {
                // use user-specified name
                newColumns[i] = newName;
            } else {
                // grab name from header
                newColumns[i] = $('#column_sid option[value=' + i + ']').text();
            }
        }
    });
    return newColumns;
}
function requestDataImport(identifierHeaderIndex, systemFilename, rowCount, rowsEachStep) {
    //console.log('requestDataImport', identifierHeaderIndex, systemFilename, rowCount, rowsEachStep);
    $('.sres-import-status').collapse('show');
    $('.sres-import-status .progress-bar')
        .css('width', '0%')
        .attr('data-sres-valuemax', rowCount)
        .attr('data-sres-valuenow', 0);
    for (var r = 0; r < rowCount; r += rowsEachStep) {
        $.ajaxq('importer', {
            url: ENV['FILE_IMPORT_ENDPOINT'].replace('__source__', $('#input_file_type').val()).replace('__stage__', 'import'),
            data: {
                identifier_header_index: identifierHeaderIndex,
                row_start: r,
                filename: systemFilename,
                rows_to_process: rowsEachStep,
                mapper: $('#import_file_mapper').serialize()
            },
            method: 'POST',
            success: function(data) {
                data = JSON.parse(data);
                //console.log(data);
                let recordsSaved = data.records_saved;
                let recordsError = data.records_error;
                recordsReported = 0;
                let $successBar = $('.sres-import-status .progress-bar.bg-success');
                let $dangerBar = $('.sres-import-status .progress-bar.bg-danger');
                let valueMax = parseFloat($successBar.attr('data-sres-valuemax'));
                let valueNowSuccess = parseFloat($successBar.attr('data-sres-valuenow'));
                let valueNowDanger = parseFloat($dangerBar.attr('data-sres-valuenow'));
                Object.keys(data.student_data_save).forEach(function(identifier) {
                    //console.log(identifier, data.student_data_save[identifier]);
                    recordsReported++;
                    if (data.student_data_save[identifier].success) {
                        //valueNowSuccess++;
                    } else {
                        //valueNowDanger++;
                        data.student_data_save[identifier].messages.forEach(function(message, m){
                            $('.sres-import-status-container').append('<div class="alert alert-' + message[1] + '">' + message[0] + '</div>');
                        });
                    }
                });
                valueNowSuccess += recordsSaved;
                $successBar
                    .css('width', ((valueNowSuccess / valueMax) * 100) + '%')
                    .attr('data-sres-valuenow', valueNowSuccess);
                valueNowDanger += recordsError;
                $dangerBar
                    .css('width', ((valueNowDanger / valueMax) * 100) + '%')
                    .attr('data-sres-valuenow', valueNowDanger);
                if (valueNowSuccess + valueNowDanger >= valueMax) {
                    $('.sres-import-status-container .progress-spinner').collapse('hide');
                    $('.sres-import-final').collapse('show');
                }
                if (recordsReported != recordsSaved) {
                    $('.sres-import-status-container').append('<div class="alert alert-warning">Mismatch between total (' + recordsSaved.toString() + ') vs unique (' + recordsReported.toString() + ') number of records saved; this is often caused by duplicate identifiers.</div>');
                }
            }
        });
    }
}
$(document).on('click', '#import_activate', function(event){
    // check to see if a unique identifier has been selected properly
    var identifierHeaderIndex = $('#column_sid').val();
    if (identifierHeaderIndex == '') {
        let identifierSelector = $('#column_sid').parent();
        identifierSelector[0].scrollIntoView({behavior: 'smooth'});
        identifierSelector.addClass('animated flash delay-2s');
        setTimeout(function(){
            identifierSelector.removeClass('animated flash delay-2s');
        }, 4000);
        $.notify(
            {
                message: 'Please specify the column that contains a unique identifier.',
                icon: 'fa fa-exclamation-triangle'
            }, 
            {
                type: 'danger',
                animate: { enter: 'animated shake' },
                delay: 5000
            }
        );
        event.preventDefault();
        return;
    }
    // continue to import
    $('.sres-import-status').collapse('show');
    $('.sres-import-status-container .progress-spinner').collapse('show');
    var systemFilename = ENV['import_file_system_filename'];
    var rowCount = ENV['import_file_row_count'];
    var rowsEachStep = rowCount > 200 ? 200 : 50;
    var newColumns = getNewColumns();
    $('.sres-import-mapper-container').collapse('hide');
    $('.sres-import-activator-container').collapse('hide');
    if (Object.getOwnPropertyNames(newColumns).length) {
        // Need to create new columns first
        //console.log('newColumns', newColumns);
        $.ajax({
            url: ENV['FILE_IMPORT_ENDPOINT'].replace('__source__', $('#input_file_type').val()).replace('__stage__', 'create_new_columns'),
            data: {
                new_columns_request: JSON.stringify(newColumns)
            },
            method: 'POST',
            success: function(data){
                data = JSON.parse(data);
                //console.log(data);
                data.forEach(function(column){
                    $('#column_destination_' + column.index)
                        .append('<option value="' + column.uuid + '">' + column.name + '</option>')
                        .find('option[value=' + column.uuid + ']').prop('selected', true);
                    //$('input[name=column_import_action_' + column.index + '][value=existing]').parents('label.btn').trigger('click');
                    $('input[name=column_import_action_' + column.index + '][value=existing]').prop('checked', true);
                    $('.sres-import-status-container').append('<div class="alert alert-info">New column created: ' + column.name + '</div>');
                });
                requestDataImport(identifierHeaderIndex, systemFilename, rowCount, rowsEachStep);
            }
        });
    } else {
        // Only importing into existing
        requestDataImport(identifierHeaderIndex, systemFilename, rowCount, rowsEachStep)
    }
});
