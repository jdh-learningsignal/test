$(document).on('click', '#new_summary', function(){
    
    let template = Handlebars.compile(document.getElementById("summary_card_template").innerHTML)
    let tempId = 'summary_' + Date.now();
    $('#summaries_container').append(template({
        id: tempId
    }));
    let newCard = $('#' + tempId);
    //newCard.attr('data-sres-summary-id', tempId);
    $('#summary_id').val(tempId);
    $('#summary_name').val('');
    $('#summary_description').val('');
    $('#summary_canonical_reference').val('');
    $('#summary_calculation_mode').trigger('change');
    $('#summary_presentation_mode').trigger('change');
    $('#summary_grouping_mode').val('disabled').trigger('change');
    
    updateSummaryRepresentation(tempId);
    
    //$('#summary_config_form .sres-condition-column-remove').trigger('click');
    $('#summary_column_reference').val('').trigger('chosen:updated');
    $('#summary_grouping_column_reference').val('').trigger('chosen:updated');
    
    newCard.find('.sres-summary-card-action-edit').addClass('animated slower flash infinite');
    newCard.find('.card').addClass('border-primary');
    sresToggleSummaryConfigPane(true);
    window.setTimeout(function(){
        newCard.get(0).scrollIntoView({behavior:'smooth'});
    }, 500);
    
});

/**
    Load existing summaries
**/
function appendSummaryFromConfig(summaryConfig) {
    let template = Handlebars.compile(document.getElementById("summary_card_template").innerHTML)
    calcModeExtraConfig = JSON.stringify(summaryConfig.representation_config.calculation.extra_config)
    presModeExtraConfig = JSON.stringify(summaryConfig.representation_config.presentation.extra_config)
    let config = {
        canonicalReference: summaryConfig.canonical_reference,
        id: summaryConfig.uuid,
        columnReferenceEncoded: encodeURIComponent(summaryConfig.column_reference),
        calcMode: summaryConfig.representation_config.calculation.mode,
        'calcModeExtraConfig': calcModeExtraConfig ? calcModeExtraConfig : '{}',
        presMode: summaryConfig.representation_config.presentation.mode,
        'presModeExtraConfig': presModeExtraConfig ? presModeExtraConfig : '{}',
        groupingMode: summaryConfig.representation_config.grouping ? summaryConfig.representation_config.grouping.mode : 'disabled',
        groupingComparisonMode: summaryConfig.representation_config.grouping ? summaryConfig.representation_config.grouping.comparison_mode : 'disabled',
        groupingColumnReference: summaryConfig.representation_config.grouping ? summaryConfig.representation_config.grouping.column_reference : '',
        name: summaryConfig.name,
        description: summaryConfig.description
    }
    $('#summaries_container').append(template(config));
    updateSummaryRepresentation(summaryConfig.uuid);
}
function loadExistingSummaries() {
    $('#summaries_container').html('<div class="spinner-border" role="status"><span class="sr-only">Loading...</span></div>');
    $.ajax({
        url: ENV['LIST_SUMMARIES_ENDPOINT'],
        method: 'GET',
        success: function(data){
            data = JSON.parse(data);
            if (data.summaries) {
                data.summaries.forEach(function(summary){
                    appendSummaryFromConfig(summary);
                });
            } else {
                $('#summaries_container').html('No summaries found. Click the new summary button to add a new summary.');
            }
        }
    });
}
$(document).ready(function(){
    // wait until chart library is loaded
    google.charts.setOnLoadCallback(loadExistingSummaries);
});

/**
    Configuration pane
**/
// Pane closing
$(document).on('click', '.sres-pane-close', function(){
    sresToggleSummaryConfigPane(false);
});
function sresToggleSummaryConfigPane(visible) {
    $('.sres-summary-card-action-edit').removeClass('animated slower flash infinite');
    $('.sres-summary-card .card').removeClass('border-primary');
    if (visible) {
        $('.sres-main').addClass('pushed');
        $('.sres-pane').addClass('open');
    } else {
        $('.sres-main').removeClass('pushed');
        $('.sres-pane').removeClass('open');
    }
    window.setTimeout(function(){
        $(window).trigger('resize');
    }, 750);
    $('#summary_column_reference').chosen({
        width: '100%',
        search_contains: true,
        placeholder_text_multiple: 'Select'
    });
    $('#summary_grouping_column_reference').chosen({
        width: '100%',
        search_contains: true,
        placeholder_text_single: 'Select'
    });
    $('#summary_save').removeClass('d-none');
    if ($('#summary_id').val().startsWith('summary_')) {
        // hide the update button if making new summary
        $('#summary_save').addClass('d-none');
    }
}
// Column selector
$(document).on('click', '.sres-condition-column-placeholder', function(){
    let receiver = $(this).siblings('input:hidden.sres-condition-column-receiver');
    show_column_chooser(receiver.attr('id'), '', null, null, null, null, receiver.val(), true);
});
// Change summary config
$(document).on('input', '#summary_column_reference, #summary_calculation_mode, #summary_presentation_mode, #summary_grouping_mode, #summary_grouping_column_reference, #summary_grouping_comparison_mode', function(){
    let summaryId = $('#summary_config_form input[name=summary_id]').val();
    let summaryCard = $(".sres-summary-card[id='" + summaryId + "']");
    summaryCard.attr('data-sres-column-reference-encoded', encodeURIComponent($('#summary_column_reference').val()));
    summaryCard.attr('data-sres-calculation-mode', $('#summary_calculation_mode').val());
    summaryCard.attr('data-sres-presentation-mode', $('#summary_presentation_mode').val());
    summaryCard.attr('data-sres-grouping-mode', $('#summary_grouping_mode').val());
    summaryCard.attr('data-sres-grouping-comparison-mode', $('#summary_grouping_comparison_mode').val());
    summaryCard.attr('data-sres-grouping-column-reference', $('#summary_grouping_column_reference').val());
    if ($(this).attr('id') == 'summary_grouping_column_reference') {
        let groupingSelect = summaryCard.find('.sres-summary-grouping-values');
        groupingSelect.html('').trigger('chosen:updated');
    }
    updateSummaryRepresentation(summaryId);
});
$(document).on('change', '#summary_column_reference', function(){
    let disableConfig = false;
    if ($(this).val().length == 0) {
        disableConfig = true;
    }
    $('#summary_calculation_mode').prop('disabled', disableConfig);
    $('.sres-calculation-extra-config-field').prop('disabled', disableConfig);
    $('#summary_presentation_mode').prop('disabled', disableConfig);
    $('#summary_name').prop('disabled', disableConfig).trigger('input');
    $('#summary_description').prop('disabled', disableConfig);
    //$('#summary_save_new').prop('disabled', disableConfig);
    //$('#summary_save').prop('disabled', disableConfig);
    //$('#summary_delete').prop('disabled', disableConfig);
});
// Extra config for summary calculation and presentation modes
$(document).on('change', '#summary_calculation_mode, #summary_presentation_mode', function(){
    // hide all
    $('[data-sres-calculation-extra-config-parent]').addClass('d-none');
    $('[data-sres-presentation-extra-config-parent]').addClass('d-none');
    // show relevant
    let currentModeId = $(this).val();
    let configType = $(this).attr('id') == 'summary_calculation_mode' ? 'calculation' : 'presentation';
    $("[data-sres-" + configType + "-extra-config-parent='" + currentModeId + "']").removeClass('d-none');
});
$(document).on('input', '.sres-calculation-extra-config-field, .sres-presentation-extra-config-field', function(){
    let allConfig = {};
    let configType = $(this).hasClass('sres-calculation-extra-config-field') ? 'calculation' : 'presentation';
    $('.sres-' + configType + '-extra-config-field').each(function(){
        let configId = $(this).attr('data-sres-' + configType + '-extra-config-id');
        allConfig[configId] = $(this).val();
    });
    let summaryId = $('#summary_config_form input[name=summary_id]').val();
    let summaryCard = $(".sres-summary-card[id='" + summaryId + "']");
    summaryCard.attr('data-sres-' + configType + '-mode-extra-config', JSON.stringify(allConfig));
    updateSummaryRepresentation(summaryId);
});
// Grouping mode change
$(document).on('change', '#summary_grouping_mode', function(){
    let summaryId = $('#summary_config_form input[name=summary_id]').val();
    let summaryCard = $(".sres-summary-card[id='" + summaryId + "']");
    if ($(this).val() == 'enabled') {
        $('#summary_grouping_column_reference').parents('.form-group').removeClass('d-none');
        $('#summary_grouping_comparison_mode').parents('.form-group').removeClass('d-none');
        summaryCard.find('.sres-summary-grouping-container').removeClass('d-none');
    } else {
        $('#summary_grouping_column_reference').parents('.form-group').addClass('d-none');
        $('#summary_grouping_comparison_mode').parents('.form-group').addClass('d-none');
        summaryCard.find('.sres-summary-grouping-container').addClass('d-none');
    }
});
// Change summary name and description
$(document).on('input', '#summary_name, #summary_description', function(){
    let summaryId = $('#summary_config_form input[name=summary_id]').val();
    let summaryCard = $(".sres-summary-card[id='" + summaryId + "']");
    let summaryName = $('#summary_name').val();
    let summaryDesc = $('#summary_description').val();
    summaryCard.attr('data-sres-summary-name', summaryName);
    summaryCard.attr('data-sres-summary-description', summaryDesc);
    summaryCard.find('.sres-summary-card-title').text(summaryName);
    summaryCard.find('.sres-summary-card-subtitle').text(summaryDesc);
    let disableSaveButtons = true;
    if (summaryName.length > 0) {
        disableSaveButtons = false;
    }
    $('#summary_save_new').prop('disabled', disableSaveButtons);
    $('#summary_save').prop('disabled', disableSaveButtons);
});
// Canonical reference
$(document).on('click', '#summary_canonical_reference', function(){
    $(this).get(0).select();
});
// Summary save/delete
$(document).on('click', '#summary_save_new, #summary_save, #summary_delete', function(){
    let summaryId = $('#summary_config_form input[name=summary_id]').val();
    let summaryCard = $(".sres-summary-card[id='" + summaryId + "']");
    let method = undefined;
    switch ($(this).attr('id')) {
        case 'summary_save_new':
            method = 'POST';
            break;
        case 'summary_save':
            method = 'PUT';
            break;
        case 'summary_delete':
            method = 'DELETE';
            // see if it's a temp one
            if (summaryId.startsWith('summary_')) {
                // yes, not saved yet, just remove from DOM
                summaryCard.remove();
                sresToggleSummaryConfigPane(false);
                return;
            }
            break;
    }
    $.ajax({
        url: ENV['SUMMARIES_CRUD_ENDPOINT'].replace('__summary_uuid__', summaryId),
        method: method,
        data: {
            col: summaryCard.attr('data-sres-column-reference-encoded'),
            calc_mode: summaryCard.attr('data-sres-calculation-mode'),
            calc_mode_extra_config: summaryCard.attr('data-sres-calculation-mode-extra-config'),
            pres_mode: summaryCard.attr('data-sres-presentation-mode'),
            pres_mode_extra_config: summaryCard.attr('data-sres-presentation-mode-extra-config'),
            group_mode: summaryCard.attr('data-sres-grouping-mode'),
            group_comparison_mode: summaryCard.attr('data-sres-grouping-comparison-mode'),
            group_column_reference: summaryCard.attr('data-sres-grouping-column-reference'),
            name: summaryCard.attr('data-sres-summary-name'),
            description: summaryCard.attr('data-sres-summary-description'),
        },
        success: function(data){
            data = JSON.parse(data);
            if (data.success) {
                $.notify(
                    { message: 'Successfully updated this summary' },
                    { type: 'success' }
                );
                summaryCard.attr('data-sres-canonical-reference', data.canonical_reference);
                switch (method) {
                    case 'DELETE':
                        summaryCard.remove();
                        sresToggleSummaryConfigPane(false);
                        break;
                    case 'POST':
                        //data.config.canonical_reference = data.canonical_reference;
                        //appendSummaryFromConfig(data.config);
                        loadExistingSummaries();
                        sresToggleSummaryConfigPane(false);
                        break;
                }
            } else {
                $.notify(
                    { message: 'Unexpected error updating this summary' },
                    { type: 'danger' }
                );
            }
        },
        error: function(err){
            console.error(err);
            $.notify(
                { message: 'Unexpected error updating this summary' },
                { type: 'danger' }
            );
            
        }
    });
});

/**
    For each card
**/
// Edit or delete the summary
$(document).on('click', '.sres-summary-card-action-edit, .sres-summary-card-action-delete', function(){
    let summaryId = $(this).parents('.sres-summary-card').attr('id');
    let summaryCard = $('.sres-summary-card#' + summaryId);
    $('#summary_id').val(summaryId);
    $('#summary_name').val(summaryCard.attr('data-sres-summary-name'));
    $('#summary_description').val(summaryCard.attr('data-sres-summary-description'));
    $('#summary_calculation_mode').val(summaryCard.attr('data-sres-calculation-mode')).trigger('change');
    // calc mode extra config
    let calcModeExtraConfig = summaryCard.attr('data-sres-calculation-mode-extra-config');
    try {
        calcModeExtraConfig = JSON.parse(calcModeExtraConfig);
    } catch(e) {
        calcModeExtraConfig = {};
    }
    Object.keys(calcModeExtraConfig).forEach(function(configId){
        $('#summary_calculation_mode_extra_config_' + configId).val(calcModeExtraConfig[configId]);
    });
    // pres mode extra config
    let presModeExtraConfig = summaryCard.attr('data-sres-presentation-mode-extra-config');
    try {
        presModeExtraConfig = JSON.parse(presModeExtraConfig);
    } catch(e) {
        presModeExtraConfig = {};
    }
    Object.keys(presModeExtraConfig).forEach(function(configId){
        $('#summary_presentation_mode_extra_config_' + configId).val(presModeExtraConfig[configId]);
    });
    // keep going
    $('#summary_presentation_mode').val(summaryCard.attr('data-sres-presentation-mode')).trigger('change');
    $('#summary_column_reference').val(decodeURIComponent(summaryCard.attr('data-sres-column-reference-encoded')).split(',')).trigger('chosen:updated');
    $('#summary_canonical_reference').val(summaryCard.attr('data-sres-canonical-reference'));
    $('#summary_grouping_mode').val(summaryCard.attr('data-sres-grouping-mode')).trigger('change');
    $('#summary_grouping_comparison_mode').val(summaryCard.attr('data-sres-grouping-comparison-mode'));
    $('#summary_grouping_column_reference').val(summaryCard.attr('data-sres-grouping-column-reference')).trigger('chosen:updated');
    if ($(this).hasClass('sres-summary-card-action-edit')) {
        // open the pane
        $('#summary_column_reference').trigger('change');
        sresToggleSummaryConfigPane(true);
        // indicate this summary is being edited
        summaryCard.find('.sres-summary-card-action-edit').addClass('animated slower flash infinite');
        summaryCard.find('.card').addClass('border-primary');
    } else {
        // just delete
        $('#summary_delete').trigger('click');
    }
});

// Grouping value change
$(document).on('input', '.sres-summary-grouping-values', function(){
    let summaryId = $(this).parents('.sres-summary-card').attr('id');
    let summaryCard = $('.sres-summary-card#' + summaryId);
    let groupingValues = $(this).val();
    updateSummaryRepresentation(summaryId, false);
});



