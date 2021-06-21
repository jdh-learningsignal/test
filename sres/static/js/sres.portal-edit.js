$(document).ready(function(){
    [ 'administrator', 'teacher', 'viewer' ].forEach(function(u){
        $('#authorised_' + u + 's').chosen({
            width: '100%', 
            no_results_text: 'Please use the button to add users', 
            placeholder_text_multiple: 'Please use the button to add users',
            disable_search: true
        });
    });
});

var tinymceBasicToolbar = ['bold italic underline | strikethrough subscript superscript | removeformat | forecolor backcolor | bullist numlist | indent outdent | alignleft aligncenter alignright alignjustify', 'link unlink | image table hr charmap | cut copy paste pastetext | undo redo', 'styleselect fontselect fontsizeselect | code searchreplace'];
function initialiseEditor(selector) {
    var elementSelector = '';
    if (typeof id == 'undefined') {
        // Initialise all
        elementSelector = '.tinymce-basic';
    } else {
        elementSelector = selector;
    }
    let initConfig = {
        selector: elementSelector,
        toolbar: tinymceBasicToolbar,
        menubar: false,
        inline: true,
        plugins: 'code textcolor colorpicker lists link image table hr charmap paste searchreplace',
        table_default_attributes: {
            'class': 'table'
        },
        images_upload_url: ENV['FILE_UPLOAD_ENDPOINT'],
        images_upload_base_path: '',
        convert_urls: false,
        relative_urls: false,
        remove_script_host: false,
        images_upload_credentials: true,
        extended_valid_elements: 'i[class]'
    }
    if (ENV['FONT_FORMATS']) {
        initConfig['font_formats'] = ENV['FONT_FORMATS'];
    }
    tinymce.init(initConfig);
}

/** 
    Deployment
**/
$(document).ready(function(){
    $.post(
        ENV['SHORTEN_URL_ENDPOINT'],
        {long_url: $('#deployment-link-long').val()},
        function(data) {
            data = JSON.parse(data);
            $('#deployment-link-short').val(data.short_url)
        }
    );
});

/**
	Available dates
**/
$(document).on('change', '#available_from, #available_to', function() {
	//var fromDate = new Date($('#available_from').val());
	//var toDate = new Date($('#available_to').val());
	//var now = new Date();
	if ( moment($('#available_from').val()).isAfter(moment(), 'day') || ( moment($('#available_to').val()).isBefore(moment(), 'day') && !moment($('#available_to').val()).isSame(moment(), 'day') ) ) {
    //if (now < fromDate || now >= toDate) {
		$('#available_dates_unavailable').removeClass('d-none');
	} else {
		$('#available_dates_unavailable').addClass('d-none');
	}
});
$(document).ready(function(){
	$('#available_from').trigger('change');
});

/**
    Panels
**/
var panels = [];
function updateStringified() {
    $("input:hidden[name=panels]").val(JSON.stringify(panels));
    $(".sres-querybuilder-container").each(function(){
        if ($(this).parents('#panel_template').length == 1) {
            // ignore
        } else {
            try {
                //console.log('updating rules?!?!', $(this).queryBuilder('getRules'));
                let $hiddenField = $(this).siblings('input:hidden[name=rules_for_' + $(this).attr('id') + ']');
                $hiddenField.val(JSON.stringify($(this).queryBuilder('getRules')));
            } catch(e) {
                console.error(e);
            }
        }
    });
}
$(document).on('submit', 'form#sv_edit_form', function(){
    updateStringified();
});
// Adding a panel in the middle somewhere
function addPanelBeforePosition(insertBeforePanelNumber, args) {
    $('button[id=add_panel]').trigger('click', args);
    //console.log('insertBeforePanelNumber', insertBeforePanelNumber);
    if (insertBeforePanelNumber == -1) {
        // no need to shift; add at the end
    } else {
        let lastPanelNumber = panels[panels.length - 1];
        let position = panels.indexOf(insertBeforePanelNumber);
        for (let n = 0; n < panels.length - position - 1; n++) {
            $('#panel_raise-panel-x' + lastPanelNumber).trigger('click');
        }
    }
}
$(document).on('click', 'button.sres-interpanel-hover-add', function(event, args){
    let insertBeforePanelNumber = parseInt($(this).attr('data-sres-position'));
    addPanelBeforePosition(insertBeforePanelNumber, args);
});
// Adding a panel at the bottom
$(document).on('click', "button[id=add_panel]", function(event, args) {
    var panelNumber = 0;
    if (panels.length > 0) {
        panelNumber = Math.max.apply(Math, panels) + 1;
    } else {
        panelNumber = 1;
    }
    //console.log('new panel number ', panelNumber, args);
    $clone = $("#panel_template").clone(true, true);
    panels.push(panelNumber);
    //updateStringified();
    $clone
        .attr("id", "panel-panel-x" + panelNumber)
        .find("[id*=-panel-x]").each(function() {
            var idAndName = $(this).attr("id").replace(/[-]x[0-9]*/, "-x" + panelNumber);
            $(this).attr("id", idAndName);
            if ($(this).attr("name")) {
                $(this).attr("name", idAndName);
            }
        });
    $clone
        .removeClass("d-none")
        .show()
        .appendTo("#panel_container");
    // Set panel content if exists
    if (args) {
        $('div#panel_content-panel-x' + panelNumber).html(args['content']);
    }
    // Initialise tinymce
    $("#panel-panel-x" + panelNumber)
        .find("div[class~=tinymce-basic]").each(function() {
            //console.log($(this));
            initialiseEditor('div[id=panel_content-panel-x' + panelNumber + ']');
        });
    // Update the dropdowns and associated configs
    if (args) {
        //console.log(panelNumber, args['content']);
        // Show when
        $('[id=panel_showwhen-panel-x' + panelNumber + ']')
            .val(args['show_when'])
            .trigger('change');
        // Mode
        $('[id=panel_mode-panel-x' + panelNumber + ']')
            .val(args['mode'])
            .trigger('change');
        // Availability
        $('[id=panel_availability-panel-x' + panelNumber + ']')
            .val(args['availability'] ? args['availability'] : 'available')
            .trigger('change');
        $('[id=panel_availability_from_date-panel-x' + panelNumber + ']').val(
            args['availability_from_str'] ? moment(args['availability_from_str']).format('YYYY-MM-DD') : moment().format('YYYY-MM-DD')
        );
        $('[id=panel_availability_from_time-panel-x' + panelNumber + ']').val(
            args['availability_from_str'] ? moment(args['availability_from_str']).format('HH:mm:ss') : '00:00:00'
        );
        $('[id=panel_availability_to_date-panel-x' + panelNumber + ']').val(
            args['availability_to_str'] ? moment(args['availability_to_str']).format('YYYY-MM-DD') : moment().format('YYYY-MM-DD')
        );
        $('[id=panel_availability_to_time-panel-x' + panelNumber + ']').val(
            args['availability_to_str'] ? moment(args['availability_to_str']).format('HH:mm:ss') : '23:59:59'
        );
        // Collapsible
        $('[id=panel_collapsible-panel-x' + panelNumber + ']')
            .val(args['collapsible'] ? args['collapsible'] : 'disabled')
            .trigger('change');
        $('[id=panel_collapsible_default_display-panel-x' + panelNumber + ']')
            .val(args['collapsible_default_display'] ? args['collapsible_default_display'] : 'show')
            .trigger('change');
        // Reloadable
        $('[id=panel_trigger_reload_on_save-panel-x' + panelNumber + ']')
            .val(args['trigger_reload_on_save'] ? args['trigger_reload_on_save'] : 'disabled')
            .trigger('change');
    }
    // Init the queryBuilder
    queryBuilderOpts.filters = queryBuilderFilters;
    let opts = $.extend(true, {}, queryBuilderOpts);
    if (args) {
        //console.log('xx', args['conditions']);
        opts['rules'] = args['conditions'];
    }
    $("div[id=panel_conditions-panel-x" + panelNumber + "]").queryBuilder(opts);
    //console.log('finishing up adding panel', panelNumber, opts);
    refreshTooltips();
    return false;
});
// Change panel show_when
$(document).on('change', "select[id^=panel_showwhen-panel-x]", function() {
    var panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    if ($(this).val() == 'conditions') {
        $("#panel_conditions-panel-x" + panelNumber).collapse("show");
    } else {
        $("#panel_conditions-panel-x" + panelNumber).collapse("hide");
    }
});
// Change panel mode
$(document).on('change', "select[id^=panel_mode-panel-x]", function() {
    var panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    switch ($(this).val()) {
        case 'write':
        case 'show-inputs':
            $("#panel_mode_warning-panel-x" + panelNumber).removeClass("d-none").show();
            $("#panel_trigger_reload_on_save-panel-x" + panelNumber).removeClass("d-none").show();
            break;
        default:
            $("#panel_mode_warning-panel-x" + panelNumber).addClass("d-none").hide();
            $("#panel_trigger_reload_on_save-panel-x" + panelNumber).addClass("d-none").show();
            $("#panel_mode_unwriteable_columns_warning-panel-x" + panelNumber).addClass("d-none").hide();
    }
});
// Change panel availability
$(document).on('change', "select[id^=panel_availability-panel-x]", function() {
    let panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    if ($(this).val() == 'available-between') {
        $("#panel_availability_config-panel-x" + panelNumber).removeClass("d-none").show();
    } else {
        $("#panel_availability_config-panel-x" + panelNumber).addClass("d-none").hide();
    }
});
// Change panel collapsible
$(document).on('change', "select[id^=panel_collapsible-panel-x]", function() {
    let panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    switch ($(this).val()) {
        case 'disabled':
        case 'linked':
            $("#panel_collapsible_default_display-panel-x" + panelNumber).addClass("d-none");
            break;
        case 'enabled':
            $("#panel_collapsible_default_display-panel-x" + panelNumber).removeClass("d-none");
    }
});
// Raise or lower panel
$(document).on('click', "button[id^=panel_lower-panel-x],button[id^=panel_raise-panel-x]", function() {
    var panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    var direction = '';
    var checkIndexExtreme = 0;
    if ($(this).attr('id').indexOf('raise') > 0) {
        direction = 'raise';
        checkIndexExtreme = 0;
    } else {
        direction = 'lower';
        checkIndexExtreme = panels.length - 1;
    }
    if (panels[checkIndexExtreme] == panelNumber) {
        //console.log('cannot ' + direction + ' any more');
    } else {
        var panelPosition = panels.indexOf(panelNumber);
        if (direction == 'raise') {
            $("#panel-panel-x" + panelNumber).insertBefore("#panel-panel-x" + panels[panelPosition - 1]);
            panels.move(panelPosition, panelPosition - 1);
        } else if (direction == 'lower') {
            $("#panel-panel-x" + panelNumber).insertAfter("#panel-panel-x" + panels[panelPosition + 1]);
            panels.move(panelPosition, panelPosition + 1);
        }
    }
    updateStringified();
});
// Insert data field trigger
$(document).on('click', '.sres-select-column-trigger', function(){
    let target = $(this).attr('data-sres-insert-column-target');
    if (typeof target == 'undefined') {
        target = $(this).siblings('.sres-tinymce-editor').attr('id');
    }
    show_column_chooser(target, '$');
});
// Deleting a panel
$(document).on('click', "button[id^=panel_delete-panel-x]", function() {
    var panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    $(this).parents('.sres-panel-parent').remove();
    panels.splice(panels.indexOf(panelNumber), 1);
    updateStringified();
});
// Cloning a panel
function collectPanelConfig(panelNumber){
    return {
        content:                        $('div#panel_content-panel-x' + panelNumber).html(),
        show_when:                      $('[id=panel_showwhen-panel-x' + panelNumber + ']').val(),
        conditions:                     $("div[id=panel_conditions-panel-x" + panelNumber + "]").queryBuilder('getRules'),
        mode:                           $('[id=panel_mode-panel-x' + panelNumber + ']').val(),
        availability:                   $('[id=panel_availability-panel-x' + panelNumber + ']').val(),
        availability_from_str:          $('[id=panel_availability_from_date-panel-x' + panelNumber + ']').val() + ' ' + $('[id=panel_availability_from_time-panel-x' + panelNumber + ']').val(),
        availability_to_str:            $('[id=panel_availability_to_date-panel-x' + panelNumber + ']').val() + ' ' + $('[id=panel_availability_to_time-panel-x' + panelNumber + ']').val(),
        collapsible:                    $('[id=panel_collapsible-panel-x' + panelNumber + ']').val(),
        collapsible_default_display:    $('[id=panel_collapsible_default_display-panel-x' + panelNumber + ']').val(),
        trigger_reload_on_save:         $('[id=panel_trigger_reload_on_save-panel-x' + panelNumber + ']').val()
    }
}
$(document).on('click', "button[id^=panel_clone-panel-x]", function() {
    let sourcePanelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    let sourcePanelConfig = collectPanelConfig(sourcePanelNumber);
    //console.log('sourcePanelConfig', sourcePanelConfig);
    if (panels.indexOf(sourcePanelNumber) == panels.length - 1) {
        // add at the end
        addPanelBeforePosition(-1, sourcePanelConfig);
    } else {
        addPanelBeforePosition(panels[panels.indexOf(sourcePanelNumber) + 1], sourcePanelConfig);
    }
    updateStringified();
});
// Repopulate existing panels
$(document).ready(function(){
    existingPanels.forEach(function(existingPanel){
        //console.log('repopulating section', existingPanel);
        $("button[id=add_panel]").trigger(
            'click',
            existingPanel
        );
    });
});
// Hover to add
$(document).on('mouseenter', '.sres-panel-parent', function(event){
    var panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    $(this).append('<div class="sres-interpanel-hover-controls"><button type="button" class="btn btn-sm btn-primary sres-interpanel-hover-add" data-sres-position="' + panelNumber + '"><span class="fa fa-plus"></span> Add panel</button></div>');
});
$(document).on('mouseleave', '.sres-panel-parent', function(event){
    $(this).find('.sres-interpanel-hover-controls').remove();
});

/**
    Validation
**/
$(document).on('submit', '#sv_edit_form', function(e){
    let messages = [];
    // Make sure at least one panel exists
    if (panels.length == 0) {
        messages.push("<p>You need at least one panel.</p>");
    }
    // Pool all panel contents and check for column references
    let allPanelContents = '';
    let allPanelActiveConditions = '';
    $("div[id^=panel_content-panel-x]").each(function(index, element){
        allPanelContents = allPanelContents + $(this).html();
        if ($(this).parents('.sres-panel-parent').find('.sres-panel-show-when').val() == 'conditions') {
            allPanelActiveConditions += JSON.stringify($(this).parents('.sres-panel-parent').find('.sres-querybuilder-container').queryBuilder('getRules'));
        }
    });
    var columnReferences = (allPanelContents + allPanelActiveConditions).match(/(?:[\$"])(COL_|SMY_)[A-Z0-9a-z_\.]+(?:[\$"])/g);
    if (!columnReferences) {
        messages.push("<p>At least one data field needs to be referred to in at least one panel in this portal, but it doesn't look like this is the case. At least one data field is needed so that the SRES knows which list(s) from which to draw data.</p>");
    }
    // Block submission and show errors if necessary
    if (messages.length > 0) {
        $("#submit_check_warning_body").html(messages.join(''));
        $("#submit_check_warning").modal('show');
        e.preventDefault();
        return false;
    }
    return true;
});

/**
    Student writeabiliy check
**/
function checkStudentWriteability(forceCheck) {
    // check if any column references have changed
    $("div[id^=panel_content-panel-x]").each(function(index, element){
        let columnReferences = $(this).html().match(/(?:[\$])(COL_)[A-Z0-9a-z_\.]+(?:[\$])/g);
        let panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
        if (forceCheck || ($(this).data('sres-column-references') == null) || ($(this).data('sres-column-references').length != columnReferences.length)) {
            // update
            $(this).data('sres-column-references', columnReferences);
            // check if request is needed
            switch ($(this).parents('.sres-panel-parent').find('.sres-panel-mode-selector').val()) {
                case 'write':
                case 'show-inputs':
                    // request
                    $.ajax({
                        url: ENV['GET_COLUMN_WRITEABILITY_ENDPOINT'],
                        method: 'POST',
                        data: {
                            input_string: $(this).html(),
                            _extra: JSON.stringify({
                                panel_number: panelNumber
                            })
                        },
                        success: function(data) {
                            data = JSON.parse(data);
                            let thisPanelNumber = data._extra.panel_number;
                            let unwriteableColumns = data.columns.filter(function(column) {
                                if (column.authorised) {
                                    return !column.student_editing_allowed;
                                }
                                return false;
                            });
                            if (unwriteableColumns.length) {
                                $("#panel_mode_unwriteable_columns_warning-panel-x" + thisPanelNumber)
                                    .removeClass("d-none").show()
                                    .find('.sres-unwriteable-columns-list').html(function(){
                                        let str = 'These column(s) may need reconfiguration: ';
                                        unwriteableColumns.forEach(function(unwriteableColumn){
                                            str += unwriteableColumn.friendly_name;
                                            if (!unwriteableColumn.is_active) {
                                                str += ' <span class="fa fa-info-circle"></span> Active dates do not include today.';
                                            }
                                            if (!unwriteableColumn.is_self_data_entry_enabled) {
                                                str += ' <span class="fa fa-info-circle"></span> Student editing permissions not switched on.';
                                            }
                                            str += '&nbsp;<a href="' + unwriteableColumn.edit_link + '" title="Edit column settings" target="_blank"><span class="fa fa-cog"></span></a> ';
                                        });
                                        return str;
                                    });
                            } else {
                                $("#panel_mode_unwriteable_columns_warning-panel-x" + thisPanelNumber)
                                    .addClass("d-none").hide()
                                    .find('.sres-unwriteable-columns-list').html('');
                            }
                        }
                    });
            }
        }
    });
}
$(document).on('change', '.sres-panel-mode-selector', function(){
    let panelNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    switch ($(this).val()) {
        case 'write':
        case 'show-inputs':
            checkStudentWriteability(true);
            break;
        default:
            $("#panel_mode_unwriteable_columns_warning-panel-x" + panelNumber).addClass("d-none").hide();
            break;
    }
});
$(document).on('click', '.sres-unwriteable-columns-recheck', function(){
    $.notify({message:'Checking...'}, {type: 'info'});
    checkStudentWriteability(true);
});
$(document).on('sres:columnreferenceinserted', function(event, args) {
    checkStudentWriteability();
});

// Pick a column
$(document).on('click', 'span.sres-condition-column-placeholder', function(){
	let receiver = $(this).siblings('input:hidden.sres-condition-column-receiver');
    show_column_chooser(receiver.attr('id'), '', null, null, null, receiver.attr('data-sres-tableuuid'), receiver.val(), true, "teacherallocation", true, true);
});




