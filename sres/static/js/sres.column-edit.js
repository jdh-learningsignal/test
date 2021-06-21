$(document).ready(function(){
    $("#permissions_edit_except_user").chosen({width:'50%', placeholder_text_multiple:'Select user(s) if necessary'});
});
    
$(document).on('click', 'a.sres-data-type-unlock', function(){
    $('input:radio[name=dataType]').prop('disabled', false);
});

/** Column data type **/
$(document).on('click, change', "input:radio[name=dataType]", function(){
    console.log('blah');
    $("div[data-role=detailedOptions]")
        .addClass('d-none')
        .find("input").prop('required', false);
    switch ($(this).val()) {
        case 'counter':
            $("div[id='counter']")
                .removeClass('d-none')
                .find("input").prop('required', true);
            $("div[id=row_auto_proceed]").removeClass('d-none');
            break;
        case 'toggle':
        case 'attendance': //i.e. select-and-scan
            $("div[id=row_auto_proceed]").removeClass('d-none');
            //purposely no break;
        case 'mark': //i.e. scan-and-select
            $("div[id='selectFromList']")
                .removeClass('d-none')
                /*.find("input").attr('required','required')*/;
            //$("div[id='recordTimestamp']").removeClass('d-none');
            if ($(this).val() != 'toggle') { $("div[id='allowFreeInput']").removeClass('d-none'); }
            break;
        case 'signinoutmemory':
            $("div[id=signinoutmemory]").removeClass('d-none');
            $("#signinoutmemory_firstweek").prop('required', true);
        case 'signinout':
            $("div[id=signinout]").removeClass('d-none');
            $("div[id=signinout_messages]").removeClass('d-none');
        case 'submission': //i.e. timestamp
            $("div[id=row_auto_proceed]").removeClass('d-none');
            break;
        case 'multiEntry':
            $("div[id=multiEntry]").removeClass('d-none');
            break;
        case 'image':
            $("div[id=datatype_image]").removeClass('d-none');
            break;
        case 'file':
            $("div[id=datatype_file]").removeClass('d-none');
            break;
    }
});
$(document).on('change', 'select[name=auto_reset_active], select[name=allowFreeInput], select[name=auto_backup_email_active], select[name=notifyEmail]', function(){
    if ($(this).val() == 'true') {
        $('#' + $(this).attr('name') + '_options').removeClass('d-none');
    } else {
        $('#' + $(this).attr('name') + '_options').addClass('d-none');
    }
});
$(document).ready(function(){
    $("input:radio[name=dataType]:checked").each(function(){
        $(this).trigger('change');
    });
    $('select[name=auto_reset_active], select[name=allowFreeInput], select[name=auto_backup_email_active], select[name=notifyEmail]').trigger('change');
});

/** Bulk mode toggles **/
$(document).on('change', 'input:checkbox[name=toggle_bulk_add_mode]', function() {
	if ($(this).prop('checked')) {
		// bulk mode
		$('div.sres-add-single')
			.addClass('d-none')
			.find('[data-sres-required]').removeProp('required').removeAttr('required');
		$('div.sres-add-bulk')
            .removeClass('d-none')
            .find('[data-sres-required]').prop('required', true);
	} else {
		// single mode
		$('div.sres-add-single')
			.removeClass('d-none')
			.find('[data-sres-required]').prop('required', true);
		$('div.sres-add-bulk')
            .addClass('d-none')
            .find('[data-sres-required]').removeProp('required').removeAttr('required');
	}
}).ready(function(){
	$('input:checkbox[name=toggle_bulk_add_mode]').trigger('change');
});

/** Quick info inserting **/
$(document).on('click', "button.sres-quickinfo-insert-field", function() {
    let targetEditorId = $(this).siblings('.sres-tinymce-editor').attr('id');
    if ($(this).attr('data-sres-field') == 'column') {
        show_column_chooser(tinymce.editors[targetEditorId].id, '$', null, false);
    } else {
        tinymce.editors[targetEditorId].insertContent($(this).val());
    }
});

/** Notification email editor **/
$(document).on('click', 'button.sres-notifyemail-subject-insertdatafield', function(){
	show_column_chooser('notifyEmailSubject', '$', null, false);
});
$(document).on('click', 'button.sres-notifyemail-body-insertdatafield', function(){
	show_column_chooser(tinymce.editors['notifyEmailBody'].id, '$', null, false);
});

/**
	Date stepping
**/
$(document).on("click", "button.sres-date-shortcut", function(){
	let dateElement = $(this).parents(".input-group").find("input[type=date]");
	if ($(this).hasClass("sres-date-shortcut-monday")) {
		dateElement.val(moment().day(1).format("YYYY-MM-DD"));
	} else if ($(this).hasClass("sres-date-shortcut-friday")) {
		dateElement.val(moment().day(5).format("YYYY-MM-DD"));
	} else if ($(this).hasClass("sres-date-shortcut-today")) {
		dateElement.val(moment().format("YYYY-MM-DD"));
	} else if ($(this).hasClass("sres-date-shortcut-plus1")) {
		dateElement.get(0).stepUp(1);
	} else if ($(this).hasClass("sres-date-shortcut-minus1")) {
		dateElement.get(0).stepDown(1);
	} else if ($(this).hasClass("sres-date-shortcut-plus7")) {
		dateElement.get(0).stepUp(7);
	} else if ($(this).hasClass("sres-date-shortcut-minus7")) {
		dateElement.get(0).stepDown(7);
	}
});

/** 
    Select-from-list editor modal ('edit selectable options')
**/
function parseSelectableOptions(sourceData){
    let dataArray = [];
    try {
        dataArray = JSON.parse(sourceData);
    } catch(err) {
        // Might just be comma-separated string?
        let data = sourceData.split(",");
        data.forEach(function(point) {
            dataArray.push({
                "display": point,
                "value": point,
                "description":''
            });
        });
    }
    return dataArray;
}
function parseSelectableOptionsForRow(source){
    let sourceData = $("input[id=" + source + "]").val();
    let dataArray = [];
    if (sourceData != "") {
        dataArray = parseSelectableOptions(sourceData);
    }
    return dataArray;
}
$(document).on('show.bs.modal', "#modal_select_from_list_editor", function(event) {
    var button = $(event.relatedTarget);
    var source = button.data("sres-modal-source");
    var modal = $(this);
    // Clear existing modal
    modal.find("div[class~='sres-select-from-list-row']:not([id='select_from_list_row_template'])").each(function() {
        $(this).remove();
    });
    // Update source/target
    modal.find("#modal_select_from_list_editor_confirm").data("sres-modal-source", source);
    // Grab source string and parse
    var sourceData = $("input[id=" + source + "]").val();
    //console.log(sourceData);
    if (sourceData != "") {
        // Populate if necessary
        dataArray = parseSelectableOptions(sourceData);
        //console.log(dataArray);
        dataArray.forEach(function(dataRow) {
            var row = addModalSelectFromListRow();
            row.find("input[class~='sres-select-from-list-row-display']").val(dataRow.display);
            row.find("input[class~='sres-select-from-list-row-value']").val(dataRow.value);
            row.find("textarea[class~='sres-select-from-list-row-description']").val(dataRow.description ? dataRow.description : '');
        });
    }
    refreshTooltips();
});
$(document).on('click', "#modal_select_from_list_editor_confirm", function(event, args) {
    // Defensive checks for user
    let rows = gatherModalSelectFromListRows();
    let displays = rows.map(function(obj){return obj.display});
    let values = rows.map(function(obj){return obj.value});
    const count = names => names.reduce((a, b) => Object.assign(a, {[b]: (a[b] || 0) + 1}), {});
    const duplicates = dict => Object.keys(dict).filter((a) => dict[a] > 1);
    let messages = [];
    if (duplicates(count(displays)).length) {
        messages.push('The text for "What to display on the form" appears to be duplicated in some options.');
    }
    if (displays.map(function(obj){return obj == "" ? 1 : 0}).reduce(function(total,value){return total += value})) {
        messages.push('Some text for "What to display on the form" appears to be blank.');
    }
    if (duplicates(count(values)).length) {
        messages.push('<span class="badge badge-danger">Critical</span> The values for "What to save in the database" appear to be duplicated in some options. This means if a user selects one of these options, the data that is saved to the database will be ambiguous.');
    }
    if (values.map(function(obj){return obj == "" ? 1 : 0}).reduce(function(total,value){return total += value})) {
        messages.push('Some value(s) for "What to save in the database" may be blank. This means no value (i.e. blank data) will be saved to the database for these option(s).');
    }
    if ( messages.length == 0 || (typeof args !== 'undefined' && typeof args.ignoreWarnings !== 'undefined' && args.ignoreWarnings == true) ) {
        // proceed
        // Store data and exit
        let source = $(this).data("sres-modal-source");
        updateModalSelectFromListSource(source);
        $("#modal_select_from_list_editor").modal('hide');
    } else {
        let warningHtml = '<p><span class="fa fa-exclamation-triangle"></span> There may be some issues with these settings.</p><ul><li>';
        warningHtml += messages.join('</li><li>');
        warningHtml += '</li></ul><p>Do you wish to proceed anyway?</p>';
        $('#modal_select_from_list_warning .modal-body').html(warningHtml);
        $('#modal_select_from_list_warning').modal('show');
    }
});
$(document).on('click', '#modal_select_from_list_warning_proceed', function(event){
    $('#modal_select_from_list_warning').modal('hide');
    $('#modal_select_from_list_editor_confirm').trigger('click', {ignoreWarnings: true});
});
$(document).on('click', "#select_from_list_add_option", function(event) {
    addModalSelectFromListRow();
    refreshTooltips();
});
$(document).on('click', "button[class~='sres-select-from-list-row-delete']", function(event) {
    event.stopPropagation();
    $(this).closest(".sres-select-from-list-row").remove();
    return false;
});
$(document).on('click', "button[class~='sres-select-from-list-row-more-options-toggle']", function(event) {
    event.stopPropagation();
    $(this).closest(".sres-select-from-list-row").find(".sres-select-from-list-row-more-options").collapse('toggle');
    return false;
});
$(document).on('click', "button[class~='sres-select-from-list-row-move-up'], button[class~='sres-select-from-list-row-move-down']", function(event) {
    event.stopPropagation();
    if ($(this).attr('data-sres-direction') == 'up') {
        var p = $(this).closest(".sres-select-from-list-row").prev("div[class~='sres-select-from-list-row']:not([id='select_from_list_row_template'])");
        if (p.length != 0) {
            var r = $(this).closest('.sres-select-from-list-row').detach();
            r.insertBefore(p);
        }
    } else {
        var n = $(this).closest(".sres-select-from-list-row").next("div[class~='sres-select-from-list-row']:not([id='select_from_list_row_template'])");
        if (n.length != 0) {
            var r = $(this).closest('.sres-select-from-list-row').detach();
            r.insertAfter(n);
        }
    }
});
function addModalSelectFromListRow() {
    var clonedRow = $("#select_from_list_row_template").clone()
        .appendTo("#select_from_list_rows_container")
        .removeClass('d-none')
        .removeAttr('id');
    return clonedRow;
}
function gatherModalSelectFromListRows() {
    var arr = [];
    $("#select_from_list_rows_container div[class~='sres-select-from-list-row']:not([id=select_from_list_row_template])").each(function(){
        //console.log($(this));
        arr.push({
            "display":$(this).find("input[class~='sres-select-from-list-row-display']").val(),
            "value":$(this).find("input[class~='sres-select-from-list-row-value']").val(),
            "description":$(this).find("textarea[class~='sres-select-from-list-row-description']").val()
        });
    });
    return arr;
}
function updateModalSelectFromListSource(targetInputId) {
    var arr = gatherModalSelectFromListRows();
    //console.log(targetInputId, JSON.stringify(arr));
    $("input[id=" + targetInputId + "]")
        .val(JSON.stringify(arr))
        .trigger('change');
}
$(document).on('change', '[data-sres-role=multi_entry_select]', function(){
    /** Triggered when the selectable options change **/
    // Check slider configuration
    if ($(this).parents('.sres-multientry-row').find('.sres-multientry-type-selector').val() == 'slider') {
        let $warningNotice = $(this).parents('.sres-multientry-selectable-options').find('.sres-multientry-slider-warning-nonnumeric-options');
        if ($(this).parents('.sres-multientry-selectable-options').find('.sres-multientry-slider-mode').val().includes('numeric')) {
            let selectableOptions = parseSelectableOptions($(this).val());
            let nonNumericOptionExists = false;
            selectableOptions.forEach(function(option){
                if (isNaN(option.value)) {
                    nonNumericOptionExists = true;
                }
            });
            if (nonNumericOptionExists) {
                $warningNotice.removeClass('d-none');
            } else {
                $warningNotice.addClass('d-none');
            }
        } else {
            $warningNotice.addClass('d-none');
        }
    }
}).ready(function(){
    $('[data-sres-role=multi_entry_select]').trigger('change');
});

/**
    Multi entry
**/
$(document).ready(function(){
    $("#multi_entry_sortable").sortable({
        handle: '.sres-multientry-handle',
        forceHelperSize: true
    });
    $('select[name^=multi_entry_type_]').each(function(){
        $(this).trigger('change');
    });
});
$(document).on("click", "#multi_entry_add_row", function(){
    add_row_multi_entry();
});
function initialiseLabelEditors(n) {
    let selector = '';
    if (typeof n == 'undefined') {
        selector = 'div[id^=multi_entry_label_' + ']';
    } else {
        selector = 'div[id=multi_entry_label_' + n + ']';
    }
    let initConfig = Object.assign({}, basicConfig);
    initConfig.selector = selector;
    initConfig.forced_root_block = '';
    initConfig.extended_valid_elements = 'i[class]';
    if (ENV['FONT_FORMATS']) {
        initConfig['font_formats'] = ENV['FONT_FORMATS'];
    }
    tinymce.init(initConfig);
    return true;
}
function add_row_multi_entry(cloneSourceRowN){
    var clonedRow;
    var cloning = false;
    if (typeof cloneSourceRowN != 'undefined') {
        // need to clone
        clonedRow = $("div[class~=sres-multientry-row][id='multi_entry_" + cloneSourceRowN + "']").clone();
        cloning = true;
    } else {
        // just add a blank
        clonedRow = $('div[class~=sres-multientry-row][id^=multi_entry_]:first').clone();
    }
    multi_entry_count++;
    clonedRow
        .attr('data-n', multi_entry_count)
        .find('div[id^=multi_entry_label_]')
            .attr('id', 'multi_entry_label_' + multi_entry_count)
            .html(cloning ? "Clone of " + $("div[id=multi_entry_label_" + cloneSourceRowN + "]").html() : '')
            .end()
        .find('div[id^=multi_entry_maximumValue_]')
            .attr('id', 'multi_entry_maximumValue_' + multi_entry_count)
            .html(cloning ? "Clone of " + $("div[id=multi_entry_maximumValue_" + cloneSourceRowN + "]").html() : '')
            .end()
        .find('select[name^=multi_entry_regex_]')
            .attr('name', 'multi_entry_regex_' + multi_entry_count)
            .val(cloning ? $("select[name=multi_entry_regex_" + cloneSourceRowN + "]").val() : '.*')
            .end()
        .find('[data-n]')
            .attr('data-n', multi_entry_count)
            .end()
        .find('[data-sres-n]')
            .attr('data-sres-n', multi_entry_count)
            .end()
        .find('select[name^=multi_entry_type_]')
            .attr('name', 'multi_entry_type_' + multi_entry_count)
            .attr('data-n', multi_entry_count)
            .val(cloning ? $("select[name=multi_entry_type_" + cloneSourceRowN + "]").val() : 'regex')
            .end()
        .find('select[name^=multi_entry_required_]')
            .attr('name', 'multi_entry_required_' + multi_entry_count)
            .attr('data-n', multi_entry_count)
            .end()
        .find('select[name^=multi_entry_render_calculated_value_]')
            .attr('name', 'multi_entry_render_calculated_value_' + multi_entry_count)
            .attr('data-n', multi_entry_count)
            .end()
        .find('select[name^=multi_entry_editing_allowed_by_]')
            .attr('name', 'multi_entry_editing_allowed_by_' + multi_entry_count)
            .attr('data-n', multi_entry_count)
            .end()
        .find('input[name^=multi_entry_render_calculated_value_config_]')
            .attr('name', 'multi_entry_render_calculated_value_config_' + multi_entry_count)
            .attr('data-n', multi_entry_count)
            .end()
        .find('input[name^=multi_entry_select_]')
            .attr('name', 'multi_entry_select_' + multi_entry_count)
            .attr('id', 'multi_entry_select_' + multi_entry_count)
            .val(cloning ? $("input[name=multi_entry_select_" + cloneSourceRowN + "]").val() : '')
            .end()
        .find('select[name^=multi_entry_select_mode_]')
            .attr('name', 'multi_entry_select_mode_' + multi_entry_count)
            .end()
        .find('select[name^=multi_entry_select_display_mode_]')
            .attr('name', 'multi_entry_select_display_mode_' + multi_entry_count)
            .attr('id', 'multi_entry_select_display_mode_' + multi_entry_count)
            .end()
        .find('select[name^=multi_entry_slider_mode_]')
            .attr('name', 'multi_entry_slider_mode_' + multi_entry_count)
            .attr('id', 'multi_entry_slider_mode_' + multi_entry_count)
            .end()
        .find('input[name^=multi_entry_slider_step_]')
            .attr('name', 'multi_entry_slider_step_' + multi_entry_count)
            .attr('id', 'multi_entry_slider_step_' + multi_entry_count)
            .end()
        .find('select[name^=multi_entry_range_mode_]')
            .attr('name', 'multi_entry_range_mode_' + multi_entry_count)
            .attr('id', 'multi_entry_range_mode_' + multi_entry_count)
            .end()
        .find('select[name^=multi_entry_accordion_header_]')
            .attr('name', 'multi_entry_accordion_header_' + multi_entry_count)
            .attr('id', 'multi_entry_accordion_header_' + multi_entry_count)
            .end()
        .find('select[name^=multi_entry_extra_save_button_]')
            .attr('name', 'multi_entry_extra_save_button_' + multi_entry_count)
            .attr('id', 'multi_entry_extra_save_button_' + multi_entry_count)
            .end()
        .find('button[id^=multi_entry_editor_select_]')
            .attr('id', 'multi_entry_editor_select_' + multi_entry_count)
            .attr('data-sres-modal-source', 'multi_entry_select_' + multi_entry_count)
            .end()
        .find('a[id^=multi_entry_delete_row_]')
            .attr('id', 'multi_entry_delete_row_' + multi_entry_count)
            .removeClass('d-none')
            .end()
        .attr('id', 'multi_entry_' + multi_entry_count)
        .appendTo('#multi_entry_sortable')
        .find('select[name=multi_entry_type_' + multi_entry_count + ']')
            .trigger('change');
    $('#multi_entry_sortable div[class~=sres-multientry-row] a[id^=multi_entry_delete_row_]').removeClass('d-none');
    initialiseLabelEditors(multi_entry_count);
    return clonedRow;
}
$(document).on("click", "button[id^='multi_entry_clone_row_']", function(){
    var n = $(this).closest('div[class~=sres-multientry-row]').attr('data-n');
    add_row_multi_entry(n);
});
$(document).on("click", "button[id^='multi_entry_delete_row_']", function(){
    $(this).closest('div[class~=sres-multientry-row]').remove();
    if ($('#multi_entry_sortable div[class~=sres-multientry-row]').length <= 1){
        $('#multi_entry_sortable div[class~=sres-multientry-row] a[id^=multi_entry_delete_row_]').addClass('d-none');
    };
});
function serialize_multi_entry(){
    $('input[name=multi_entry_sort_order]').val($("#multi_entry_sortable").sortable('serialize', {key:'id'}));
};
$(document).on('change', 'select[name^=multi_entry_type_]', function(){
    let o = $(this);
    let n = o.attr('data-n');
    let type = o.val();
    // Hide inputs
    $("div.sres-multientry-selectable-options[data-sres-n=" + n + "]").addClass('d-none');
    $("div.sres-multientry-label-options[data-sres-n=" + n + "]").addClass('d-none');
    $("div.sres-multientry-regex-options[data-sres-n=" + n + "]").addClass('d-none');
    $(".sres-multientry-select-mode[data-sres-n=" + n + "]").addClass('d-none');
    $(".sres-multientry-select-display-mode-container[data-sres-n=" + n + "]").addClass('d-none');
    $(".sres-multientry-slider-mode-container[data-sres-n=" + n + "]").addClass('d-none');
    $(".sres-multientry-slider-config[data-sres-n=" + n + "]").addClass('d-none');
    // Selectively show inputs
    switch (type) {
        case "regex":
        //case "regex-long"
            $("div.sres-multientry-regex-options[data-sres-n=" + n + "]").removeClass('d-none');
            break;
        case "select":
            $(".sres-multientry-select-display-mode-container[data-sres-n=" + n + "]").removeClass('d-none');
        case "dropdown":
            $("div.sres-multientry-selectable-options[data-sres-n=" + n + "]").removeClass('d-none');
            $(".sres-multientry-select-mode[data-sres-n=" + n + "]").removeClass('d-none');
            break;
        case "slider":
            $("div.sres-multientry-selectable-options[data-sres-n=" + n + "]").removeClass('d-none');
            $(".sres-multientry-slider-mode-container[data-sres-n=" + n + "]").removeClass('d-none');
            $("select[name=multi_entry_slider_mode_" + n + "]").trigger('change');
            break;
        case "label-only":
            $("div.sres-multientry-label-options[data-sres-n=" + n + "]").removeClass('d-none');
            break;
    }
    // Selectively show config for calculated value
    switch (type) {
        case 'regex':
        case 'slider':
            $(".sres-multientry-render-calculated-value-config[data-sres-n=" + n + "]").removeClass('d-none');
            break;
        default:
            $(".sres-multientry-render-calculated-value-config[data-sres-n=" + n + "]").addClass('d-none');
            $('select[name=multi_entry_render_calculated_value_' + n + ']').val('no').trigger('change');
            break;
    }
});
$(document).on('change', '.sres-multientry-slider-mode', function(){
    let n = $(this).attr('data-sres-n');
    $(".sres-multientry-slider-step[data-sres-n=" + n + "]").addClass('d-none');
    $(".sres-multientry-range-mode[data-sres-n=" + n + "]").addClass('d-none');
    $(this).parents('.sres-multientry-selectable-options').find('[data-sres-role=multi_entry_select]').trigger('change');
    switch ($(this).val()) {
        case 'numeric-free':
            $(".sres-multientry-range-mode[data-sres-n=" + n + "]").removeClass('d-none');
        case 'numeric-snap':
            $(".sres-multientry-slider-step[data-sres-n=" + n + "]").removeClass('d-none');
            break;
    }
});

/**
    Clone column
**/
$(document).ready(function(){
    $("#clone_to_other_list_selector").chosen({width:'100%'});
    $("#clone_to_other_list_button").on('click', function() {
        window.location = $(this).attr('data-sres-action') + '?target_table_uuid=' + $("#clone_to_other_list_selector").val();
    });
});
$(document).on('click', '#cloneButton', function(){
    window.location = $(this).attr('data-sres-action');
});

/**
    Delete column
**/
$(document).ready(function(){
    $("#deleteButton").on('click', function() {
        window.location = $(this).attr('data-sres-action');
    });
});

/**
    Self and peer data entry by students
**/
$(document).ready(function(){
    $("#permissions_edit_mode_student").trigger('change');
    $("#custom_options_peer_data_entry_condition_column").trigger('change');
});
$(document).on('change', '#permissions_edit_mode_student', function(){
    switch ($(this).val()) {
        case 'deny':
            $(".sres-student-data-entry-container").collapse('hide');
            $(".sres-student-peer-data-entry-container").collapse('hide');
            $(".sres-student-self-data-entry-container").collapse('hide');
            break;
        case 'self':
            $(".sres-student-data-entry-container").collapse('show');
            $(".sres-student-self-data-entry-container").collapse('show');
            $("#custom_options_student_direct_access").trigger('change');
            break;
        case 'self,peer':
            $(".sres-student-data-entry-container").collapse('show');
            $(".sres-student-self-data-entry-container").collapse('show');
            $("#custom_options_student_direct_access").trigger('change');
            break;
        case 'peer':
            $(".sres-student-data-entry-container").collapse('show');
            $(".sres-student-self-data-entry-container").collapse('hide');
            $("#custom_options_student_direct_access").trigger('change');
            break;
    }
});
$(document).on('change', '#custom_options_student_direct_access', function(){
    if ($(this).val() != 'deny' && $("#permissions_edit_mode_student").val().indexOf('peer') != -1) {
        $(".sres-student-peer-data-entry-container").collapse('show');
        $(".sres-student-data-entry-warning").removeClass('d-none');
    } else {
        $(".sres-student-peer-data-entry-container").collapse('hide');
        $(".sres-student-data-entry-warning").addClass('d-none');
    }
});
$(document).on('change', '#custom_options_peer_data_entry_condition_column', function(){
    if ($(this).val() == '') {
        $(".sres-peer-data-entry-condition-config").addClass('d-none');
    } else {
        $(".sres-peer-data-entry-condition-config").removeClass('d-none');
    }
});

/**
    Only show when
**/
$(document).ready(function(){
    $('#custom_options_only_show_condition_column').trigger('change');
});
$(document).on('change', '#custom_options_only_show_condition_column', function(){
    if ($(this).val() == '') {
        $(".sres-only-when-condition-config").addClass('d-none');
    } else {
        $(".sres-only-when-condition-config").removeClass('d-none');
    }
});

/**
    Render calculated value
**/
$(document).on('change', 'select[name^=multi_entry_render_calculated_value_]', function(){
    if ($(this).val() == 'no') {
        $(this).siblings('.sres-multientry-render-calculated-value-config-button').addClass('d-none');
    } else {
        $(this).siblings('.sres-multientry-render-calculated-value-config-button').removeClass('d-none');
    }
}).ready(function(){
    $('select[name^=multi_entry_render_calculated_value_]').trigger('change');
});
$(document).on('click', 'button.sres-multientry-render-calculated-value-config-button', function(){
    let n = $(this).siblings('input[name^=multi_entry_render_calculated_value_config_]').data('n');
    let config = $('input[name=multi_entry_render_calculated_value_config_' + n + ']').val();
    config = JSON.parse(config);
    // render existing subfields - except current one
    $('#render_calculated_values_editor_subfield_list').html('');
    $('.sres-multientry-row').each(function(){
        if ($(this).data('n') != n) {
            let _n = $(this).data('n');
            let template = Handlebars.compile(document.getElementById("render_calculated_values_editor_subfield_list_item").innerHTML);
            $('#render_calculated_values_editor_subfield_list').append(template({
                subfield: parseInt(_n) - 1, // Convert this into standard 0-based index
                label: $(this).find('.sres-multientry-label').text()
            }));
        }
    });
    // update the configuration in the modal
    $('#modal_render_calculated_values_editor [data-sres-key]').each(function(){
        let _key = $(this).data('sres-key');
        if (_key == 'subfields' && config.subfields) {
            let subfieldIndex = config.subfields.indexOf( $(this).val() );
            if (subfieldIndex > -1) {
                $(this).prop('checked', true);
                let multiplier = 1;
                if (typeof config.subfield_multipliers !== 'undefined' && typeof config.subfield_multipliers[subfieldIndex] !== 'undefined') {
                    multiplier = config.subfield_multipliers[subfieldIndex];
                }
                $(this)
                    .parents('.sres-render-calculated-values-editor-list-item')
                    .find("[id='render_calculated_values_editor_subfield_list_item_multiplier_" + $(this).val() + "']")
                    .val( multiplier );
            }
        } else {
            if (config.hasOwnProperty(_key)) {
                $(this).val(config[_key]);
            }
        }
    });
    // Show modal
    $('#modal_render_calculated_values_editor_confirm').data('sres-current-n', n);
    $('#modal_render_calculated_values_editor').modal('show');
});
$(document).on('click', '#modal_render_calculated_values_editor_confirm', function(){
    let config = {
        subfields: [],
        subfield_multipliers: []
    };
    let currentN = $('#modal_render_calculated_values_editor_confirm').data('sres-current-n');
    // save the configuration from the modal
    $('#modal_render_calculated_values_editor [data-sres-key]').each(function(){
        let _key = $(this).data('sres-key');
        if (_key == 'subfields') {
            if ($(this).prop('checked')) {
                config.subfields.push( $(this).val() );
                config.subfield_multipliers.push( 
                    $(this)
                        .parents('.sres-render-calculated-values-editor-list-item')
                        .find("[id='render_calculated_values_editor_subfield_list_item_multiplier_" + $(this).val() + "']")
                        .val() 
                );
            }
        } else {
            config[_key] = $(this).val();
        }
    });
    // save config to form element
    $('input[name=multi_entry_render_calculated_value_config_' + currentN + ']').val(JSON.stringify(config));
    // Hide modal
    $('#modal_render_calculated_values_editor').modal('hide');
});

/**
    Tag aggregation settings
**/
$(document).on('change', '#custom_options_use_for_tag_aggregation', function(){
    if ($(this).val() == 'true') {
        $('.sres-tag-aggregation-related-setting').removeClass('d-none');
    } else {
        $('.sres-tag-aggregation-related-setting').addClass('d-none');
    }
});
$(document).ready(function(){
    $('#custom_options_use_for_tag_aggregation').trigger('change');
});

/**
    Apply to others
**/
$(document).on('change', '#apply_to_others_active, #permissions_edit_mode_student', function(){
    if ($('#permissions_edit_mode_student').val() != 'deny' && $(this).val() == 'true') {
        $('.sres-warning-apply-to-others-student-editing').removeClass('d-none');
    } else {
        $('.sres-warning-apply-to-others-student-editing').addClass('d-none');
    }
});
$(document).on('click', ".sres-warning-apply-to-others-student-editing a.sres-shortcut-hint", function(){
    $('#custom_options_student_data_entry_trigger_apply_to_others').parent().addClass('bg-danger');
    window.setTimeout(function(){
        $('#custom_options_student_data_entry_trigger_apply_to_others').parent().removeClass('bg-danger');
    }, 3000);
    $('#custom_options_student_data_entry_trigger_apply_to_others').get(0).scrollIntoView({behavior:'smooth'});
});
$(document).ready(function(){
    $('#apply_to_others_active').trigger('change');
});
