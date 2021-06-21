$(document).ready(function(){
    $('#authorised_administrators').chosen({
        width: '100%', 
        no_results_text: 'Please use the button to add users', 
        placeholder_text_multiple: 'Please use the button to add users',
        disable_search: true
    });
});

// Pick a column
$(document).on('click', 'span.sres-condition-column-placeholder', function(){
	var receiver = $(this).siblings('input:hidden.sres-condition-column-receiver');
    show_column_chooser(receiver.attr('id'), '', null, null, null, null, receiver.val(), true);
});

// Tracking counter name
$(document).on('change', "#filter_name", function() {
    $("#email_tracking_column_name").val('Tracking counter for ' + $(this).val());
});
$(document).ready(function() {
    if (!ENV['filter_disabled']) {
        $("#filter_name").trigger('change');
    }
});

/**
    Primary conditions
**/
// Toggler
$(document).on('change', '#conditions_use_advanced', function(){
    if ($(this).prop('checked')) {
        $('#conditions_advanced_container').collapse('show');
        $('#conditions_simple_container').collapse('hide');
    } else {
        $('#conditions_simple_container').collapse('show');
        $('#conditions_advanced_container').collapse('hide');
    }
});
$(document).ready(function(){
    $('#conditions_use_advanced').trigger('change');
});
// Initialise conditions wizard queryBuilder
$(document).ready(function(){
    // Update the filters in opts
    queryBuilderOpts.filters = queryBuilderFilters;
    // Init the queryBuilder
    let opts = $.extend(true, {}, queryBuilderOpts);
    if (Object.keys(queryBuilderRules).length > 0) {
        opts['rules'] = queryBuilderRules;
    }
    $("#primary_conditions").queryBuilder(opts)
        .on('rulesChanged.queryBuilder', function(a, b){
            // live checking of conditions
            if (ENV['allow_conditions_preview']) {
                $.ajax({
                    url: ENV['PREVIEW_CONDITIONS_ENDPOINT'],
                    method: 'POST',
                    data: {
                        'conditions': JSON.stringify($(this).queryBuilder('getRules'))
                    },
                    success: function(data){
                        try {
                            data = JSON.parse(data);
                            let records = Object.keys(data.data).length;
                            if (records > 0) {
                                $('#conditions_warning').addClass('d-none');
                                $.notify(
                                    { message: 'These primary conditions will pick up ' + records + ' students.'},
                                    { type: 'info' }
                                );
                            } else {
                                let warningText = 'These primary conditions are not picking up any students. You may want to double-check the primary conditions.';
                                $('#conditions_warning').removeClass('d-none').find('#conditions_warning_text').text(warningText);
                                $.notify(
                                    { message: warningText },
                                    { type: 'danger' }
                                );
                            }
                        } catch(e) {
                            let warningText = 'There appears to be a problem with the primary conditions. Please double-check the conditions are configured and that the comparisons suit the data being analysed.';
                            $('#conditions_warning').removeClass('d-none').find('#conditions_warning_text').text(warningText);
                            $.notify(
                                { message: warningText },
                                { type: 'danger' }
                            );
                        }
                    }
                });
            }
        });
    $('#primary_conditions').trigger('rulesChanged.queryBuilder');
});
// Detect when primary conditions are modified
$(document).on('change click', '#primary_conditions input, #primary_conditions select', function(){
    ENV['allow_conditions_preview'] = true;
});
// Advanced conditions initialise tinymce editor
$(document).ready(function(){
    tinymce.init({
        selector: '.tinymce-code-only',
        toolbar: false,
        menubar: false,
        inline: true,
        statusbar: false,
        readonly: ENV['filter_disabled'] == "disabled" ? true : false,
        content_style: "p {font-family: Monospace;}"
    });
});
// Advanced conditions insert column
$(document).on('click', '#conditions_advanced_expression_button_column', function(){
    show_column_chooser('conditions_advanced_expression', '$', null, false, true, null, null, true);
});
// Advanced conditions insert operators
$(document).on('click', "button[id^=conditions_advanced_expression_button_operator_]", function() {
    tinymce.editors['conditions_advanced_expression'].insertContent($(this).text());
});
// Advanced conditions validate expression
$(document).on('click', '#conditions_advanced_expression_button_validate', function(){
    $.notify({message: 'Checking expression...'},{type: 'info'});
    $.ajax({
        url: ENV['CHECK_ADVANCED_EXPRESSION_ENDPOINT'],
        method: 'POST',
        data: {
            body: tinymce.editors['conditions_advanced_expression'].getContent()
        },
        success: function(data) {
            data = JSON.parse(data);
            if (data == true) {
                $.notify({message: 'Expression appears valid.'},{type: 'success'});
            } else {
                $.notify({message: 'Expression is invalid.'},{type: 'danger'});
            }
        }
    });
});

/**
    Contact type
**/
$(document).on('change', 'input:checkbox[name^=contact_type_]', function(){
    // Adjust visibility of major containers
    let containersEnabled = [];
    if ( $('input:checkbox[name=contact_type_email]').prop('checked') || $('input:checkbox[name=contact_type_canvasinbox]').prop('checked') ) {
        containersEnabled.push('message');
    } 
    if ( $('input:checkbox[name=contact_type_sms]').prop('checked') ) {
        containersEnabled.push('sms');
    }
    ['message', 'sms'].forEach(function(containerType) {
        if (containersEnabled.includes(containerType)) {
            $('.sres-contact-type-configuration-container-' + containerType)
                .collapse('show');
        } else {
            $('.sres-contact-type-configuration-container-' + containerType)
                .collapse('hide');
        }
    });
    // Adjust visibility of minor config fields
    let contactTypesEnabled = [];
    ['email', 'canvasinbox'].forEach(function(contactType){
        if ($('input:checkbox[name=contact_type_' + contactType + ']').prop('checked')) {
            contactTypesEnabled.push(contactType);
        }
    });
    $('.sres-contact-type-config').addClass('d-none');
    contactTypesEnabled.forEach(function(contactType){
        $('.sres-contact-type-' + contactType).removeClass('d-none');
    });
    // Adjust required fields
    $('[data-sres-required-for]').each(function(){
        $(this).prop('required', false);
        let requiredFor = $(this).attr('data-sres-required-for');
        if (containersEnabled.includes(requiredFor) || contactTypesEnabled.includes(requiredFor)) {
            $(this).prop('required', true);
        }
    });
});
$(document).ready(function(){
    $('input:checkbox[name^=contact_type_]').trigger('change');
});

// Insert data field trigger
$(document).on('click', '.sres-select-column-trigger', function(){
    let target = $(this).attr('data-sres-insert-column-target');
    if (typeof target == 'undefined') {
        target = $(this).siblings('.sres-tinymce-editor').attr('id');
    }
    show_column_chooser(target, '$');
});

/**
    SMS config
**/
$(document).on("change", "#sms_target", function(){
    if ($(this).val() == "custom") {
        $("#sms_target_custom_chooser").removeClass("d-none");
    } else {
        $("#sms_target_custom_chooser").addClass("d-none");
    }
});
$(document).ready(function(){
    $("#sms_target").trigger("change");
});

/**
    Email addresses
**/
$(document).on('keyup', '#to_email', function(){
    if ($(this).val().trim()) {
        $('#recipient_email_override_active_alert').removeClass('d-none');
    } else {
        $('#recipient_email_override_active_alert').addClass('d-none');
    }
});
$(document).ready(function(){
    $('#to_email').trigger('keyup');
});

/**
    Email sections
**/
var tinymceBasicToolbar = ['bold italic underline | strikethrough subscript superscript | removeformat | forecolor backcolor | bullist numlist | indent outdent | alignleft aligncenter alignright alignjustify', 'link unlink | image table hr charmap | cut copy paste pastetext | undo redo', 'styleselect fontselect fontsizeselect | code searchreplace'];
function initialiseEditor(selector, disabled) {
    var elementSelector = '';
    if (typeof selector == 'undefined') {
        // Initialise all
        elementSelector = '.tinymce-basic';
    } else {
        elementSelector = selector;
    }
    //console.log('initialising...', elementSelector);
    let initConfig = {
        selector: elementSelector,
        toolbar: tinymceBasicToolbar,
        menubar: false,
        inline: true,
        plugins: 'code textcolor colorpicker lists link image table hr charmap paste searchreplace',
        min_height: 80,
        images_upload_url: ENV['FILE_UPLOAD_ENDPOINT'],
        images_upload_base_path: '',
        convert_urls: false,
        relative_urls: false,
        remove_script_host: false,
        images_upload_credentials: true,
        readonly: disabled == "disabled" ? true : false,
        content_style: "p {font-family: Arial, Helvetica, sans-serif;}"
    };
    if (ENV['FONT_FORMATS']) {
        initConfig['font_formats'] = ENV['FONT_FORMATS'];
    }
    tinymce.init(initConfig)
}
$(document).ready(function(){
    initialiseEditor('div[id=email_body_first]', ENV['filter_disabled']);
	initialiseEditor('div[id=email_body_last]', ENV['filter_disabled']);
});
var sections = [];
function updateStringified() {
    $("input:hidden[name=sections]").val(JSON.stringify(sections));
    $(".sres-querybuilder-container").each(function(){
        if ($(this).parents('#section_template').length == 1) {
            // ignore
        } else {
            try {
                //console.log('updating rules?!?!', $(this).queryBuilder('getRules'));
                let $hiddenField = $(this).siblings('input:hidden[name=rules_for_' + $(this).attr('id') + ']');
                $hiddenField.val(JSON.stringify($(this).queryBuilder('getRules')));
            } catch(e) {
                console.log(e);
            }
        }
    });
}
$(document).on('submit', 'form#edit_filter_form', function(event){
    updateStringified();
    // check primary conditions
    if (!$('#conditions_use_advanced').prop('checked')) {
        // using simple conditions
        if (!$('#rules_for_primary_conditions').val() || $('#rules_for_primary_conditions').val() == 'null') {
            $.notify({message:'Primary conditions appear to be missing. At least one primary condition must be set to focus this filter.'}, {type: 'danger'});
            event.preventDefault();
            return false;
        }
    }
});
// Adding a section in the middle somewhere
function addSectionBeforePosition(insertBeforeSectionNumber, args) {
    $('button[id=add_section]').trigger('click', args);
    if (insertBeforeSectionNumber == -1) {
        // no need to shift; add at the end
    } else {
        let lastSectionNumber = sections[sections.length - 1];
        let position = sections.indexOf(insertBeforeSectionNumber);
        for (let n = 0; n < sections.length - position - 1; n++) {
            $('#section_raise_section_x' + lastSectionNumber).trigger('click');
        }
    }
}
$(document).on('click', 'button.sres-intersection-hover-add', function(event, args){
    let insertBeforeSectionNumber = parseInt($(this).attr('data-sres-position'));
    addSectionBeforePosition(insertBeforeSectionNumber, args);
});
// Adding a section at the bottom
function filterAddSection(args) {
    var sectionNumber = 0;
    if (sections.length > 0) {
        sectionNumber = Math.max.apply(Math, sections) + 1;
    } else {
        sectionNumber = 1;
    }
    //console.log('new section number ', sectionNumber, args);
    $clone = $("#section_template").clone(true, true);
    sections.push(sectionNumber);
    $clone
        .attr("id", "section_section_x" + sectionNumber)
        .find("[id*=_section_x]").each(function() {
            var idAndName = $(this).attr("id").replace(/[_]x[0-9]*/, "_x" + sectionNumber);
            //console.log(idAndName, sectionNumber);
            $(this).attr("id", idAndName);
            if ($(this).attr("name")) {
                $(this).attr("name", idAndName);
            }
        });
    $clone
        .removeClass("d-none")
        .show()
        .appendTo("#section_container");
    if (args) {
        // Set the section content before initialising tinymce
        // For some reason setContent doesn't work?!?!
        $('div#email_body_section_section_x' + sectionNumber).html(args['content']);
    }
    // Initialise tinymce
    $("#section_section_x" + sectionNumber)
        .find("div[class~=tinymce-basic]").each(function() {
            //console.log('initialising editor', $(this));
            initialiseEditor('div#email_body_section_section_x' + sectionNumber, false);										
        });
    if (args) {
        //console.log(sectionNumber, args['content']);
        $('[id=section_showwhen_section_x' + sectionNumber + ']')
            .val(args['show_when'])
            .trigger('click');
    }
    // Init the queryBuilder
    queryBuilderOpts.filters = queryBuilderFilters;
    let opts = $.extend(true, {}, queryBuilderOpts);
    if (args) {
        //console.log('xx', args['conditions']);
        opts['rules'] = args['conditions'];
    }
    $("div[id=section_conditions_section_x" + sectionNumber + "]").queryBuilder(opts);
    //console.log('finishing up adding section', sectionNumber);
    //updateStringified();
    refreshTooltips();
    return false;
}
$(document).on('click', "button[id=add_section]", function(event, args) {
    filterAddSection(args);
});
// Section interactivity
$(document).on('click', "select[id^=section_showwhen_section_x]", function() {
    var sectionNumber = parseInt(/(?:[_]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    if ($(this).val() == 'conditions') {
        $("#section_conditions_section_x" + sectionNumber).collapse('show');
        $('#email_body_section_section_x' + sectionNumber).parent().addClass('col-sm-6').removeClass('col');
    } else {
        $("#section_conditions_section_x" + sectionNumber).collapse('hide');
        $('#email_body_section_section_x' + sectionNumber).parent().addClass('col').removeClass('col-sm-6');
    }
});
$(document).on('click', "button[id^=section_lower_section_x],button[id^=section_raise_section_x]", function() {
    let sectionNumber = parseInt(/(?:[_]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    let direction = '';
    let checkIndexExtreme = 0;
    if ($(this).attr('id').indexOf('raise') > 0) {
        direction = 'raise';
        checkIndexExtreme = 0;
    } else {
        direction = 'lower';
        checkIndexExtreme = sections.length - 1;
    }
    if (sections[checkIndexExtreme] == sectionNumber) {
        console.log('cannot ' + direction + ' any more');
    } else {
        let sectionPosition = sections.indexOf(sectionNumber);
        if (direction == 'raise') {
            $("#section_section_x" + sectionNumber).insertBefore("#section_section_x" + sections[sectionPosition - 1]);
            sections.move(sectionPosition, sectionPosition - 1);
        } else if (direction == 'lower') {
            $("#section_section_x" + sectionNumber).insertAfter("#section_section_x" + sections[sectionPosition + 1]);
            sections.move(sectionPosition, sectionPosition + 1);
        }
    }
    updateStringified();
    return false;
});
// Deleting a section
$(document).on('click', "button[id^=section_delete_section_x]", function() {
    var sectionNumber = parseInt(/(?:[_]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    $(this).parents('#section_section_x' + sectionNumber).remove();
    sections.splice(sections.indexOf(sectionNumber), 1);
    tinymce.editors["email_body_section_section_x" + sectionNumber].remove();
    updateStringified();
    return false;
});
// Cloning a section
function collectSectionConfig(sectionNumber){
    return {
        conditions: $("div[id=section_conditions_section_x" + sectionNumber + "]").queryBuilder('getRules'),
        content: $('div#email_body_section_section_x' + sectionNumber).html(),
        show_when: $('[id=section_showwhen_section_x' + sectionNumber + ']').val()
    }
}
$(document).on('click', "button[id^=section_clone_section_x]", function() {
    let sourceSectionNumber = parseInt(/(?:[_]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    let sourceSectionConfig = collectSectionConfig(sourceSectionNumber);
    if (sections.indexOf(sourceSectionNumber) == sections.length - 1) {
        // add at the end
        addSectionBeforePosition(-1, sourceSectionConfig);
    } else {
        addSectionBeforePosition(sections[sections.indexOf(sourceSectionNumber) + 1], sourceSectionConfig);
    }
    updateStringified();
});
// Repopulate existing sections
$(document).ready(function(){
    existingEmailSections.forEach(function(existingSection){
        //console.log('repopulating section', existingSection);
        filterAddSection(existingSection);
    });
});
// Hover to add
$(document).on('mouseenter', '.sres-section-parent', function(event){
    var sectionNumber = parseInt(/(?:[_]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
    $(this).prepend('<div class="sres-intersection-hover-controls"><button type="button" class="btn btn-sm btn-primary sres-intersection-hover-add" data-sres-position="' + sectionNumber + '"><span class="fa fa-plus"></span> Add section</button></div>');
});
$(document).on('mouseleave', '.sres-section-parent', function(event){
    $(this).find('.sres-intersection-hover-controls').remove();
});


