var tinymceBasicOneLineToolbar = ['bold, italic, underline, strikethrough, styleselect, fontselect, fontsizeselect | forecolor, backcolor', 'cut, copy, paste | undo, redo, removeformat | subscript, superscript | code'];

$(document).on('click', 'button[name=delete_button]', function(){
    $('#confirm_delete').modal('show');
});

/**
	Simple aggregator column selector
**/
$(document).ready(function() {
	$("#select_attributes").multiSelect({
        keepOrder: true,
		afterInit: function(ms) {
			var that = this,
				$selectableSearch = that.$selectableUl.prev(),
				selectableSearchString = '#'+that.$container.attr('id')+' .ms-elem-selectable:not(.ms-selected)';
			that.qs1 = $selectableSearch.quicksearch(selectableSearchString)
				.on('keydown', function(e){
				  if (e.which === 40){
					that.$selectableUl.focus();
					return false;
				  }
			});
		},
		afterSelect: function(value) {
			this.qs1.cache();
		},
		afterDeselect: function(value) {
			this.qs1.cache();
		},
		selectableHeader: 'Available columns (click to select)<br /><input type="text" class="search-input form-control w-100" autocomplete="off" placeholder="Search" size="30">',
		selectionHeader: '<div>Selected columns<br /><input type="text" class="form-control invisible"></div>'
	});
	/*if ($('#select_attributes').val() != null && $('#select_attributes').val().length != $('#select_columns_crosslist_warning').attr('data-sres-expected-column-count')) {
		$('#select_columns_crosslist_warning').removeClass('d-none');
	}*/
});
// Column selector - this is needed to keep the selections in order - put back attribute selections
$(document).ready(function(){
	// Select columns in order
	var sel = $("input:hidden[name=select_attributes_columns_ordered]").val();
    try {
        sel = JSON.parse(sel); // This should return an array under the new way of storing attributes.
        if (!Array.isArray(sel)) {
            sel = sel.split(","); // For legacy storage format.
        }
    } catch(e) {
        sel = sel.split(",");
    }
    //console.log(sel);
	for (var i = 0; i < sel.length; i++) {
		if (sel[i] != '') {
			$("#select_attributes").multiSelect('select', sel[i]);
		}
	}
});

/**
	Show/hide various settings
**/
$(document).on('click', "input:radio[name=aggregator_type]", function(){
	$("div.sres-aggregator-parameters").addClass('d-none');
	$(this).parents('.alert').find("div.sres-aggregator-parameters").removeClass('d-none');
});
// Trigger a few cosmetic things for page load.
$(document).ready(function(){
	$("input:radio[name=aggregator_type]:checked").trigger('click');
	$("select[name=recalculate_trigger]").trigger('change');
	$("#aggregator_type_mathematical_operations_formula").trigger('change');
});

/**
    t-axis aggregator settings
**/
$(document).on('change', 'input:checkbox[name=aggregation_axis_t]', function(){
    if ($(this).prop('checked')) {
        $(".sres-aggregator-axis-t-config").collapse('show');
    } else {
        $(".sres-aggregator-axis-t-config").collapse('hide');
    }
    $("select[name=aggregation_axis_t_source_limit]").trigger('change');
});
$(document).on('change', "select[name=aggregation_axis_t_source_limit]", function(){
    if ($(this).val() == 'no') {
        $(".sres-aggregator-axis-t-source-limit-config").collapse('hide');
    } else {
        $(".sres-aggregator-axis-t-source-limit-config").collapse('show');
    }
});
$(document).ready(function(){
    $('input:checkbox[name=aggregation_axis_t]').trigger('change');
});

/**
    Mathematical operations
**/
$(document).on('click', "#aggregator_mathematical_operations_button_column", function() {
	show_column_chooser('aggregator_type_mathematical_operations_formula', '$', null, false, true, ENV['table_uuid'], null, true);
});
$(document).on('click', "button[id^=aggregator_mathematical_operations_button_operator_]", function() {
    tinymce.editors['aggregator_type_mathematical_operations_formula'].insertContent($(this).text());
});
$(document).on('click', '#aggregator_mathematical_operations_button_validate', function(){
    $.notify({message: 'Checking formula...'},{type: 'info'});
    $.ajax({
        url: ENV['CHECK_FORMULA_EXPRESSION_ENDPOINT'],
        method: 'POST',
        data: {
            body: tinymce.editors['aggregator_type_mathematical_operations_formula'].getContent()
        },
        success: function(data) {
            data = JSON.parse(data);
            if (data == true) {
                $.notify({message: 'Formula is mathematically valid.'},{type: 'success'});
            } else {
                $.notify({message: 'Formula is invalid.'},{type: 'danger'});
            }
        }
    });
});

/**
	Case builder
**/
// Cases
var cases = [];
function updateStringified() {
	$("input:hidden[name=aggregator_type_case_builder_cases]").val(JSON.stringify(cases));
    $(".sres-querybuilder-container[id^=builder-basic-case-x][id!=builder-basic-case-x]").each(function(){
        try {
            //console.log('updating rules?!?!', $(this).queryBuilder('getRules'));
            $hiddenField = $(this).siblings('input:hidden[name=rules-for-' + $(this).attr('id') + ']');
            $hiddenField.val(JSON.stringify($(this).queryBuilder('getRules')));
        } catch(e) {
            console.error(e);
        }
    });
}
$(document).on('submit', '#edit_aggregator_form', function(){
    updateStringified();
});
// Adding a case
$(document).on('click', "button[id=add_case]", function(event, args) {
	var caseNumber = 0;
	if (cases.length > 0) {
		caseNumber = Math.max.apply(Math, cases) + 1;
	} else {
		caseNumber = 1;
	}
	//console.log('new case number ', caseNumber);
	$clone = $("#case_template").clone(true, true);
	cases.push(caseNumber);
	updateStringified();
	$clone
		.attr("id", "case-case-x" + caseNumber)
		.find("[id*=-case-x]").each(function() {
			var idAndName = $(this).attr("id").replace(/[-]x[0-9]*/, "-x" + caseNumber);
			$(this).attr("id", idAndName);
			if ($(this).attr("name")) {
				$(this).attr("name", idAndName);
			}
			$(this).siblings('label[for*=-case-x]').attr('for', idAndName);
		});
    if (args) {
        //$clone.find("textarea[id^=case_content-case-x]").val(args['content']);
        $clone.find("div[id^=case_content-case-x]").html(args['content']);
        if (args['default_case'] == '1') {
            $clone.find("input:checkbox[id^=case_conditions_default-case-x]").prop('checked', true);
        }
    }
	$clone
        .removeClass("d-none")
		.show()
		.appendTo("#cases_container");
    // Update the filters in opts
    queryBuilderOpts.filters = queryBuilderFilters;
    // Init the queryBuilder
    //console.log('args', args);
    let opts = $.extend(true, {}, queryBuilderOpts);
    if (args) {
        opts['rules'] = args['rules'];
    } 
    $("div[id=builder-basic-case-x" + caseNumber + "]").queryBuilder(opts);
    // Initialise tinymce
    let initConfig = {
        selector: 'div[id=case_content-case-x' + caseNumber + '].tinymce-basic-oneline',
        toolbar: tinymceBasicOneLineToolbar,
        menubar: false,
        inline: true,
        statusbar: true,
        plugins: 'code textcolor colorpicker paste',
        forced_root_block: '',
        relative_urls: false,
        remove_script_host: false,
        convert_urls: false
    };
    if (ENV['FONT_FORMATS']) {
        initConfig['font_formats'] = ENV['FONT_FORMATS'];
    }
    tinymce.init(initConfig);
});
// Deleting a case
$(document).on('click', "button[id^=case_delete-case-x]", function() {
	var caseNumber = parseInt(/(?:[-]x)([0-9]*)$/.exec($(this).attr("id"))[1]);
	$("#case-case-x" + caseNumber).remove();
	cases.splice(cases.indexOf(caseNumber), 1);
	updateStringified();
});
// Repopulate cases
$(document).ready(function(){
    caseBuilderCases.forEach(function(caseBuilderCase){
        //console.log('caseBuilderCase', caseBuilderCase);
        $("button[id=add_case]").trigger(
            'click',
            caseBuilderCase
        );
    });
});

/**
    Self and peer review
**/
$(document).ready(function(){
    $('#aggregator_type_self_peer_review_grouping_column').chosen({
        width: '100%',
        search_contains: true,
        placeholder_text_multiple: 'Select'
    });
    $('#aggregator_type_self_peer_review_score_column').chosen({
        width: '100%',
        search_contains: true,
        placeholder_text_multiple: 'Select'
    });
});

// Mapper aggregator
$(document).on('input', 'textarea[name=aggregator_type_mapper_inputs], textarea[name=aggregator_type_mapper_outputs]', function(){
    $('#aggregator_type_mapper_inputs_line_count').html($('textarea[name=aggregator_type_mapper_inputs]').val().split('\n').length);
    $('#aggregator_type_mapper_outputs_line_count').html($('textarea[name=aggregator_type_mapper_outputs]').val().split('\n').length);
});
$(document).ready(function(){
    $('textarea[name=aggregator_type_mapper_inputs]').on('scroll', function(){
        $('textarea[name=aggregator_type_mapper_outputs]').get(0).scrollTop = $('textarea[name=aggregator_type_mapper_inputs]').get(0).scrollTop;
    });
    $('textarea[name=aggregator_type_mapper_outputs]').on('scroll', function(){
        $('textarea[name=aggregator_type_mapper_inputs]').get(0).scrollTop = $('textarea[name=aggregator_type_mapper_outputs]').get(0).scrollTop;
    });
    $('textarea[name=aggregator_type_mapper_inputs]').trigger('input');
});

/**
    Force recalculation
**/
$(document).on('click', '.sres-aggregator-recalculate-now', function(){
    sresRecalculateAggregatorAll(
        ENV['column_uuid'],
        ENV['GET_ALL_IDENTIFIERS_ENDPOINT'],
        ENV['AGGREGATOR_RECALCULATION_ENDPOINT'],
        undefined,
        undefined,
        $("#modal_progress")
    );
});
