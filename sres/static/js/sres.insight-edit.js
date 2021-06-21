
// TinyMCE
$(document).ready(function(){
	var tinymceBasicToolbar = ['bold italic underline | strikethrough subscript superscript | removeformat | forecolor backcolor | bullist numlist | indent outdent | alignleft aligncenter alignright alignjustify', 'link unlink | image table hr charmap | cut copy paste pastetext | undo redo', 'styleselect fontselect fontsizeselect | code'];
	tinymce.init({
		selector: '.sres-tinymce-editor',
		toolbar: tinymceBasicToolbar,
		menubar: false,
		inline: true,
		plugins: 'code textcolor lists link image table hr charmap paste',
        convert_urls: false,
        relative_urls: false,
        remove_script_host: false
	})
});

// Chosens
$(document).ready(function(){
	$('#authorised_administrators').chosen({
		width: '100%', 
		no_results_text: 'Please use the button to add users', 
		placeholder_text_multiple: 'Please use the button to add users'
	});
	$('select[name=alert_teacher_allocation_columnuuid]').chosen({
		width: '50%'
	});
});

// Pick a column
$(document).on('click', 'span.sres-condition-column-placeholder', function(){
	var receiver = $(this).siblings('input:hidden.sres-condition-column-receiver');
	if (receiver.attr("name") == "alert_teacher_allocation_columnuuid") {
		show_column_chooser(receiver.attr('id'), '', null, null, null, receiver.attr('data-sres-tableuuid'), receiver.val(), true, "teacherallocation");
	} else {
		show_column_chooser(receiver.attr('id'), '', null, null, null, receiver.attr('data-sres-tableuuid'), receiver.val(), true);
	}
});

// Frequency config
$(document).on("change", "select#alert_interval", function(){
	switch ($(this).val()) {
		case "hours":
			$("#alert_frequency").removeClass("d-none");
			break;
		case "week":
		case "month":
			$("#alert_frequency").addClass("d-none");
			break;
	}
});
$(document).ready(function(){
	$("select#alert_interval").trigger("change");	
});

// Trigger config
$(document).on('change', 'select[name=trigger_type]', function() {
	$('div[id^=trigger_type_config_]').addClass('d-none');
	if ($(this).val() == '') return;
	switch ($(this).val()) {
		case 'custom_conditions':
			break;
		default:
			$('#trigger_type_config_select_columns').removeClass('d-none');
			$('#trigger_type_config_predefined').removeClass('d-none');
	}
	$('#trigger_type_config_' + $(this).val()).removeClass('d-none');
});
$(document).ready(function(){
	$('select[name=trigger_type]')
		.val($('select[name=trigger_type]').attr('data-sres-value'))
		.trigger('change');
});
// activate multi select for attributes
$(document).ready(function(){
	$("#trigger_config_select_columns_select").multiSelect({
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
		afterSelect: function(values) {
			opt = $('#trigger_config_select_columns_select option[value="' + values[0]  + '"]');
			var sel = $("input:hidden[name=trigger_config_select_columns]").val().split(",");
			sel = sel.length == 1 && sel[0] == '' ? [] : sel;
			if (sel.indexOf(values[0]) == -1) {
				sel.push(values[0]);
				$("input:hidden[name=trigger_config_select_columns]").val(sel.join(","));
			}
		},
		afterDeselect: function(values) {
			var sel = $("input:hidden[name=trigger_config_select_columns]").val().split(",");
			sel = sel.length == 1 && sel[0] == '' ? [] : sel;
			var ind = sel.indexOf(values[0]);
			if (ind > -1) {
				sel.splice(ind, 1);
			}
			$("input:hidden[name=trigger_config_select_columns]").val(sel.join(","));
		},
		selectableHeader: '<label>Available columns</label><input type="text" class="search-input form-control w-100" autocomplete="off" placeholder="Search">',
		selectionHeader: '<label>Selected columns</label><input type="text" class="invisible form-control">'
	});
});
$(document).ready(function(){
	// Select columns in order
	var sel = $("input:hidden[name=trigger_config_select_columns]").val().split(",");
	for (var i = 0; i < sel.length; i++) {
		if (sel[i] != '') {
			$("#trigger_config_select_columns_select").multiSelect('select', sel[i]);
		}
	}
});

// Insert data field button
$(document).on('click', 'button.sres-editor-insert-data-field', function() {
	show_column_chooser('content_per_student', '$');
});

