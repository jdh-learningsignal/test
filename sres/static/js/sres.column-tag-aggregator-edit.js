var tinymceBasicOneLineToolbar = 'bold, italic, underline, strikethrough, styleselect, fontsizeselect | forecolor, backcolor | cut, copy, paste | undo, redo, removeformat | subscript, superscript | code';

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

