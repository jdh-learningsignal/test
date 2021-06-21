var oTable;
var targetedCells = [];

// Generic utility
function getTableUUID() {
	return $("#main_table").attr('data-sres-tableuuid');
}
function getColumnUUID(td) {
	return $(oTable.column(td).header()).attr('data-sres-columnuuid');
}
function getColumnName(td) {
	return $(oTable.column(td).header()).attr('data-name');
}
function getSID(td) {
	return $(oTable.row(td).node()).attr('data-sres-sid');
}
function getFilteredIdentifiers() {
	var filteredRows = oTable.rows({search:'applied'}).nodes();
	var identifier_list = [];
	for ( i = 0 ; i < filteredRows.length ; i++ ) {
		identifier_list[i] = $(filteredRows[i]).attr('data-sres-sid');
	}
	return identifier_list;
}
function getViewInfo() {
	return {
		viewuuid: $('span.sres-view-info').attr('data-sres-view-uuid'),
		name: $('span.sres-view-info').attr('data-sres-view-name'),
		description: $('span.sres-view-info').attr('data-sres-view-description'),
		role: $('span.sres-view-info').attr('data-sres-view-role')
	}
}
function getSystemColumnCount() {
	return $("#main_table th.sres-systemcolumn-data").length;
}
function getCustomColumnCount() {
	return $("#main_table th.sres-column-data").length;
}
function getSystemColumnTargets() {
	return [...Array(getSystemColumnCount() + 1).keys()].slice(1, getSystemColumnCount() + 1);
}
function getCustomColumnTargets() {
	return [...Array(getSystemColumnCount() + getCustomColumnCount() + 1).keys()].slice(getSystemColumnCount() + 1, getSystemColumnCount() + getCustomColumnCount() + 1);
}
function getFrozenColumnsCount() {
	return parseInt($("#main_table").attr('data-sres-frozencolumns'));
}
function getSavedPageLength() {
	return parseInt($("#main_table").attr('data-sres-pagelength'));
}

// Sortable
$(document).ready(function(){
	$('#column_display_sortable, #system_column_display_sortable').sortable({
		handle: 'span.sres-sortable-handle',
		serialize: function($parent, $children, parentIsContainer) {
			if (parentIsContainer) {
				var ret = [];
				$parent.find('li.sres-sortable-item').each(function(){
					ret.push({
						column: $(this).attr('data-column'),
						checked: $(this).find('input:checkbox.sres-edit-column-visibility').prop('checked')
					});
				});
				return ret;
			}
		}
	});
});

// DataTable
$(document).ready(function(){
	// Set up column filtering/searching
	$('#main_table thead th.sres-searchable-column').each(function(){
		$(this).append( '<div><input type="text" placeholder="Search" aria-label="Search column" onclick="event.stopPropagation();" class="form-control sres-column-search"/></div>' );
	});
	// Build the datatable
	oTable = $("#main_table").DataTable({
		processing: true,
		serverSide: true,
		ajax: {
			url: ENV['SRES_LOAD_DATA_URL'],
			contentType: "application/json",
			method: "POST",
			data: function(d){
				d.tableuuid = getTableUUID();
				d.viewuuid = getViewInfo().viewuuid;
				let params = new URLSearchParams(window.location.search);
                d.show_inactive = params.get('showInactive') == '1' ? '1' : '';
				d.show_inactive_only = params.get('showInactive') == 'only' ? '1' : '';
				return JSON.stringify(d);
			},
			error: function(data) {
				alert('Sorry, an error occurred. ' + data.statusText);
			}
		},
		stateSave: true,
		stateSaveCallback: function(settings, data) {
			// pass
		},
		stateSaveParams: function(settings, data) {
			//console.log(data);
			for (var i = 0; i < data.columns.length; i++) {
				data.columns[i].name = $(oTable.column(i).header()).attr('data-name');
				data.columns[i].type = $(oTable.column(i).header()).hasClass('sres-systemcolumn-data') ? 'system' : 'user';
			}
			data.pageInfo = oTable.page.info();
			data.showInactive = $('#show_inactive').prop('checked');
			data.showInactiveOnly = $('#show_inactive_only').prop('checked');
		},
		order: [[2, 'asc']],
		dom: "<'row'<'col sres-datatables-column-jumper-container form-inline'><'col'f>><'row'<'col sres-datatables-length-menu'l><'col'i><'col'p>><'row'<'col'tr>>",
		/*fixedHeader: {
			'headerOffset': 0
		},*/
		scrollX: true,
		//scrollY: '50vh',
		scrollCollapse: true,
		fixedColumns: {
			leftColumns: getFrozenColumnsCount() + 1, /* + 1 because 0th column are for actions */
			heightMatch: 'auto'
		},
		pageLength: getSavedPageLength(),
		pagingType: "full_numbers",
		lengthMenu: [ [10, 25, 50, 100, 200, -1], [10, 25, 50, 100, 200, "All"] ],
		language: {
			processing: '<span class="fa fa-sync-alt spinning" aria-hidden="true"></span> Loading...',
            lengthMenu: "Show _MENU_ students",
            info: "Showing _START_ to _END_ of _TOTAL_ students",
            search: "Search data:"
		},
		autoWidth: false,
		columnDefs: [
			{
				targets: [0], 
				defaultContent: '<a href="#" class="sres-get-identifier" aria-label="Get identifier"><span class="fa fa-qrcode" aria-hidden="true" title="Get identifier"></span></a> ' + 
					'<a href="#" class="sres-view-person-info" aria-label="View/edit student info"><span class="fa fa-pencil-alt" aria-hidden="true" title="View/edit student info"></span></a> ' + 
					'<a href="#" class="sres-toggle-student-status" aria-label="Toggle active/inactive"><span class="fa fa-user" aria-hidden="true" title="Toggle active/inactive"></span></a>', 
				data: null, 
				orderable: false, 
				searchable: false,
				width: 35,
				className: 'sres-actioncolumn'
			},
			{
				targets: 'header_session',
				orderable: false,
				className: 'sres-editable-td sres-systemcolumn-data'
			},
			{
				targets: 'sres-systemcolumn-data',
				width: 80,
				className: 'sres-systemcolumn-data'
			},
			{
				targets: 'sres-column-data',
				width: 135,
				className: 'sres-editable-td sres-column-data'
			}
		]
	}).on('xhr.dt', function(e, settings, json, xhr){
		//console.log(e, settings, json, xhr);
		
		
		
	}).on('draw.dt', function(e, settings) {
		if (!ENV['SRES_IS_LIST_ADMINISTRATOR']) {
			oTable.buttons().container().insertBefore('#main_table_length');
		}
		// X-editable
		$("table.sres-main-table tbody tr td").each(function(){
			if ($(oTable.column($(this)).header()).hasClass('sres-x-editable') && $(this).find('span[data-sres-restricted-by-username]').length == 0) {
                var $td = $(this);
                $(this).editable({
					ajaxOptions: {
						type: 'POST'
					},
					type: 'text',
					emptytext: '&nbsp;',
					pk: 1,
					mode: 'inline',
					params: function(params) {
						//console.log($(this));
						params.table_uuid = getTableUUID();
						params.column_uuid = getColumnUUID($(this));
						params.data = params.value;
						params.identifier = getSID($(this));
						console.log(params);
						return params;
					}, 
					url: ENV['SET_DATA_ENDPOINT'].replace('__column_uuid__', getColumnUUID($td)),
					title: 'Edit data',
					inputclass: 'sres-editable-inputclass',
					success: function(response, newValue) {
						oTable.cell($(this)).data(newValue);
					},
					error: function(err, newValue) {
                        let errorMessage = 'Unexpected error';
                        switch (err.status) {
                            case 429:
                                errorMessage = 'Too many requests to save data within a certain amount of time than allowed. Please try again later.';
                                break;
                            default:
                                errorMessage = JSON.parse(err.responseText);
                                break;
                        }
                        $.notify({ message: errorMessage }, { type:'danger' });
                        return;
					}
				});
			} else if ( $(oTable.column($(this)).header()).hasClass('sres-column-data') ) {
				$(this).addClass('sres-pop-editable');
			} else if ($(oTable.column($(this)).header()).attr('data-name') == 'session') {
				$(this).addClass('sres-session-editable');
			};
		});
		// Studentstatus toggle
		$("table.sres-main-table tbody tr td a.sres-toggle-student-status").each(function(){
			var studentStatus = $(this).parents('tr').attr('data-sres-student-status');
			$(this).find('span').removeClass('fa-user-slash fa-user').addClass( studentStatus == 'active' ? 'fa-user-slash' : 'fa-user' );
		});
		// Cell target highlighting
		drawTargetHighlights();
		// Cell text truncation
		updateTextTruncationDisplay();
		// Recalculate VH
		adjustMainTableVH();
	}).on('mouseenter', 'td', function(ev){
		// Draw the onhover action icons
		if ($(this).find('span[data-sres-restricted-by-username]').length == 0) {
			if ($(oTable.column($(this)).header()).hasClass('sres-column-data')) {
				$(this).append('<span class="float-right sres-cell-actions"><a href="#" class="sres-cell-actions-history" role="button" title="View change history"><span class="fa fa-history" aria-hidden="true"></span></a></span>');
			}
			if ($(oTable.column($(this)).header()).hasClass('sres-editable-td')) {
				$(this).append('<span class="float-right sres-cell-actions"><a href="#" class="sres-cell-actions-edit" role="button" title="Click cell to edit"><span class="fa fa-pencil-alt" aria-hidden="true"></span></a></span>');
			}
			if (!$(oTable.column($(this)).header()).hasClass('header_actions')) {
				$(this).append('<span class="float-right sres-cell-actions"><a href="#" class="sres-cell-actions-highlight" role="button" title="Highlight"><span class="fa fa-bullseye" aria-hidden="true"></span></a></span>');
			}
		}
	}).on('mouseleave', 'td', function(){
		$(this).find('span.sres-cell-actions').remove();
	}).on('click', 'a.sres-cell-actions-highlight', function(ev){
		$(this).closest('td').editable('hide');
		ev.preventDefault();
		ev.cancelBubble = true;
		ev.stopImmediatePropagation();
		ev.stopPropagation();
		var targetTd = $(this).parents('td');
		var identifier = getSID(targetTd);
		var columnName = getColumnName(targetTd);
		var alreadyTargeted = -1;
		for (var i = 0; i < targetedCells.length; i++) {
			if (targetedCells[i]['identifier'] == identifier && targetedCells[i]['column'] == columnName) {
				alreadyTargeted = i;
				break;
			}
		}
		if (alreadyTargeted != -1) {
			targetedCells.splice(alreadyTargeted, 1);
		} else {
			targetedCells.push({
				identifier: identifier,
				column: columnName,
				td: targetTd
			});
		}
		drawTargetHighlights();
		oTable.draw('page');
		return false;
	});
	// Buttons only for authorised list users (don't show for list administrators since they can export otherwise)
	if (!ENV['SRES_IS_LIST_ADMINISTRATOR']) {
		new $.fn.dataTable.Buttons( oTable, {
			buttons: [
				{
					extend: 'csvHtml5',
					text: 'Export CSV',
					// Datatables includes the HTML elements within a header when doing an export [eg: Active/Inactive, Scan-and-enter].
					// This produces very messy CSV output which needs to be cleaned up.
					// First we break down the csv file, building an array of each header element.
					// Then a regex removes all characters between a TAB and a quotation, which cleans the header leaving only the column name
					// The csv file is then reassebmled and served to the user
					customize: function (csv) {
						var csvRows = csv.split('\n');
						var csvCols = csvRows[0].split(',');
						for ( var i = 0; i < csvCols.length; i++) {
							csvCols[i] = csvCols[i].replace(/\x09.*$/,'"');
						}
						csvRows[0] = csvCols.join(',');
						return csvRows.join('\n');
					},
					// As datatables is exporting data on the client side, it can only export data on the current page. 
					// This will check how many pages are currently in the table, then display a warning to the user if more than 1.
					// The function then continues as normal
					action: function(e, dt, button, config) {
						var info = oTable.page.info();
						if (info.pages > 1) {
							alert( '**Warning**\nYou currently have multiple pages of data.\nThis button only exports data from the current page.\nIf you want to export all of the data, please select show "All" entries from the drop down menu');
						}
						$.fn.dataTable.ext.buttons.csvHtml5.action(e, dt, button, config);
					}
				},
			],
		});
	}
});
// Operationalise column filtering/searching
$(document).on('keyup change', 'input:text.sres-column-search', function(){
	var columnName = $(this).closest('th').attr('data-name');
	var column = oTable.column(columnName + ':name');
	if (column.search() !== $(this).val()) {
		column.search($(this).val()).draw();
	}
});
// Special tooltips
$(document).on('mouseenter', 'table.sres-main-table tbody tr td img.sres-td-image, table.sres-main-table tbody tr td img.sres-td-imgurl', function(e) {
	/*$(this).tooltip({
        html: true,
        title: '<img style="padding:5px;" src="' + $(this).get(0).src + '" />'
    }).tooltip('show');*/
    var content = '<img class="p-2" src="' + $(this).get(0).src + '" alt="" />';
    $(this).attr('data-tippy-content', content);
    refreshTooltips();
});
// Datatable height (vh) adjustment
function adjustMainTableVH() {
	var scrollHeadBottom = $("div.dataTables_scrollHead")[0].getBoundingClientRect().bottom;
	var windowHeight = Math.max(document.documentElement.clientHeight, window.innerHeight || 0);
	var newHeight = windowHeight - scrollHeadBottom;
	$("div.dataTables_scrollBody").height(newHeight);
}
$(window).on('resize', function(){
	adjustMainTableVH();
});
// X-editable hack for frozen columns
$(document).on('click', 'div.DTFC_LeftBodyLiner table td.sres-editable-td.editable', function(e1, e2) {
	var el = $(this);
	setTimeout(
		function() {
			el.editable('show', true);
		},
		10
	);
});
$(document).on('hidden', 'td.sres-editable-td.editable', function(e1, e2) {
	if (e2 == 'save') {
		var cell = oTable.cell($(this));
		if (cell.index().column <= getFrozenColumnsCount()) {
			$('div.DTFC_LeftBodyLiner table td.sres-editable-td.editable[data-dt-row=' + cell.index().row + '][data-dt-column=' + cell.index().column + ']')
				.html(cell.data());
		}
	}
});

// Dropdown menus for table
var activeDropdownMenu = null;
var activeDropdownMenuParent = null;
$(document).on('click', function (e) {
	if (activeDropdownMenu) {
		//console.log('hiding now', activeDropdownMenu, activeDropdownMenuParent);
		activeDropdownMenuParent.append(activeDropdownMenu.detach());
		activeDropdownMenu.hide();
		activeDropdownMenu = null;
		activeDropdownMenuParent = null;
	}
});
$(document).on('show.bs.dropdown', 'div.dataTables_scrollHead table thead tr th, div.DTFC_LeftHeadWrapper table thead tr th', function(e) {
	if (activeDropdownMenu) {
		$(document).trigger('click');
	} else {
		activeDropdownMenuParent = $(e.target).find('button');
		activeDropdownMenu = $(e.target).find('div.dropdown-menu');
		//console.log('showing menu now', activeDropdownMenu, activeDropdownMenuParent);
		$('body').append(activeDropdownMenu.detach());
		var eOffset = activeDropdownMenuParent.offset();
		activeDropdownMenu.css({
			'display' : 'block',
			'top' : eOffset.top + activeDropdownMenuParent.outerHeight(),
			'left' : eOffset.left - activeDropdownMenu.outerWidth() + activeDropdownMenuParent.outerWidth(),
			'min-width' : '80px'/*,
			'position' : 'relative'*/
		});
	}
	return false;
});

/** 
    Bulk column operations
**/
function getCheckedColumnUuids(){
    let columnUuids = [];
    $('.sres-column-options-checkbox:checked').each(function(){
        columnUuids.push( $(this).parents('th[data-sres-columnuuid]').attr('data-sres-columnuuid') );
    });
    return columnUuids;
}
$(document).on('click', '.sres-selected-column-settings-select-toggle', function(){
    switch ($(this).attr('data-sres-select-action')) {
        case 'all':
            $(this).siblings("[data-sres-select-action='enable']").trigger('click');
            $('.sres-column-options-checkbox').prop('checked', true).first().trigger('input');
            break;
        case 'none':
            $(this).siblings("[data-sres-select-action='enable']").trigger('click');
            $('.sres-column-options-checkbox').prop('checked', false).first().trigger('input');
            break;
        case 'enable':
            $('.sres-column-options-container .sres-column-options-checkbox-container').removeClass('d-none');
            $('.sres-column-options-placeholder .sres-column-options-checkbox-container').removeClass('d-none');
            $('.sres-column-options-container .sres-column-options-button').addClass('rounded-right');
            break;
    }
});
$(document).on('input', '.sres-column-options-checkbox', function(){
    // are any checked?
    if (getCheckedColumnUuids().length > 0) {
        $('#settings_columns .sres-selected-column-settings').removeClass('d-none');
        $('#settings_columns').addClass('animated flash');
        setTimeout(function(){
            $('#settings_columns').removeClass('animated flash');
        }, 1000);
    } else {
        $('#settings_columns .sres-selected-column-settings').addClass('d-none');
    }
});
$(document).on('click', '.sres-selected-column-settings', function(){
    let numberSelected = getCheckedColumnUuids().length;
    switch ($(this).attr('data-sres-select-action')) {
        case 'delete':
            if (confirm("Are you sure you want to delete the " + numberSelected + " selected columns? This action cannot be undone.")) {
                $('.sres-column-options-checkbox:checked').each(function(){
                    let columnUuid = $(this).parents('th[data-sres-columnuuid]').attr('data-sres-columnuuid');
                    let columnName = $(this).parents('th[data-sres-columnuuid]').attr('data-sres-column-name');
                    $.ajaxq('sres_api_columns_delete', {
                        url: ENV['API_COLUMN_DELETE'].replace('__column_uuid__', columnUuid),
                        method: 'DELETE',
                        success: function(data) {
                            if (data.success) {
                                $.notify({message:'Column deleted: ' + data.name.replace('_', '_<wbr>') + '. Refresh this page to update.'}, {type: 'success'});
                                oTable.column(columnUuid + ':name').visible(false);
                            }
                        },
                        error: function(err) {
                            $.notify({message:'Error deleting column: ' + columnName.replace('_', '_<wbr>')}, {type: 'danger'});
                        }
                    });
                });
            }
            break;
        case 'export_data':
            // this is dealt with by the .sres-export-filtered click event method
            break;
    }
});

// Refresh list
$(document).on('click', 'a.sres-list-refresh', function(ev){
	oTable.draw(false);
});

// Clear all highlights
$(document).on('click', 'a.sres-list-highlights-clear', function(ev){
	targetedCells = [];
	drawTargetHighlights();
	oTable.draw(false);
});

// Clear all searches
$(document).on('click', 'a.sres-list-clear-all-searches', function(ev){
    $('#main_table_filter input[type=search]').val('');
	oTable.search('').draw(false);
    $('input.sres-column-search').val('').trigger('change');
});

// Toggle cell text wrapping
var textTruncated = true;
$(document).on('click', 'a.sres-list-toggle-cell-truncation', function(ev){
	textTruncated = !textTruncated;
	updateTextTruncationDisplay();
	oTable.draw('page');
});
function updateTextTruncationDisplay() {
	if (textTruncated) {
		$('#main_table tbody td:not(.sres-systemcolumn-data)').removeClass('sres-cell-not-truncated').addClass('sres-cell-truncated');
	} else {
		$('#main_table tbody td:not(.sres-systemcolumn-data)').addClass('sres-cell-not-truncated').removeClass('sres-cell-truncated');
	}
}

// Highlight targeted cells
function drawTargetHighlights() {
	// Remove highlight for all
	$('#main_table tbody td.sres-cell-highlight-target').each(function(){
		$(this).removeClass('sres-cell-highlight-target');
	});
	$(oTable.cells().nodes()).removeClass('bg-info');
	oTable.columns('.bg-info').header().to$().removeClass('bg-info');
	// Reapply highlight for still-targeted cells
	for (var i = 0; i < targetedCells.length; i++) {
		var rowVisible = $('#main_table tbody tr[data-sres-sid=' + targetedCells[i]['identifier'] + ']').length != 0;
		//var td = targetedCells[i]['td'];
		var td = null;
		$('#main_table tbody tr[data-sres-sid=' + targetedCells[i]['identifier'] + '] td').each(function(){
			if (td == null && $(oTable.column($(this)).header()).attr('data-name') == targetedCells[i]['column']) {
				td = $(this);
			}
		});
		$(oTable.column(targetedCells[i]['column'] + ':name').nodes()).addClass('bg-info');
		$(oTable.column(targetedCells[i]['column'] + ':name').header()).addClass('bg-info');
		if (rowVisible) {
			$(oTable.row(td).nodes()).find('td').addClass('bg-info');
			td.addClass('sres-cell-highlight-target');
			$(oTable.cell(td).node()).addClass('sres-cell-highlight-target');
		}
	}
};

// Get all identifiers based on oTable state
function getAllFilteredIdentifiers(callback){
    let state = JSON.parse(oTable.ajax.params());
    state.tableuuid = getTableUUID();
    state.viewuuid = getViewInfo().viewuuid;
    let params = new URLSearchParams(window.location.search);
    state.show_inactive = params.get('showInactive') == '1' ? '1' : '';
    state.show_inactive_only = params.get('showInactive') == 'only' ? '1' : '';
    $.ajax({
        url: ENV['GET_FILTERED_IDENTIFIERS_ENDPOINT'],
        contentType: "application/json",
        method: "POST",
        data: JSON.stringify(state),
        success: function(data){
            //console.log(data);
            callback(data);
        }
    })
}

// Create filter from searches
function poolConditionsFromSearches() {
	var conditions = {};
	var tableColumns = oTable.state().columns;
	for (var i = 0; i < tableColumns.length; i++) {
		if (tableColumns[i].search.search.length > 0) {
			var columnuuid = tableColumns[i].name.toUpperCase();
			var value = tableColumns[i].search.search;
			conditions[columnuuid] = [{
				columnuuid: columnuuid,
				value: value
			}];
		}
	}
	return conditions;
}
$(document).on('click', 'a.sres-filter-from-searches', function(ev){
    // TODO
	var selectedConditions = poolConditionsFromSearches();
	$.notify({message:'Creating filter...'}, {type: 'info'});
	$.ajax({
		url: 'VisDashboard.cfc?method=make_filter_remote',
		method: 'POST',
		data: {
			tableuuid: getTableUUID(),
			conditions_input: encodeURIComponent(JSON.stringify(selectedConditions)),
			description: 'Generated from list search',
			comparator_override: 'LIKE'
		},
		success: function(data) {
			data = JSON.parse(data);
			//console.log(data);
			if (data.success && data.filteruuid.length) {
				$.notify({message:'Filter created. Loading, please wait...'}, {type: 'success', delay:5000});
				window.location = 'editFilter.cfm?action=edit&filteruuid=' + data.filteruuid;
			} else {
				$.notify({message:'Error creating filter'}, {type: 'danger'});
			}
		}
	});
});

// td change history
$(document).on('click', 'a.sres-cell-actions-history', function(ev){
	var td = $(this).parents('td');
	$('#main_modal')
		.find('.modal-title').html('Change history').end()
		.find('.modal-body').html('<span class="fa fa-refresh spinning" aria-hidden="true"></span> Loading...').end()
		.modal('show');
	$.ajax({
		method: "GET",
		url: ENV['GET_CHANGE_HISTORY_ENDPOINT'].replace('__column_uuid__', getColumnUUID(td)).replace('__identifier__', getSID(td)),
		success: function(data) {
			data = JSON.parse(data);
			//console.log(data);
			var html = "";
			if (data.history.length == 0) {
				html = "No history recorded";
                $('#main_modal').find('.modal-body').html(html);
			} else {
                let tableTemplate = Handlebars.compile(document.getElementById("change_history_table_template").innerHTML);
                let tableHtml = tableTemplate({
                    columnUuid: getColumnUUID(td),
                    identifier: getSID(td),
                    showMultipleReports: data.multiple_reports_mode_enabled
                });
                $('#main_modal').find('.modal-body').html(tableHtml);
                let changeHistoryContainer = $('#main_modal').find('.modal-body table.sres-change-history-container tbody');
                let rowTemplate = Handlebars.compile(document.getElementById("change_history_row_template").innerHTML);
				for (var h = 0; h < data.history.length; h++) {
					changeHistoryContainer.append(rowTemplate({
                        timestamp: data.history[h].timestamp,
                        value: data.history[h].value,
                        authUser: data.history[h].auth_user,
                        realAuthUser: data.history[h].real_auth_user,
                        id: data.history[h].id,
                        showMultipleReports: data.multiple_reports_mode_enabled,
                        reportNumber: data.history[h].report_number
                    }));
				}
                
                /*
                html = '<div><table data-sres-changehistory-columnuuid="' + getColumnUUID(td) + '" data-sres-changehistory-identifier="' + getSID(td) + '" class="table sres-change-history-container" style="font-size:small;"><thead><tr><th>Timestamp</th><th>Value changed to</th><th>By user</th><th>&nbsp;</th></tr></thead><tbody>';
				for (var h = 0; h < data.history.length; h++) {
					html += '<tr>' + 
                        '<td>' + data.history[h].timestamp + '</td>' +
                        '<td><div class="sres-change-history-data-display">' + data.history[h].value + '</div></td>' +
                        '<td>' + data.history[h].auth_user + '</td>' +
                        '<td><a href="javascript:void(0);"><span class="fa fa-redo sres-changehistory-revert" aria-hidden="true" title="Revert to this value" data-sres-changehistory-id="' + data.history[h].id + '"></span></a></td>' +
                        '</tr>';
				}
				html += "</tbody></table></div>";
                */
                
                
			}
		},
		error: function(xhr) {
			//console.log(xhr);
			var message = JSON.parse(xhr.responseText).message;
			alert("Error requesting change history. " + message);
		}
	});
	ev.stopPropagation();
	ev.stopImmediatePropagation();
});
// change history reversion
$(document).on("click", "span.sres-changehistory-revert, span.sres-changehistory-revert-on-behalf", function(){
	if (!confirm("Are you sure you want to revert to this data from the change history?")) {
		return false;
	}
    let onBehalfOf = $(this).hasClass('sres-changehistory-revert-on-behalf') ? true : false;
	let id = $(this).attr("data-sres-changehistory-id");
	let columnuuid = $(this).parents("table").attr("data-sres-changehistory-columnuuid");
	let identifier = $(this).parents("table").attr("data-sres-changehistory-identifier");
	$.ajax({
		url: ENV['REVERT_CHANGE_HISTORY_ENDPOINT'],
		method: "POST",
        data: {
            column_uuid: columnuuid,
            id: id,
            identifier: identifier,
            on_behalf_of: onBehalfOf
        },
		success: function(data) {
			//data = JSON.parse(data);
			//console.log(data);
			$('#main_modal').modal("hide");
			oTable.draw(false);
		},
		error: function(xhr) {
			//console.log(xhr);
			var message = JSON.parse(xhr.responseText).message;
			alert("Error reverting change history. " + message);
		}
	});
});

// Show iframe data editor
$(document).on('click', 'table.sres-main-table tbody td.sres-pop-editable', function(e){
	if ($(this).find('span[data-sres-restricted-by-username]').length != 0) {
		return false;
	}
	var columnuuid = getColumnUUID($(this));
	var tableuuid = getTableUUID();
	var sid = getSID($(this));
	var td = $(this);
    var src = ENV['DATA_ENTRY_SINGLE_ENDPOINT'].replace('__column_uuid__', columnuuid).replace('__identifier__', sid);
	$('#main_modal')
		.find('.modal-title').html('Edit data').end()
		.find('.modal-body').html('<iframe src="' + src + '" width="99.6%" height="720" frameborder="0"></iframe>').end()
		.modal('show')
		.on('hidden.bs.modal', function(e){
			oTable.cell(td).invalidate().draw(false);
		});
});

// Show QR
$(document).on('click', 'a.sres-get-identifier', function(e){
	var tr = $(this).parents('tr');
	window.open(ENV['GET_ID_ENDPOINT'].replace('__identifier__', tr.attr('data-sres-sid')));
});

// Show person info
$(document).on('click', 'a.sres-view-person-info, a.sres-show-single-person-info', function(e){
	var tableuuid = getTableUUID();
	var identifier = '';
	if ($(this).hasClass('sres-view-person-info')) {
		identifier = getSID($(this).parents('td'));
	}
    if (identifier) {
        url = ENV['VIEW_SINGLE_STUDENT_ENDPOINT'].replace('__mode__', 'view').replace('__identifier__', encodeURIComponent(identifier));
    } else {
        url = ENV['VIEW_SINGLE_STUDENT_ENDPOINT'].replace('__mode__', 'identify');
    }
	var td = $(this);
	$('#main_modal')
		.find('.modal-title').html('Information').end()
		.find('.modal-body').html('<iframe src="' + url + '" width="99.6%" height="720" frameborder="0"></iframe>').end()
		.modal('show')
		.on('hidden.bs.modal', function(e){
			oTable.cell(td).invalidate().draw(false);
		});
});

// Toggle student active/inactive
$(document).on('click', 'a.sres-toggle-student-status', function(e){
	var tr = $(this).parents('tr')
	var studentStatus = tr.attr('data-sres-student-status');
	var newStudentStatus = studentStatus == 'active' ? 'inactive' : 'active';
	var sid = getSID($(this).parents('td'));
	var tableuuid = getTableUUID();
	var caller = $(this);
	$.ajax({
		url: ENV['SET_DATA_ENDPOINT'].replace('__column_uuid__', 'status'),
		method: 'POST',
		data: {
			'table_uuid': tableuuid,
			'column_uuid': 'status',
			'identifier': sid,
			'data': newStudentStatus
		},
		success: function(data){
			data = JSON.parse(data);
			if (data.success == true || data.success == 'true') {
				caller.find("span").removeClass("fa-user fa-user-slash");
				caller.parents('tr').attr('data-sres-student-status', data.data.saved);
				if (data.data.saved == 'active') {
					caller
						.find("span").addClass('fa-user-slash').attr('title', 'Make person inactive').end()
						.parents("tr").addClass("sres-row-active").removeClass("sres-row-inactive");
					$(oTable.row(caller.closest('tr')).nodes()).addClass("sres-row-active").removeClass("sres-row-inactive");
				} else {
					caller
						.find("span").addClass('fa-user').attr('title', 'Make person active').end()
						.parents("tr").addClass("sres-row-inactive").removeClass("sres-row-active");
					$(oTable.row(caller.closest('tr')).nodes()).addClass("sres-row-inactive").removeClass("sres-row-active");
				}
			}
		},
		error: function(data) {
			// TODO
		}
	});
});

// Add student
$(document).on('click', 'a.sres-add-student-manually', function(ev){
	var tableuuid = getTableUUID();
	$('#main_modal')
		.find('.modal-title').html('Information').end()
		.find('.modal-body').html('<iframe src="' + ENV['VIEW_SINGLE_STUDENT_ENDPOINT'].replace('__mode__', 'new') + '" width="99.6%" height="720" frameborder="0"></iframe>').end()
		.modal('show')
		.on('hidden.bs.modal', function(e){
			oTable.draw();
		});
});

// Make labels
$(document).on('click', 'a.sres-make-labels', function(ev){
	var newForm = document.createElement('FORM');
	newForm.method = 'POST';
	newForm.action = ENV['MAKE_DOC_ENDPOINT'];
	var csrfToken = document.createElement('INPUT');
	csrfToken.type = 'hidden';
	csrfToken.name = 'csrf_token';
	csrfToken.value = ENV['CSRF_TOKEN'];
	newForm.appendChild(csrfToken);
	var identifiers = document.createElement('INPUT');
	identifiers.type = 'hidden';
	identifiers.name = 'identifiers';
	identifiers.value = getFilteredIdentifiers().join(',');
	newForm.appendChild(identifiers);
	var w = window.open('', 'form-target');
	newForm.target = 'form-target';
	document.body.appendChild(newForm);
	newForm.submit();
});

// Download class list
$(document).on('click', 'a.sres-download-class-list', function(ev){
	var newForm = document.createElement('FORM');
	newForm.method = 'POST';
	newForm.action = ENV['EXPORT_CLASS_LIST_ENDPOINT'];
	var csrfToken = document.createElement('INPUT');
	csrfToken.type = 'hidden';
	csrfToken.name = 'csrf_token';
	csrfToken.value = ENV['CSRF_TOKEN'];
	newForm.appendChild(csrfToken);
	var identifiers = document.createElement('INPUT');
	identifiers.type = 'hidden';
	identifiers.name = 'identifier_list';
	identifiers.value = getFilteredIdentifiers().join(',');
	newForm.appendChild(identifiers);
	var w = window.open('', 'form-target');
	newForm.target = 'form-target';
	document.body.appendChild(newForm);
	newForm.submit();
});

// Export for all filtered students
$(document).on('click', 'a.sres-export-filtered', function(ev){
    let that = $(this);
    getAllFilteredIdentifiers(function(allFilteredIdentifiers){
        let newForm = document.createElement('FORM');
        newForm.method = 'POST';
        newForm.action = ENV['EXPORT_DATA_ENDPOINT'];
        let csrfToken = document.createElement('INPUT');
        csrfToken.type = 'hidden';
        csrfToken.name = 'csrf_token';
        csrfToken.value = ENV['CSRF_TOKEN'];
        newForm.appendChild(csrfToken);
        let identifiers = document.createElement('INPUT');
        identifiers.type = 'hidden';
        identifiers.name = 'identifiers';
        identifiers.value = JSON.stringify(allFilteredIdentifiers);
        newForm.appendChild(identifiers);
        //console.log(allFilteredIdentifiers);
        //console.log(JSON.stringify(allFilteredIdentifiers));
        if (typeof that.attr('data-sres-view-uuid') !== 'undefined') {
            let viewUuid = document.createElement('INPUT');
            viewUuid.type = 'hidden';
            viewUuid.name = 'view_uuid';
            viewUuid.value = that.attr('data-sres-view-uuid');
            newForm.appendChild(viewUuid);
        }
        if (typeof that.attr('data-sres-columnuuid') !== 'undefined') {
            let columnUuid = document.createElement('INPUT');
            columnUuid.type = 'hidden';
            if (that.attr('data-sres-columnuuid') == '__selected__') {
                columnUuid.name = 'column_uuids';
                columnUuid.value = getCheckedColumnUuids().join(',');
            } else {
                columnUuid.name = 'column_uuid';
                columnUuid.value = that.attr('data-sres-columnuuid');
            }
            console.log(columnUuid);
            newForm.appendChild(columnUuid);
        }
        let w = window.open('', 'form-target');
        newForm.target = 'form-target';
        document.body.appendChild(newForm);
        newForm.submit();
    });
});

// Toggle show/hide inactive
$(document).on('change', '#show_inactive, #show_inactive_only', function(ev){
	let showInactive = $('#show_inactive').prop('checked');
	let showInactiveOnly = $('#show_inactive_only').prop('checked');
    let params = new URLSearchParams(window.location.search);
    if (showInactiveOnly) {
        params.set('showInactive', 'only');
    } else if (showInactive) {
        params.set('showInactive', '1');
    } else {
        params.delete('showInactive');
    }
    let newUrl = window.location.protocol + "//" + window.location.host + window.location.pathname + '?' + params.toString();
    //window.location = newUrl;
    window.history.pushState({path:newUrl}, '', newUrl);
    oTable.draw();
});

/**
    Recalculate aggregators
**/
function recalculateAggregatorAll(columnuuid) {
	let showInactive = $("#show_inactive").prop('checked');
    let showInactiveOnly = $('#show_inactive_only').prop('checked');
    sresRecalculateAggregatorAll(
        columnuuid,
        ENV['GET_ALL_IDENTIFIERS_ENDPOINT'],
        ENV['AGGREGATOR_RECALCULATION_ENDPOINT'],
        showInactive,
        showInactiveOnly,
        $("#modal_progress"),
        oTable
    );
}
function recalculateAggregator(columnuuid, predefinedIdentifiers) {
    sresRecalculateAggregator(
        columnuuid,
        ENV['AGGREGATOR_RECALCULATION_ENDPOINT'],
        predefinedIdentifiers,
        $("#modal_progress"),
        oTable
    );
}

/**
    Trigger send notification emails
**/
function forceApplyToOthers(columnuuid) {
	$("#modal_progress")
		.find('.modal-title').html('Force apply to others...').end()
		.find('.sres-modal-message').html('Processing...').end()
		.find('.modal-footer').addClass('d-none').end()
		.find('.progress-bar').css('width', '0%').end()
		.modal('show')
		.on('hidden.bs.modal', function(e){
			oTable.draw(false);
		});
    $.ajax({
        url: ENV['GET_COLUMN_CHANGE_HISTORY_UNIQUE_USERS'].replace('__column_uuid__', columnuuid),
        method: 'GET',
        success: function(data) {
            //console.log(data);
            let usernames = JSON.parse(data);
            let totalRecords = usernames.length;
            let completedRecords = 0;
            let errorRecords = 0;
            $("#modal_progress").find('.sres-modal-message').html('Processing records from ' + usernames.length + ' unique originating users...');
            for (let r = 0; r < usernames.length; r++) {
                $.ajaxq('sres_force_apply_to_others', {
                    url: ENV['APPLY_TO_OTHERS_RETROSPECTIVE'].replace('__column_uuid__', columnuuid) + '?source_username=' + usernames[r],
                    method: 'GET',
                    success: function(data) {
                        data = JSON.parse(data);
                        if (data.success) {
                            completedRecords++;
                            $('#modal_progress .sres-modal-progress .progress-bar').css('width', (completedRecords / totalRecords * 100) + '%');
                            if (completedRecords == totalRecords) {
                                $("#modal_progress")
                                    .find('.sres-modal-message').removeClass('d-none').append('<hr>Finished.').end()
                                    .find('.modal-footer').removeClass('d-none').end();
                                oTable.draw(false);
                            }
                        } else {
                            errorRecords++;
                            console.error(data);
                        }
                    },
                    error: function(err) {
                        errorRecords++;
                        console.error(err);
                    }
                });
            }
        }
    });
}
/**
    Trigger send notification emails
**/
function resendNotificationEmails(columnuuid) {
	var identifiers = getFilteredIdentifiers();
	$("#modal_progress")
		.find('.modal-title').html('Sending notification emails...').end()
		.find('.sres-modal-message').html('Processing ' + identifiers.length + ' records...').end()
		.find('.modal-footer').addClass('d-none').end()
		.find('.progress-bar').css('width', '0%').end()
		.modal('show')
		.on('hidden.bs.modal', function(e){
			oTable.draw(false);
		});
	var totalRecords = identifiers.length;
	var completedRecords = 0;
	var errorRecords = 0;
	console.log(columnuuid, identifiers, totalRecords, completedRecords);
	for (var i = 0; i < identifiers.length; i++) {
		$.ajaxq('sres_notification_emails_resend', {
			url: 'StudentData.cfc?method=email_student_data&columnuuid=' + columnuuid + '&tableuuid=' + getTableUUID() + '&identifier=' + identifiers[i],
			method: 'GET',
			success: function(data) {
				completedRecords++;
				$('#modal_progress .sres-modal-progress .progress-bar').css('width', (completedRecords / totalRecords * 100) + '%');
				if (completedRecords == totalRecords) {
					$("#modal_progress")
						.find('.sres-modal-message').removeClass('d-none').append('<hr>Finished.').end()
						.find('.modal-footer').removeClass('d-none').end();
					oTable.draw(false);
				}
			},
			error: function(data) {
				errorRecords++;
				console.log(data);
				$("#modal_progress .sres-modal-message").append('<br>Error sending notification. ' + data.responseText);
			}
		});
	}
};

/**
    Bulk apply data
**/
function applyDataBulk(columnuuid) {
	var identifiers = getFilteredIdentifiers();
	$("#modal_bulk_apply")
		.find('.modal-title').html('Apply data to ' + identifiers.length + ' people').end()
		.find('.sres-modal-bulk-apply-input').attr('data-sres-columnuuid', columnuuid).end()
		.find('.sres-modal-message').html('').end()
		.find('.sres-modal-progress').addClass('d-none').end()
		.find('.sres-modal-footer-bulk-apply').removeClass('d-none').end()
		.find('.progress-bar').css('width', '0%').end()
		.modal('show')
		.on('shown.bs.modal', function(e){
			$('#modal_bulk_apply .sres-modal-bulk-apply-input').focus();
		})
		.on('hidden.bs.modal', function(e){
			oTable.draw(false);
		});
}
$(document).on('click', '#modal_bulk_apply button.sres-modal-footer-bulk-apply', function(ev) {
	var identifiers = getFilteredIdentifiers();
	var totalRecords = identifiers.length;
	var completedRecords = 0;
	var errorRecords = 0;
	var columnuuid = $('#modal_bulk_apply .sres-modal-bulk-apply-input').attr('data-sres-columnuuid');
	console.log(columnuuid, identifiers, totalRecords, completedRecords);
	$('#modal_bulk_apply .sres-modal-progress').removeClass('d-none');
	var identifierStep = identifiers.length > 10 ? 10 : 1;
    for (var i = 0; i < identifiers.length; i += identifierStep) {
        var identifiersArray = [];
		if (identifierStep == 1) {
            identifiersArray.push(identifiers[i]);
		} else {
			let identifiersSliced = identifiers.slice(i, i + identifierStep);
            identifiersArray = identifiersArray.concat(identifiersSliced);
		}
		$.ajaxq('sres_bulk_apply', {
			url: ENV['SET_DATA_ENDPOINT'].replace('__column_uuid__', columnuuid),
			method: 'POST',
			data: {
				'table_uuid': getTableUUID(),
				'column_uuid': columnuuid,
				'identifiers': identifiersArray,
				'data': $('#modal_bulk_apply .sres-modal-bulk-apply-input').val()
			},
			success: function(data) {
				data = JSON.parse(data);
                Object.keys(data).forEach(function(identifier, currentResult) {
                    completedRecords++;
                    if (currentResult.success == false || currentResult.success == 'false') {
                        errorRecords++;
                        $("#modal_bulk_apply .sres-modal-message").append('<br>Error saving data for ' + currentResult.person.sid);
                    }
                    $('#modal_bulk_apply .sres-modal-progress .progress-bar').css('width', (completedRecords / totalRecords * 100) + '%');
                    if (completedRecords == totalRecords) {
                        $("#modal_bulk_apply")
                            .find('.sres-modal-message').append('<hr>Finished.').end()
                        oTable.draw(false);
                    }
                });
			}
		});
	}
});

/**
    Custom views
**/

// Save current view
$(document).ready(function(){
	$('#modal_views_save select.sres-modal-views-authusers').chosen({
		width:'100%',
		placeholder_text_multiple: 'Select from existing list user(s)'
	});
});
function saveCurrentView(showUnsavedMessage) {
	var viewInfo = getViewInfo();
	if (showUnsavedMessage) {
		$("div.sres-modal-views-unsaved").removeClass('d-none');
	} else {
		$("div.sres-modal-views-unsaved").addClass('d-none');
	}
	if (viewInfo.viewuuid != '') {
		$('#modal_views_save')
			.find('button.sres-modal-views-save-over').removeClass('d-none').end()
			.find('input:text.sres-modal-views-save-name').val(viewInfo.name).end()
			.find('input:text.sres-modal-views-save-description').val(viewInfo.description).end()
            .find('input:checkbox.sres-modal-views-save-default').prop('checked', viewInfo.role == 'default' ? true : false);
	} else {
		$('#modal_views_save')
			.find('button.sres-modal-views-save-over').addClass('d-none').end()
			.find('input:text.sres-modal-views-save-name').val('').end()
			.find('input:text.sres-modal-views-save-description').val('').end()
            .find('input:checkbox.sres-modal-views-save-default').prop('checked', false);
	}
    $('#modal_views_save button.sres-modal-views-save-new').html('Save as new view');
	$('#modal_views_save')
		.find('.sres-modal-message').html('').end()
		.modal('show');
}
$(document).ready(function(){
	if ($("a.sres-view-unsaved").length > 0) {
		saveCurrentView(true);
	}
});
$(document).on('click', '#modal_views_save button.sres-modal-views-save-over, #modal_views_save button.sres-modal-views-save-new', function(ev){
	// check at least a name is specified
    if (!$('#modal_views_save input:text.sres-modal-views-save-name').val()) {
        alert('Please name this custom view.');
        return false;
    }
    // continue
    let viewuuid = '';
	var role = $('#sres_modal_views_save_default').prop('checked') ? 'default' : 'additional';
	if ($(this).hasClass('sres-modal-views-save-over')) {
		viewuuid = getViewInfo().viewuuid;
	} else {
		viewuuid = '';
	}
	var authusers = $('#modal_views_save select.sres-modal-views-authusers').val() ? JSON.stringify($('#modal_views_save select.sres-modal-views-authusers').val()) : JSON.stringify([]);
	$.ajax({
		url: ENV['SAVE_VIEW_ENDPOINT'],
		method: 'POST',
		data: {
			table_uuid: getTableUUID(),
			state: JSON.stringify(oTable.state()),
			view_uuid: viewuuid,
			name: $('#modal_views_save input:text.sres-modal-views-save-name').val(),
			description: $('#modal_views_save input:text.sres-modal-views-save-description').val(),
			role: role,
			auth_users: authusers,
			frozencolumns: $("select.sres-edit-column-visibility-system-freeze-columns").val(),
			displayrestricted: $("select.sres-edit-column-show-restricted-by-username").val()
		},
		success: function(data) {
			data = JSON.parse(data);
			console.log(data);
			if (data.success) {
				$('#modal_views_save .sres-modal-message').html('<div class="alert alert-success">Successfully saved. <a href="' + ENV['VIEW_TABLE_URL'] + '?view=' + data.view_uuid + '" role="button" class="btn btn-primary">Show me</a></div>');
			} else {
				$('#modal_views_save .sres-modal-message').html('<div class="alert alert-danger">Unspecified error while saving</div>');
			}
            $('#modal_views_save button.sres-modal-views-save-new').html('Save again as another new view');
		},
		error: function(data) {
			$('#modal_views_save .sres-modal-message').html('<div class="alert alert-danger">Unspecified error while saving</div>');
		}
	});
});
$(document).on('click', 'button.sres-modal-views-authusers-addall', function(){
	$('select.sres-modal-views-authusers option').prop('selected', true);
	$('select.sres-modal-views-authusers').trigger('chosen:updated');
	
});

// Delete current view
function deleteCurrentView() {
	var viewuuid = getViewInfo().viewuuid;
	if (viewuuid.length <= 0) {
		alert('There is no view to delete');
		return false;
	}
	$.ajax({
		url: ENV['DELETE_VIEW_ENDPOINT'],
		method: 'DELETE',
		data: {
			table_uuid: getTableUUID(),
			view_uuid: viewuuid
		},
		success: function(data) {
			console.log(data);
			data = JSON.parse(data);
			if (data.success) {
				alert('View successfully deleted');
				window.location.href = ENV['VIEW_TABLE_URL'];
				return true;
			} else {
				alert('Error deleting view');
				return false;
			}
		},
		error: function(data) {
			console.log(data);
			alert('Error deleting view');
		}
	});
	
}

/**
    List column config
**/

// Edit list columns
function editColumnsShowing(){ 
	$("#modal_views_edit_columns").modal('show');
};
$(document).on('click', '#checkAllUserDefined', function(){
	var checked = $(this).prop('checked');
	$('#modal_views_edit_columns input:checkbox.sres-edit-column-visibility-user').each(function(){
		$(this).prop('checked', checked);
	});
});
$(document).on('click', '#checkAllSystem', function(){
	var checked = $(this).prop('checked');
	$('#modal_views_edit_columns input:checkbox.sres-edit-column-visibility-system').each(function(){
		$(this).prop('checked', checked);
	});
});
function applyColumnsShowing(){
	var systemColumns = $('#system_column_display_sortable').sortable('serialize').get();
	var userColumns = $('#column_display_sortable').sortable('serialize').get();
	console.log(systemColumns, userColumns);
	var newForm = document.createElement('FORM');
	newForm.method = 'POST';
	newForm.action = location.href;
	var csrfToken = document.createElement('INPUT');
	csrfToken.type = 'hidden';
	csrfToken.name = 'csrf_token';
	csrfToken.value = ENV['CSRF_TOKEN'];
	newForm.appendChild(csrfToken);
    var elAction = document.createElement('INPUT');
    elAction.type = 'hidden';
    elAction.name = 'action';
    elAction.value = 'set_columns_showing';
    newForm.appendChild(elAction);
	var elSystemColumns = document.createElement('INPUT');
	elSystemColumns.type = 'hidden';
	elSystemColumns.name = 'system_columns';
	elSystemColumns.value = JSON.stringify(systemColumns);
	newForm.appendChild(elSystemColumns);
	var elFrozenColumns = document.createElement('INPUT');
	elFrozenColumns.type = 'hidden';
	elFrozenColumns.name = 'frozencolumns';
	elFrozenColumns.value = $("select.sres-edit-column-visibility-system-freeze-columns").val();
	newForm.appendChild(elFrozenColumns);
	var elDisplayRestricted = document.createElement('INPUT');
	elDisplayRestricted.type = 'hidden';
	elDisplayRestricted.name = 'displayrestricted';
	elDisplayRestricted.value = $("select.sres-edit-column-show-restricted-by-username").val();
	newForm.appendChild(elDisplayRestricted);
	var elUserColumns = document.createElement('INPUT');
	elUserColumns.type = 'hidden';
	elUserColumns.name = 'user_columns';
	elUserColumns.value = JSON.stringify(userColumns);
	newForm.appendChild(elUserColumns);
	document.body.appendChild(newForm);
	newForm.submit();
};

/**
    Merge records
**/
// Show merging config UI
$(document).on('click', '.sres-merge-records', function(){
    if (oTable.rows()[0].length != 2) {
        alert("There must be exactly two visible rows.");
        return false;
    }
    let oid_a = $(oTable.row(0).node()).attr('data-sres-oid');
    let oid_b = $(oTable.row(1).node()).attr('data-sres-oid');
    $.when(
        $.ajax(ENV['GET_STUDENT_DATA_ENDPOINT'].replace('__oid__', oid_a)),
        $.ajax(ENV['GET_STUDENT_DATA_ENDPOINT'].replace('__oid__', oid_b)),
        $.ajax(ENV['GET_COLUMNS_LIST_ENDPOINT'] + '?base_only=1')
    ).done(function(a, b, c){
        data_a = JSON.parse(a[0]);
        data_b = JSON.parse(b[0]);
        columns = JSON.parse(c[0]);
        console.log(data_a, data_b, columns);
        $('#modal_merge_records_rows').html('');
        let template = Handlebars.compile(document.getElementById("merge_records_row_template").innerHTML);
        // iterate config values
        Object.keys(data_a.config).forEach(function(k){
            $('#modal_merge_records_rows').append(template({
                column_uuid: k,
                column_header: k,
                oid_a: oid_a,
                oid_b: oid_b,
                content_a: data_a.config[k],
                content_b: data_b.config[k]
            }));
        });
        // iterate data values
        columns.columns.forEach(function(column, i){
            $('#modal_merge_records_rows').append(template({
                column_uuid: column.value,
                column_header: column.display_text,
                oid_a: oid_a,
                oid_b: oid_b,
                content_a: data_a.data[column.value],
                content_b: data_b.data[column.value]
            }));
        });
        $('#modal_merge_records').modal('show');
        $('.sres-merge-record-selector[data-sres-merge-record-column-uuid=sid][value=a]').trigger('click');
    });
});
// UI for which record is primary/secondary
$(document).on('click', '.sres-merge-record-selector[data-sres-merge-record-column-uuid=sid]', function(){
    $('.badge.sres-merge-record-a').addClass('d-none');
    $('.badge.sres-merge-record-b').addClass('d-none');
    if ($(this).val() == 'a') {
        $('.badge-primary.sres-merge-record-a').removeClass('d-none');
        $('.badge-secondary.sres-merge-record-b').removeClass('d-none');
    } else {
        $('.badge-primary.sres-merge-record-b').removeClass('d-none');
        $('.badge-secondary.sres-merge-record-a').removeClass('d-none');
    }
});
// Selecting all
$(document).on('click', 'button.sres-merge-record-select-all', function(){
    let recordCol = $(this).attr('data-sres-merge-record-col');
    $('input:radio.sres-merge-record-selector[value=' + recordCol + ']').trigger('click');
});
// Performing the merge
$(document).on('click', '.sres-merge-record-apply', function(){
    let primaryRecord = $('input:checked.sres-merge-record-selector[data-sres-merge-record-column-uuid=sid]').val();
    let secondaryRecord = primaryRecord == 'a' ? 'b' : 'a';
    let oid_a = $('input[name=sres_merge_record_column_sid][value=' + primaryRecord + ']').parents('[data-sres-merge-record-oid]').attr('data-sres-merge-record-oid');
    let oid_b = $('input[name=sres_merge_record_column_sid][value=' + secondaryRecord + ']').parents('[data-sres-merge-record-oid]').attr('data-sres-merge-record-oid');
    console.log(primaryRecord, secondaryRecord, oid_a, oid_b);
    let mappingData = {};
    $('.sres-merge-record-selector:checked').each(function(){
        mappingData[$(this).attr('name')] = $(this).val();
    });
    // Call server
    $.ajax({
        url: ENV['MERGE_STUDENT_RECORDS_ENDPOINT'].replace('__oid_a__', oid_a).replace('__oid_b__', oid_b),
        method: 'POST',
        data: mappingData,
        success: function(data){
            data = JSON.parse(data);
            alert('Merged ' + data.success_count + ' fields successfully, ' + data.failure_count + ' failed.');
            if (data.failure_count > 0) {
                alert('The secondary record was not removed due to problems with the merge process.');
            } else {
                $('#modal_merge_records').modal('hide');
                oTable.draw();
            }
        }
    });
});

/**
    Collapsing row_info_actions
**/
$(document).on('hidden.bs.collapse shown.bs.collapse', '#row_info_actions', function(event){
    oTable.draw();
    if (event.type == 'hidden') {
        $('#collapse_row_info_actions span').removeClass('fa-chevron-up').addClass('fa-chevron-down');
    } else {
        $('#collapse_row_info_actions span').removeClass('fa-chevron-down').addClass('fa-chevron-up');
    }
    console.log(event);
});

/**
    Column jumper
**/
$(document).ready(function(){
    $('.sres-datatables-column-jumper-container').html(
        '<label for="#sres_datatables_column_jumper">Jump to column:</label>' +
        '<input type="text" class="form-control form-control-sm ml-2" id="sres_datatables_column_jumper">'
    );
    $("#sres_datatables_column_jumper").autocomplete(
        {
            minLength: 2,
            autoWidth: false/*,
            autoselect: true*/
        }, 
        {
            name: 'search-results',
            source: function(query, callback){
                //console.log(query, callback);
                let foundColumns = [];
                oTable.columns('.sres-column-data').every(function(index){
                    let column = oTable.column(index);
                    let header = column.header();
                    let columnName = $(header).find('.sres-column-title-text').text().toLowerCase();
                    let columnDescription = $(header).find('.sres-column-info').text().toLowerCase();
                    if ((columnName + columnDescription).indexOf(query.toLowerCase()) > -1) {
                        foundColumns.push({
                            columnuuid: $(header).attr('data-sres-columnuuid'),
                            left: $(header).position().left,
                            name: $(header).find('.sres-column-title-text').text(),
                            description: $(header).find('.sres-column-info').text()
                        });
                    }
                });
                //console.log('foundColumns', foundColumns);
                callback(foundColumns);
            },
            templates: {
                suggestion: function(suggestion, answer){
                    //console.log(suggestion, answer);
                    let template = Handlebars.compile(document.getElementById("column_jumper_result_line_template").innerHTML);
                    return template({
                        columnuuid: suggestion.columnuuid,
                        name: suggestion.name,
                        description: suggestion.description,
                        left: suggestion.left
                    })
                },
                empty: function(){
                    return '<span>No columns found</span>';
                }
            }
        }
    ).on('autocomplete:selected', function(event, suggestion, dataset, context) {
        //console.log('autocomplete:selected', event, suggestion, dataset, context);
        let DTFCwidth = $('.DTFC_LeftHeadWrapper').width();
        let scrollHeadLeft = $('.dataTables_scrollHeadInner').position().left;
        let left = $(oTable.column("[data-sres-columnuuid='" + suggestion.columnuuid + "']").nodes()[0]).position().left;
        let scrollTo = left - DTFCwidth; // - scrollHeadLeft;
        $('.dataTables_scrollBody').scrollLeft(scrollTo);
    });
    $(document).on('click', "#sres_datatables_column_jumper", function(){
        setTimeout(function(){
            let e = $.Event("keydown");
            e.which = 40;
            $("#sres_datatables_column_jumper").trigger(e);
        }, 100);
    });
});


