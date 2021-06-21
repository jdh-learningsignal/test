
$(document).ready(function(){
	$("#column_rereferencer_list_select").chosen({width: '100%', search_contains: true});
});
$(document).on('click', '[data-sres-trigger-click=".sres-column-referencer-show"]', function(){
    let preselectTableUuid = $(this).attr('data-sres-target-table-uuid');
    $('button.sres-column-rereferencer-show').trigger('click', {preselectTableUuid: preselectTableUuid});
});
$(document).on("click", "button.sres-column-rereferencer-show", function(event, params) {
    let defaultTableUuid = '';
    if (typeof params !== 'undefined' && params.hasOwnProperty('preselectTableUuid')) {
        defaultTableUuid = params.preselectTableUuid;
    }
	$("#column_rereferencer_list_select").val(defaultTableUuid).trigger("change").trigger("chosen:updated");
	$("#column_rereferencer").modal('show');
});
$(document).on("change", "#column_rereferencer_list_select", function() {
	$("#column_rereferencer_column_suggestions_container").html('');
	$("#column_rereferencer_column_suggestions").addClass("d-none");
	$("#column_rereferencer_button_do").addClass("d-none");
	$("#column_rereferencer_column_suggestions_hint").addClass("d-none");
	$("#column_rereferencer_column_suggestions_failed").addClass("d-none");
	$("#column_rereferencer_tracking_counter").addClass("d-none");
	if ($(this).val() != "") {
		var targetTableUuid = $("#column_rereferencer_list_select").val().replace("StudentList_", "");
        // if querybuilder is active, trigger adding the selected table to list-chooser-show-chooser
        if ($("#list_chooser_table_list")) {
            if ($("#list_chooser_table_list").val().indexOf(targetTableUuid) == -1) {
                let newList = $("#list_chooser_table_list").val();
                newList.push(targetTableUuid);
                $("#list_chooser_table_list")
                    .val(newList)
                    .trigger('chosen:updated');
                $("#list_chooser_button_do").trigger("click");
            }
        }
        // continue
		var rereferenceableColumns = getAllRereferenceableColumns();
		$("#column_rereferencer_loading").removeClass('d-none');
		$.ajax({
			method: "post",
			url: ENV['REREFERENCE_COLUMNS_ENDPOINT'],
			data: {
				"target_table_uuid": targetTableUuid,
				"existing_columns": JSON.stringify(rereferenceableColumns)
			},
			success: function(data){
				data = JSON.parse(data);
				//console.log(data);
				//console.log(rereferenceableColumns);
                $("#column_rereferencer_column_suggestions_hint").removeClass("d-none");
				$("#column_rereferencer_column_suggestions").removeClass("d-none");
				$("#column_rereferencer_button_do").removeClass("d-none");
				var c = -1;
				for (var columnReference in data["suggested_columns"]) {
					c++;
					let suggestionSelectId = "column_rereferencer_suggestion_" + c;
					if (!data["suggested_columns"].hasOwnProperty(columnReference)) {
						continue;
					}
					let suggestedColumn = data["suggested_columns"][columnReference];
					// Start building selector
					let columnSelectorHtml = '<label for="' + suggestionSelectId + '">' + suggestedColumn["original_column_display_full"] + ' ';
					columnSelectorHtml += '&nbsp;<code>' + columnReference + '</code>&nbsp; should be re-referenced to</label>';
					columnSelectorHtml += '<div class="form-group">';
					// Selection dropdown
					let suggestionSelectHtml = "";
					let suggestionCount = 0;
					suggestionSelectHtml += '<select class="form-control sres-column-rereferencer-select-suggestion" id="' + suggestionSelectId + 
                        '" data-sres-type="' + suggestedColumn["reference_type"] + 
                        '" data-sres-current-reference="' + suggestedColumn["current_column_reference"] + 
                        '" data-sres-source-element-id="' + suggestedColumn["source_element_id"] + 
                        '" data-sres-source-element-selector="' + suggestedColumn["source_element_selector"] + 
                        '">';
					for (var s = 0; s < suggestedColumn["suggested_target_column_references"].length; s++) {
						let suggestion = suggestedColumn["suggested_target_column_references"][s];
                        suggestionSelectHtml += '<option value="' + suggestion["column_reference"] + (suggestedColumn['magic_formatter'].length ? '.' + suggestedColumn['magic_formatter'] : '') + '">';
                        suggestionSelectHtml += suggestion["full_display"];
                        suggestionSelectHtml += '</option>';
                        suggestionCount++;
					}
					suggestionSelectHtml += '<option value="_other">Let me pick another column</option>';
					suggestionSelectHtml += '<option value="_none">Do not re-reference</option>';
					suggestionSelectHtml += '</select>';
					// Finalise html
					columnSelectorHtml += suggestionSelectHtml;
					columnSelectorHtml += '</div>';
					// Insert
					$("#column_rereferencer_column_suggestions_container").append(columnSelectorHtml);
					if (suggestionCount == 0) {
						$("#" + suggestionSelectId).val("_none");
						$("#" + suggestionSelectId + " option[value='_none']").text("No suitable suggestion found - do not re-reference");
					}
					// Format
					$("select.sres-column-rereferencer-select-suggestion").trigger("change");
				}
                $("#column_rereferencer_loading").addClass('d-none');
			},
			error: function() {
				$("#column_rereferencer_column_suggestions_failed").removeClass("d-none");
                $("#column_rereferencer_loading").addClass('d-none');
			},
			complete: function() {
				$("#column_rereferencer_loading").addClass('d-none');
			}
		});
		if (location.pathname.indexOf("/filters/") > 0) {
			// Tracking counter alert
			if (typeof $("input[name=tracking_record_tableuuid]").val() !== 'undefined' && targetTableUuid != $("input[name=tracking_record_tableuuid]").val()) {
				$("#column_rereferencer_tracking_counter").removeClass("d-none");
			}
		}
	}
});
$(document).on("click", "#column_rereferencer_button_do", function() {
	// Column re-referencing
	$("select.sres-column-rereferencer-select-suggestion").each(function(){
		let targetColumnReference = $(this).val();
		if (targetColumnReference != "_none" && targetColumnReference != "_other") {
			let targetTableUuid = $("#column_rereferencer_list_select").val().replace("StudentList_", "");
			let targetColumnDisplayName = $(this).find("option:selected").text();
			let currentColumnReference = $(this).attr("data-sres-current-reference");
			//let referenceType = $(this).attr("data-sres-type");
			//let sourceElementId = $(this).attr("data-sres-source-element-id");
			//let sourceElementSelector = $(this).attr("data-sres-source-element-selector");
			/*if (sourceElementId) {
                var sourceElement = $("#" + sourceElementId);
            } else {
                var sourceElement = $(sourceElementSelector);
            }*/
            let rereferenceableColumns = getAllRereferenceableColumns();
            rereferenceableColumns.forEach(function(e){
                if (e.column_reference == currentColumnReference) {
                    var sourceElement = $(e.source_element_selector);
                    var re = null;
                    switch (e.type) {
                        case "raw":
                            re = new RegExp("" + currentColumnReference, "g");
                            sourceElement.val(sourceElement.val().replace(re, targetColumnReference)).trigger("change");
                            break;
                        case "value":
                            re = new RegExp("\\$" + currentColumnReference + "\\$", "g");
                            sourceElement.val(sourceElement.val().replace(re, "$" + targetColumnReference + "$")).trigger("change");
                            break;
                        case "text":
                            re = new RegExp("\\$" + currentColumnReference + "\\$", "g");
                            sourceElement.text(sourceElement.text().replace(re, "$" + targetColumnReference + "$")).trigger("change");
                            break;
                        case "code":
                            re = new RegExp("\\`" + currentColumnReference + "\\`", "g");
                            sourceElement.val(sourceElement.val().replace(re, "`" + targetColumnReference + "`")).trigger("change");
                            break;
                        case "html":
                            re = new RegExp("\\$" + currentColumnReference + "\\$", "g");
                            sourceElement.html(sourceElement.html().replace(re, "$" + targetColumnReference + "$")).trigger("change");
                            break;
                        case "chooser":
                            re = new RegExp(currentColumnReference, "g");
                            sourceElement.val(sourceElement.val().replace(re, targetColumnReference));
                            var sourceTableUuid = sourceElement.attr("data-sres-tableuuid");
                            sourceElement.attr("data-sres-tableuuid", targetTableUuid);
                            sourceElement.siblings("span.sres-condition-column-placeholder").html(targetColumnDisplayName);
                            sourceElement.siblings("a.sres-condition-column-placeholder-links").each(function() {
                                $(this).attr("href", $(this).attr("href").replace(currentColumnReference, targetColumnReference));
                                $(this).attr("href", $(this).attr("href").replace(sourceTableUuid, targetTableUuid));
                            });
                            break;
                        case "multiselect":
                            sourceElement.multiSelect("deselect", currentColumnReference);
                            sourceElement.multiSelect("select", targetColumnReference);
                            break;
                        case "querybuilder-chosen":
                            // update the UI
                            sourceElement.val(targetColumnReference).trigger('chosen:updated');
                            // update querybuilder
                            m = sourceElement.parents('.sres-querybuilder-container').queryBuilder('getModel');
                            for (var r = 0; r < m.rules.length; r++) {
                                if (m.rules[r].filter.id == currentColumnReference) {
                                    m.rules[r].filter.id = targetColumnReference;
                                }
                                if (m.rules[r].filter.field == currentColumnReference) {
                                    m.rules[r].filter.field = targetColumnReference;
                                }
                            }
                            break;
                    }
                    
                }
            });
            
		}
	});
	if (location.pathname.indexOf("/filters/") > 0) {
		// Tracking column business
		if ($("#column_rereferencer_tracking_counter_option").val() == "new" && typeof $("#email_tracking_column_name").val() == "undefined") {
			$("#email_tracking_config_container").prepend('<input type="text" name="email_tracking_column_name" id="email_tracking_column_name" class="form-control" required>');
			$("#filter_name").trigger("change");
		}
	}
	// Hide modal
	$("#column_rereferencer").modal('hide');
});
$(document).on("change", "select.sres-column-rereferencer-select-suggestion", function() {
	var thisId = $(this).attr("id");
	if ($(this).val() == "_other") {
		show_column_chooser(
			$(this).attr("id"), 
			"", 
			$(this).attr("id"),
			false, 
			true,
			$("#column_rereferencer_list_select").val().replace("StudentList_", ""), 
			null, 
			true
		);
	} else if ($(this).val() == "_none") {
		$("label[for='" + thisId + "']").addClass("bg-danger-light p-2 rounded").removeClass("bg-info-light");
	} else {
		$("label[for='" + thisId + "']").addClass("bg-info-light p-2 rounded").removeClass("bg-danger-light");
	}
});

function getAllRereferenceableColumns() {
	var rereferenceableColumns = [];
	$(".sres-column-rereferenceable.sres-column-rereferenceable-raw").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences(null, $(this).val(), $(this), "raw"));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-value").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences("$", $(this).val(), $(this), "value"));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-text").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences("$", $(this).text(), $(this), "text"));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-code").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences("`", $(this).val(), $(this), "code"));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-html").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences("$", $(this).html(), $(this), "html"));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-chooser").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences(null, $(this).val(), $(this), "chooser"));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-multiselect").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences(null, null, $(this), "multiselect", $(this).val()));
	});
	$(".sres-column-rereferenceable.sres-column-rereferenceable-querybuilder-chosen").each(function(){
		rereferenceableColumns = rereferenceableColumns.concat(_extractColumnReferences(null, $(this).val(), $(this), "querybuilder-chosen"));
	});
	//console.log(rereferenceableColumns);
	return rereferenceableColumns;
}
function _extractColumnReferences(delimiter, input, sourceElement, type, columnArray) {
	let boundary = typeof delimiter !== 'undefined' && delimiter !== null ? "\\" + delimiter : "";
	let re = new RegExp(boundary + "COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*" + boundary, "g");
	var columns = null;
	if (typeof columnArray !== 'undefined' && columnArray !== null) {
		columns = columnArray;
	} else {
		columns = input.match(re);
		columns = [...new Set(columns)];
	}
	if (columns !== null && columns.length > 0) {
		let returnArr = [];
		for (let c = 0; c < columns.length; c++) {
			if (type == 'querybuilder-chosen') {
                sourceElementSelector = 'select[name=' + sourceElement.attr("name") + ']';
            } else {
                sourceElementSelector = "#" + sourceElement.attr("id");
            }
            returnArr.push({
				"source_element_id": sourceElement.attr("id") ? sourceElement.attr("id") : '',
				"source_element_selector": sourceElementSelector,
				"column_reference": columns[c].replace(/\$|\`/g, ""),
				"type": type
			});
		}
		return returnArr;
	} else {
		return [];
	}
}

