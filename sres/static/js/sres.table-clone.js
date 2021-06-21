

$(document).on('click', 'button.sres-select-column-toggle-all', function() {
	$('input:checkbox[name=select_column]').each(function(){
		$(this).prop('checked', !$(this).prop('checked'));
	});
});
$(document).on('click', 'button.sres-select-column-select-all', function() {
	$('input:checkbox[name=select_column]').each(function(){
		$(this).prop('checked', true);
	});
});
$(document).on('click', 'button.sres-select-column-unselect-all', function() {
	$('input:checkbox[name=select_column]').each(function(){
		$(this).prop('checked', false);
	});
});

// Checking columns by default
$(document).ready(function(){
    $('input[name=select_column]').each(function(){
        var name = $(this).siblings('span').text();
        if (name.match(/^Tracking counter for|^CANVAS_|^CANVAS[0-9]+/gi)) {
            // don't check
        } else {
            $(this).prop('checked', true);
        }
        
    });
});

// Timeshift
$(document).on('change', 'select[name=column_timeshift]', function(){
	if ($(this).val() == 'shift') {
		$('div.sres-column-timeshift-container').removeClass('d-none');
		$('#timeshift_from').prop('required', true);
		$('#timeshift_to').prop('required', true);
	} else {
		$('div.sres-column-timeshift-container').addClass('d-none');
		$('#timeshift_from').removeProp('required');
		$('#timeshift_to').removeProp('required');
	}
});
$(document).on('change', '#timeshift_from, #timeshift_to', function(){
	let timeshiftFrom = new Date($('#timeshift_from').val());
	let timeshiftTo = new Date($('#timeshift_to').val());
	let daysDiff = Math.floor((timeshiftTo - timeshiftFrom) / (1000*24*60*60));
	if (daysDiff) {
        $('span.sres-timeshift-diff').html(Math.abs(daysDiff) + ' days ' + (daysDiff < 0 ? 'backward' : 'forward'));
    }
});
$(document).ready(function(){
	$('#timeshift_from').trigger('change');
});

// Clone mode
$(document).ready(function(){
	$("select[name=clone_mode_existing_list]").chosen({width:'100%'})
});
$(document).on("change", "select[name=clone_mode]", function(){
	$("div.sres-clone-list-mode").addClass("d-none");
	switch ($(this).val()) {
		case "new":
			$("div.sres-clone-list-new").removeClass("d-none");
			break;
		case "existing":
			$("div.sres-clone-list-existing").removeClass("d-none");
			break;
	}
});
$(document).ready(function(){
    $('select[name=clone_mode]').trigger('change');
});

/**
	column connections
**/
var columnConnections = [];
// jsPlumb
jsPlumb.ready(function(){
	var color = "gray";
	var instance = jsPlumb.getInstance({
		Connector: [ 
			"Flowchart", 
			{ stub: [50, 100], gap: 0, cornerRadius: 10, alwaysRespectStubs: true } 
		],
		ConnectionOverlays: [
			["Arrow", {
				location: 1,
				visible: true,
				width: 11,
				length: 11,
				id: "ARROW"
			}
			]
		],
		PaintStyle: {stroke: color, strokeWidth: 1},
		ConnectorStyle: { strokeWidth: 1 },
		EndpointStyle: {radius: 0.1/*, fill: color*/}		
	});
	let connectionColours = {};
	for (var c = 0; c < columnConnections.length; c++) {
		if (!(columnConnections[c].source in connectionColours)) {
			connectionColours[columnConnections[c].source] = '#'+(Math.random()*0xFFFFFF<<0).toString(16);
		}
		instance.connect({
			source: "sres_column_" + columnConnections[c].source,
			target: "sres_column_" + columnConnections[c].target,
			detachable: false,
			anchor: "Right",
			paintStyle: {stroke: connectionColours[columnConnections[c].source]}
		});
	}
});
$(document).on("click", "span.sres-column-link", function(){
	$("label.sres-column-label").removeClass("bg-danger-light").removeClass("bg-info-light");
	let columnUuid = $(this).siblings("[data-sres-columnuuid]").attr("data-sres-columnuuid");
	let linkCounter = 0;
	if ($(this).hasClass("sres-column-link-in")) {
		// Find columns that this column references or uses
		$(this).siblings("label.sres-column-label").addClass("bg-danger-light");
		for (var c = 0; c < columnConnections.length; c++) {
			if (columnConnections[c].target == columnUuid) {
				$("span[data-sres-columnuuid=" + columnConnections[c].source + "]").siblings("label.sres-column-label").addClass("bg-info-light");
				linkCounter++;
			}
		}
		$.notify({message: linkCounter + ' columns (blue) are used by this column (red)'}, {type:'info'});
	} else if ($(this).hasClass("sres-column-link-out")) {
		// Find columns that reference or use this column
		$(this).siblings("label.sres-column-label").addClass("bg-info-light");
		for (var c = 0; c < columnConnections.length; c++) {
			if (columnConnections[c].source == columnUuid) {
				$("span[data-sres-columnuuid=" + columnConnections[c].target + "]").siblings("label.sres-column-label").addClass("bg-danger-light");
				linkCounter++;
				console.log(columnConnections[c]);
			}
		}
		$.notify({message: linkCounter + ' columns (red) use this column (blue)'}, {type:'info'});
	}
});


