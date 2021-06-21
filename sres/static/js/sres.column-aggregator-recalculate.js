/**
    Recalculate aggregators
**/
function sresRecalculateAggregatorCancel() {
    $.ajaxq.abort('sres_aggregator_recalculate');
}
function sresRecalculateAggregatorAll(columnuuid, getAllIdentifiersEndpoint, aggregatorRecalculationEndpoint, showInactive, showInactiveOnly, progressModal, dataTable) {
	progressModal
		.find('.modal-title').html('Recalculating...').end()
		.find('.sres-modal-message').html('Processing...').end()
		.find('.modal-footer').addClass('d-none').end()
		.find('.progress-bar').css('width', '0%').end()
		.modal('show');
    // form the endpoint url
    let url = new URL(getAllIdentifiersEndpoint);
    let searchParams = url.searchParams
    if (showInactive || showInactiveOnly) {
        searchParams.set('include_inactive', 'true');
    } else {
        searchParams.delete('include_inactive');
    }
    url.search = searchParams.toString();
    // call
    $.ajax({
		method: 'GET',
		url: url.toString(),
		success: function(data){
			data = JSON.parse(data);
			//recalculateAggregator(columnuuid, data);
            sresRecalculateAggregator(
                columnuuid,
                aggregatorRecalculationEndpoint,
                data,
                progressModal,
                dataTable
            );
		}
	});
}
function sresRecalculateAggregator(columnuuid, aggregatorRecalculationEndpoint, predefinedIdentifiers, progressModal, dataTable) {
	let identifiers = [];
    let oTable = undefined;
    if (typeof dataTable !== 'undefined') {
        oTable = dataTable;
    }
    if (typeof predefinedIdentifiers !== 'undefined') {
		identifiers = predefinedIdentifiers;
	} else {
		identifiers = getFilteredIdentifiers();
	}
	progressModal
		.find('.modal-title').html('Recalculating...').end()
		.find('.sres-modal-message').html('Processing ' + identifiers.length + ' records...').end()
		.find('.modal-footer').addClass('d-none').end()
		.find('.progress-bar').css('width', '0%').end()
		.modal('show')
		.on('hidden.bs.modal', function(e){
			if (typeof oTable !== 'undefined') {
                oTable.draw(false);
            }
            sresRecalculateAggregatorCancel();
		});
	let totalRecords = identifiers.length;
	let completedRecords = 0;
	let errorRecords = 0;
	//console.log(columnuuid, identifiers, totalRecords, completedRecords);
	let identifierStep = identifiers.length > 10 ? 10 : 1;
	for (let i = 0; i < identifiers.length; i += identifierStep) {
		let identifierParam = "";
        let identifiersArray = [];
		if (identifierStep == 1) {
			identifierParam = "&identifier=" + identifiers[i];
            identifiersArray.push(identifiers[i]);
		} else {
			identifierParam = "&identifiers=";
			let identifiersFrom = i;
			let identifiersTo = i + identifierStep;
			let identifiersSliced = identifiers.slice(identifiersFrom, identifiersTo);
			//console.log('xx', i, identifiersFrom, identifiersTo, identifiersSliced);
			identifierParam += encodeURIComponent(JSON.stringify(identifiersSliced));
            identifiersArray = identifiersArray.concat(identifiersSliced);
		}
		progressModal.find(".sres-modal-progress .progress-bar").addClass("progress-bar-striped active");
		$.ajaxq('sres_aggregator_recalculate', {
			url: aggregatorRecalculationEndpoint.replace('__column_uuid__', columnuuid),
			method: 'POST',
            data: {
                'identifiers': identifiersArray
            },
			success: function(data) {
                try {
                    data = JSON.parse(data);
                    //console.log(data);
                    Object.keys(data).forEach(function(identifier, currentResult) {
                        completedRecords++;
                        //console.log(identifier, currentResult);
                        if (currentResult.success == false || currentResult.success == 'false') {
                            errorRecords++;
                            progressModal.find(".sres-modal-message").append('<br>Error recalculating for ' + identifier);
                        }
                        progressModal.find('.sres-modal-progress .progress-bar').css('width', (completedRecords / totalRecords * 100) + '%');
                        if (completedRecords == totalRecords) {
                            progressModal
                                .find('.sres-modal-message').removeClass('d-none').append('<hr>Finished.').end()
                                .find('.modal-footer').removeClass('d-none').end();
                            progressModal.find(".sres-modal-progress .progress-bar").removeClass("progress-bar-striped active");
                            if (typeof oTable !== 'undefined') {
                                oTable.draw(false);
                            }
                        }
                    });
                } catch(e) {
                    //alert('An unexpected error occured during recalculation.')
                    errorRecords++;
                    progressModal.find(".sres-modal-message").append('<br>An unexpected error occured during recalculation.');
                }
			}
		});
	}
};
