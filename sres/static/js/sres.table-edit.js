$(document).ready(function(){
    $('.sres-select-user-chosen').chosen({
        width: '100%',
        no_results_text: 'Please use the buttons to add users', 
        placeholder_text_multiple: 'Please use the buttons to add users'
    });
});

/** Enrollment lists **/
$(document).on('change', 'input[name=chkRepopulate]', function(){
    console.log($(this).prop('checked'));
    if ($(this).prop('checked')) {
        $('#populate_student_list_from_container')
            .find('input, button, textarea, select').prop('disabled', false);
    } else {
        $('#populate_student_list_from_container')
            .find('input, button, textarea, select').prop('disabled', true);
    }
});
$(document).ready(function(){
    $('input[name=chkRepopulate]').trigger('change');
    $('#populate_student_list_from').trigger('change');
});
$(document).on('change', '#populate_student_list_from', function() {
    $("#populate_student_list_from_container > div").hide();
    $("#populate_student_list_from_container > div[class~=" + $(this).val() + "]").show();
    $("#autoListFiles").prop("required", false);
    //console.log($(this).val());
    switch ($(this).val()) {
        case "autoList":
            $("#autoListFiles").prop("required", true);
            break;
    }
});
// autoList pre-processing/mapping
$(document).on('change', "#autoListFiles", function(){
    $("#autoListPreProcess").trigger('click');
});
$(document).on('click', "#autoListPreProcess", function(){
    if (!$("#autoListFiles")[0].files.length) {
        alert('At least one file must be selected');
        return false;
    }
    // request mapping from user
    for (var i = 0, f; f = document.getElementById('autoListFiles').files[i]; i++){
        //console.log(f);
        $("#modal_autoList_mapping_filename").html(f.name);
        Papa.parse(f, {
            header: true,
            complete: function(results) {
                //console.log(results);
                // Add map to modal
                $("select[id^=modal_autoList_mapping_map_]").html('');
                autoListFieldsToMap.forEach(function(fieldToMap){
                    if (!fieldToMap.required) {
                        //Add default option, which will mark field for no import if no like found
                        $("select[id=modal_autoList_mapping_map_" + fieldToMap.name + "]").append('<option value="NOIMPORT" selected="selected">(Ignore, do not import)</option>');
                    }
                    if (fieldToMap.show_secondary_field) {
                        $("select[id=modal_autoList_mapping_map_" + fieldToMap.name + "_secondary]").append('<option value="NOIMPORT" selected="selected">(No secondary field specified)</option>');
                    }
                    // try guess the SID smartly
                    let sidRegex = /[A-z0-9a-z]{4,}/;
                    let sidFieldRegex = /\b(s|student)?[ _]*id\b/i;
                    let probableSIDField = {
                        found: false,
                        indicatorFlagsCount: 0,
                        fieldName: undefined
                    };
                    try {
                        if (fieldToMap.name == 'sid') {
                            results.meta.fields.forEach(function(inputFieldName){
                                let indicatorFlagsCount = 0;
                                let recordsWithUnlikelyData = 0;
                                let totalRecordCount = results.data.length;
                                // look at the name of this input field
                                if (sidFieldRegex.test(inputFieldName)) {
                                    // name checks out
                                    indicatorFlagsCount++;
                                }
                                // look at data for this input field
                                let previousData = '';
                                let allData = [];
                                results.data.forEach(function(row, index){
                                    let thisRecordUnlikely = false;
                                    if (row[inputFieldName]) {
                                        // see if it's a simple text
                                        if (!sidRegex.test(row[inputFieldName])) {
                                            // unlikely because the data is too complex
                                            thisRecordUnlikely = true;
                                        }
                                        // see if it's the same length as the one before it
                                        if (index > 0) {
                                            if (row[inputFieldName].length != previousData.length) {
                                                // unlikely due to length mismatch with the one before it
                                                thisRecordUnlikely = true;
                                            }
                                            previousData = row[inputFieldName];
                                        }
                                        // see if it's different
                                        if (index > 0) {
                                            if (allData.indexOf(row[inputFieldName]) > -1) {
                                                // unlikely due to existing already
                                                thisRecordUnlikely = true;
                                            }
                                        }
                                        allData.push(row[inputFieldName]);
                                    }
                                    if (thisRecordUnlikely) {
                                        recordsWithUnlikelyData++;
                                    }
                                });
                                // evaluate
                                if ((totalRecordCount > 0) && (recordsWithUnlikelyData / totalRecordCount < 0.1)) {
                                    indicatorFlagsCount++;
                                }
                                //
                                if (indicatorFlagsCount > 0 && indicatorFlagsCount > probableSIDField.indicatorFlagsCount) {
                                    probableSIDField.indicatorFlagsCount = indicatorFlagsCount;
                                    probableSIDField.fieldName = inputFieldName;
                                    probableSIDField.found = true;
                                }
                                //console.log(inputFieldName, indicatorFlagsCount, recordsWithUnlikelyData, totalRecordCount, probableSIDField);
                            });
                        }
                    } catch(e) {
                        console.error('Could not guess SID field', e);
                    }
                    // try guess others and add to dropdown mapper
                    results.meta.fields.forEach(function(inputFieldName){
                        // Try and guess a match
                        let optionSelected = false;
                        fieldToMap.like.forEach(function(fieldLike){
                            if (inputFieldName.toLowerCase().replace(/[^a-z0-9]/,'').trim() == fieldLike.trim()) {
                                optionSelected = true;
                            }
                        });
                        // special case for unique ID
                        if (fieldToMap.name == 'sid' && probableSIDField.found && probableSIDField.fieldName == inputFieldName) {
                            optionSelected = true;
                        }
                        // Add to the dropdown
                        let optionTemplate = Handlebars.compile(document.getElementById("populate_student_list_mapper_option").innerHTML);
                        let optionHtml = optionTemplate({
                            fieldName: inputFieldName,
                            optionSelected: optionSelected,
                        });
                        $("select[id=modal_autoList_mapping_map_" + fieldToMap.name + "]").append(optionHtml);
                        if (fieldToMap.show_secondary_field) {
                            $("select[id=modal_autoList_mapping_map_" + fieldToMap.name + "_secondary]").append(optionHtml);
                        }
                    });
                });
                // Show modal
                $("#modal_autoList_mapping_filename").attr('data-delimiter', results.meta.delimiter);
                $("#modal_autoList_mapping").modal('show');
            }
        });
    }
});
$(document).on('click', "#modal_autoList_mapping_confirm", function(){
    // clear existing map
    $("#autoList_mapping_map").val('');
    // Build new map
    var mappingMap = {};
    var filename = $("#modal_autoList_mapping_filename").text().replace(/[^A-Z0-9a-z_]/g, '_');
    mappingMap[filename] = {
        delimiter: $("#modal_autoList_mapping_filename").attr('data-delimiter'),
        map: {}
    };
    autoListFieldsToMap.forEach(function(fieldToMap){
        mappingMap[filename]['map'][fieldToMap.name] = {
            field: $("select[id=modal_autoList_mapping_map_" + fieldToMap.name + "]").val()
        };
        if (fieldToMap.show_secondary_field) {
            mappingMap[filename]['map'][fieldToMap.name]['secondary_field'] = $("select[id=modal_autoList_mapping_map_" + fieldToMap.name + "_secondary]").val();
        }
    });
    //console.log(mappingMap);
    $("#autoList_mapping_map").val(JSON.stringify(mappingMap));
    $("#modal_autoList_mapping").modal('hide');
    $("#autoListPreProcess span.fa-check").removeClass('d-none');
});



$(document).on('submit', 'form[id=editStudentList]', function(event) {
    // If using DSV file and actually populating enrolments, check that mapping has been set up
    if ($("#populate_student_list_from").val() == 'autoList' && $("#chkRepopulate").prop('checked') && $("#autoList_mapping_map").val() == '') {
        alert('When using a custom comma or tab separated file, the header mapping must be set up before proceeding.');
        event.preventDefault;
        return false;
    }
    $('#submitButton').attr('disabled', true);
    // Disable submit button for a while
    window.setTimeout(reenable_submit, 3000);
    function reenable_submit() {
        $('#submitButton')
            .removeAttr('disabled')
            .val('Save');
    };
});


/** 
    Duplicate list warnings
**/
var alreadyCheckedForListExists = false;
function checkIfListExists() {
    alreadyCheckedForListExists = true;
    if ($("#uoscode").val() && $("#theyear").val() && $("#thesemester").val()) {
        $.ajax({
            url: ENV['FIND_TABLE_ENDPOINT'],
            method: 'GET',
            data: {
                year: $("#theyear").val(),
                semester: $("#thesemester").val(), 
                code: $("#uoscode").val()
            },
            success: function(data) {
                data = JSON.parse(data);
                //console.log(data);
                var otherUnits = [];
                if (typeof data.length != 'undefined' && data.length > 0) {
                    data.forEach(function(uos){
                        if (uos.uuid != ENV['table_uuid']) {
                            otherUnits.push(uos);
                        }
                    });
                    if (otherUnits.length > 0) {
                        $("#modal_duplicate_list_warning_list").html('');
                        otherUnits.forEach(function(uos){
                            //console.log(uos);
                            let message = '<div class="mb-2">';
                            message += '<a href="' + ENV['VIEW_TABLE_ENDPOINT'].replace('__table_uuid__', uos.uuid) + '" target="_blank">';
                            message += Handlebars.escapeExpression(uos.code) + ' ' + Handlebars.escapeExpression(uos.name) + ' ' + Handlebars.escapeExpression(uos.year) + ' semester ' + Handlebars.escapeExpression(uos.semester);
                            message += '</a>';
                            if (uos.authorised == 0) {
                                message += ' <span class="fa fa-lock ml-2" aria-label="You are not on the list of authorised users" data-tippy-content="You are not on the list of authorised users"></span>';
                            }
                            let contactHint = "Email list contact, " + Handlebars.escapeExpression(uos.contact.name);
                            message += '<a class="ml-2" href="mailto:' + Handlebars.escapeExpression(uos.contact.email) + '" data-tippy-content="' + contactHint + '" aria-label="' + contactHint + '"><span class="fa fa-user-shield" aria-hidden="true"></span></a>';
                            message += '</div>';
                            //console.log(message);
                            $("#modal_duplicate_list_warning_list").append(message);
                        });
                        refreshTooltips();
                        $("#modal_duplicate_list_warning").modal('show');
                    }
                }
            }
        });
    }
}
$(document).on('change', '#uoscode, #theyear, #thesemester', function() {
    alreadyCheckedForListExists = false;
    checkIfListExists();
});
$(document).ready(function(){
    setInterval(
        function(){
            if (!alreadyCheckedForListExists) checkIfListExists();
        },
        1000
    );
});

