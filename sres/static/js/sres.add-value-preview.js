/**
    Preview
**/
$(document).on('click', "#preview_identified_student", function() {
    window.location = ENV['COLUMN_SDA_PREVIEW_ENDPOINT_IDENTIFIED'].replace('__identifier__', encodeURIComponent($("#preview_identifier").val()));
});
$(document).on('click', "#preview_random_student", function() {
    window.location = ENV['COLUMN_SDA_PREVIEW_ENDPOINT_RANDOM'];
});
$(document).on('focus', "#preview_identifier", function() {
    $(this).select();
});
 $(document).on("keypress", "#preview_identifier", function(e) {
    if (e.which == 13) {
        $("#preview_identified_student").trigger("click");
        return false;
    }
});

/**
    Student search
**/
$(document).ready(function(){
    $("#preview_identifier").autocomplete(
        {
            minLength: 2,
            autoWidth: false/*,
            autoselect: true*/
        }, 
        {
            name: 'search-results',
            source: function(query, callback){
                $.ajaxq.abort('search-student-term');
                $.ajaxq('search-student-term', {
                    url: ENV['GET_STUDENTS_ENDPOINT'],
                    data: { search: query },
                    type: 'GET',
                    dataType: 'json',
                    success: function(data){
                        //console.log('search result', data);
                        callback(data['students'].map(function(x){
                            return x;
                        }));
                    }
                });
            },
            templates: {
                suggestion: function(suggestion, answer){
                    //console.log(suggestion, answer);
                    let displayName = suggestion.preferred_name + ' ' + suggestion.surname;
                    let sid = suggestion.display_sid;
                    let template = Handlebars.compile(document.getElementById("search_student_result_line_template").innerHTML);
                    return template({
                        sid: sid,
                        displayName: displayName
                    })
                },
                empty: function(){
                    return '<span>No students found</span>';
                }
            }
        }
    ).on('autocomplete:selected', function(event, suggestion, dataset, context) {
        //console.log(event, suggestion, dataset, context);
        $('#preview_identifier').val(suggestion.sid);
        $('#preview_identifier').autocomplete('val', suggestion.sid);
        $("#preview_identified_student").trigger("click");
    });
});
