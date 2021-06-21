function escapeRegExp(str) {
    return str.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
}

$(document).ready(function(){
    refreshTooltips();
});

/**
    Filter
**/
function loadFilterSentMessage(target){
    $('#interaction_details_container').html('Loading...');
    let source = document.getElementById("filter-message-template").innerHTML;
    let template = Handlebars.compile(source);
    $.ajax({
        url: ENV['GET_SENT_MESSAGES_ENDPOINT'].replace('__target__', target),
        method: 'GET',
        success: function(data){
            data = JSON.parse(data);
            $('#interaction_details_container').html('');
            data.forEach(function(record){
                let body = record.message.body.replace(new RegExp(escapeRegExp(record.log_uuid), 'g'), '');
                if (record.type == 'canvasinbox') {
                    body = body.replace(/\n/g, '<br>')
                }
                let context = {
                    loguuid: record.log_uuid,
                    target: record.target,
                    sent: record.sent,
                    subject: record.message.subject,
                    body: body,
                    type: record.type_display
                };
                $('#interaction_details_container').append(template(context));
            });
            $('#interaction_details_container').get(0).scrollIntoView({behavior: "smooth"});
            refreshTooltips();
        },
        error: function(err){
            $('#interaction_details_container').html('Error loading message.');
        }
    });
}
$(document).on('click', '[data-sres-target-fn=loadFilterSentMessage]', function(){
    loadFilterSentMessage($(this).attr('data-sres-target-identifier'));
});

/**
    Portal
**/
function previewPortalForStudent(target){
    let url = ENV['PREVIEW_PORTAL_URL'].replace('__identifier__', target);
    $('#interaction_details_container')
        .html('<a href="' + url + '" target="_blank">Open portal preview in a new tab</a><br><iframe src="' + url + '" class="sres-interaction-preview">Loading...</iframe>')
        .get(0).scrollIntoView({behavior: "smooth"});
}
$(document).on('click', '[data-sres-target-fn=previewPortalForStudent]', function(){
    previewPortalForStudent($(this).attr('data-sres-target-identifier'));
});
