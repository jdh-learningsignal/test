$(document).on('click', '#new_api_key', function(){
    $.ajax({
        url: ENV['API_KEY_ENDPOINT'],
        method: 'POST',
        data: {
            description: $('#new_api_key_description').val()
        },
        success: function(data){
            data = JSON.parse(data);
            //console.log(data);
            let template = Handlebars.compile(document.getElementById("api_key_row_template").innerHTML);
            $('#api_keys_container').append(template({
                uuid: data.uuid,
                key: data.key,
                description: data.description
            }));
            $('#new_api_key_description').val('');
        }
    });
});

$(document).on('click', '.sres-api-key-delete', function(){
    let keyUuid = $(this).parents('.sres-api-key-row').attr('data-sres-key-uuid');
    $.ajax({
        url: ENV['API_KEY_ENDPOINT'],
        data: {
            uuid: keyUuid
        },
        method: 'DELETE',
        success: function(data){
            data = JSON.parse(data);
            //console.log(data);
            $(".sres-api-key-row[data-sres-key-uuid='" + data.uuid + "']").remove();
        }
    });
});

$(document).on('click', '.sres-selectall-on-click', function(){
    let range = document.createRange();
    range.selectNodeContents($(this).get(0));
    let sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
});

