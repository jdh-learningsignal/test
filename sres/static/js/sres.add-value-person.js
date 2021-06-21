ENV['FOCUS_SEARCH_STUDENT_INPUT_AFTER_SAVE'] = false;

/**
    Column filters
**/
$(document).on('input', '#element_search_term', function(){
    var term = $(this).val();
    console.log(term);
    $('.sres-column-data-container')
        .addClass('d-none')
        .each(function(){
            var text = $(this).clone().find('script').remove().end().find('style').remove().end();
            console.log(text.text());
            // TODO accommodate for input, textarea, button, dropdown etc values to allow searching by data
            if (text.text().toLowerCase().indexOf(term.toLowerCase()) !== -1) {
                $(this).removeClass('d-none');
            }
        }
    );
});
$(document).on('click', '#element_search_clear', function(){
    $('#element_search_term').val('').trigger('input');
});
$(document).on('click', '#element_collapse_all', function(){
    $('.sres-column-data-container-body').each(function(){
        $(this).collapse('hide');
    });
});
$(document).on('click', '#element_expand_all', function(){
    $('.sres-column-data-container-body').each(function(){
        $(this).collapse('show');
    });
});

/**
    System columns
**/
$(document).on('input', 'input.sres-system-column-data', function(){
    $(this).attr('data-sres-input-dirty', true);
});
$(document).on('click', '#save_system_column_data', function(){
    if (ENV['MODE'] == 'new') {
        var payload = {};
        $('input.sres-system-column-data').each(function(){
            payload[$(this).attr('data-sres-system-column-name')] = $(this).val();
        });
        $.ajax({
            url: ENV['ADD_STUDENT_ENDPOINT'],
            method: 'POST',
            data: payload,
            success: function(data){
                data = JSON.parse(data);
                console.log(data);
                if (data.success) {
                    $.notify({message:'Student added. Loading...'}, {type: 'success'});
                    window.location = ENV['VIEW_SINGLE_STUDENT_ENDPOINT'].replace('__identifier__', data.identifier);
                } else if (data.already_exists) {
                    $.notify({message:'Student with this SID already exists. Loading...'}, {type: 'warning'});
                    window.location = ENV['VIEW_SINGLE_STUDENT_ENDPOINT'].replace('__identifier__', data.identifier);
                }
            }
        });
    } else {
        $('input.sres-system-column-data[data-sres-input-dirty=true]').each(function(){
            var data = $(this).val();
            var systemColumnName = $(this).attr('data-sres-system-column-name');
            sendData(ENV['identifier'], data, null, ENV['table_uuid'], systemColumnName);
        });
    }
});
$(document).on('sres:datasaved', function(event, args) {
    $('input.sres-system-column-data[data-sres-input-dirty=true]').each(function(){
        if ($(this).attr('data-sres-system-column-name') == args.columnuuid) {
            $(this).removeAttr('data-sres-input-dirty');
        }
    });
});
