// Chosens
$(document).ready(function(){
	$("#select_canvas_course_ids")
		.chosen({'width': '100%', search_contains: true});
	$(".sres-select-assignments")
		.chosen({'width': '100%', search_contains: true});
	$(".sres-select-quizzes")
		.chosen({'width': '100%', search_contains: true});
});

// Searcher
$(document).on('keyup', '#connection_filter', function(){
    var term = $(this).val();
    if (term) {
        $('.sres-card-container').each(function(){
            if ($(this).text().indexOf(term) !== -1) {
                $(this).removeClass('d-none');
            } else {
                $(this).addClass('d-none');
            }
        });
    } else {
        $('.sres-card-container').removeClass('d-none');
    }
});

// Force sync
$(document).on('change', '.sres-connection-configuration-card input, .sres-connection-configuration-card select', function(){
    console.log($(this));
    $(this).parents('.sres-connection-configuration-card')
        .find('.sres-refresh-now button[data-sres-connection-type]').prop('disabled', true).end()
        .find('.sres-refresh-now-warning').removeClass('d-none').end();
});
$(document).on('click', 'button[data-sres-connection-type]', function(){
    let spinner = $(this).find('.sres-refresh-now-spinner').addClass('spinning');
    window.setTimeout(function(){
        spinner.removeClass('spinning');
    }, 3000);
    $.ajax({
        url: ENV['FORCE_REFRESH_ENDPOINT'].replace('__connection_type__', $(this).attr('data-sres-connection-type')),
        method: 'GET',
        success: function(data){
            $.notify({message: 'Sync requested.'},{type: 'success'});
        },
        error: function(data){
            $.notify({message: 'Request failed.'},{type: 'danger'});
        }
    });
});

$(document).on('click', 'a.sres-action-buttons-jump', function(event){
   event.preventDefault();
   $('#update_connection')[0].scrollIntoView({behavior: 'smooth'});
   $('#update_connection').addClass('animated bounce infinite');
   window.setTimeout(function(){
       $('#update_connection').removeClass('animated bounce infinite');
   }, 3000);
});