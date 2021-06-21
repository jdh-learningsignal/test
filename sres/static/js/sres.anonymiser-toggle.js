$(document).on('click', '.sres-anonymiser-toggle', function(){
    let toggler = $(this);
    let anonymiserState = toggler.attr('data-sres-identity-anonymiser-active').toLowerCase() == 'true' ? true : false;
    if (!anonymiserState) {
        let alertHtml = '';
        alertHtml += '<div id="anonymiser_alert" class="modal" tabindex="-1">';
            alertHtml += '<div class="modal-dialog">';
                alertHtml += '<div class="modal-content">';
                    alertHtml += '<div class="modal-header">';
                        alertHtml += '<h5 class="modal-title">You are about to activate the anonymiser</h5>';
                        alertHtml += '<button type="button" class="close" data-dismiss="modal" aria-label="Close">';
                            alertHtml += '<span aria-hidden="true">&times;</span>';
                        alertHtml += '</button>';
                    alertHtml += '</div>';
                    alertHtml += '<div class="modal-body">';
                        alertHtml += '<p>Use the anonymiser to protect student identities when showing colleagues your SRES.</p>';
                        alertHtml += '<p>The anonymiser attempts to replace student names with pseudonyms, and scrambles or masks student identifiers such as SIDs, usernames, and emails. The names are derived from lists from <a href="https://www.behindthename.com/" target="_blank">www.behindthename.com</a>.</p>';
                        alertHtml += '<p class="font-weight-bold">Because SIDs are scrambled, do not attempt to save data when the anonymiser is active.</p>';
                        alertHtml += '<p>All care is taken with the anonymiser, but there may be some situations where the anonymiser is a bit \'leaky\' i.e. some identifiers may appear unintentionally.</p>';
                    alertHtml += '</div>';
                    alertHtml += '<div class="modal-footer">';
                        alertHtml += '<button type="button" class="btn btn-secondary" data-dismiss="modal">Do not activate</button>';
                        alertHtml += '<button type="button" class="btn btn-primary sres-anonymiser-confirm-activate"><span class="fa fa-user-secret"></span> Activate anonymiser</button>';
                    alertHtml += '</div>';
                alertHtml += '</div>';
            alertHtml += '</div>';
        alertHtml += '</div>';
        $('body').append(alertHtml);
        $('#anonymiser_alert').modal('show');
        $('#anonymiser_alert').on('hidden.bs.modal', function(){
            $('#anonymiser_alert').remove();
        });
        $('#anonymiser_alert .sres-anonymiser-confirm-activate').on('click', function(){
            $(document).trigger('sres:anonymisertoggle');
            $('#anonymiser_alert').modal('hide');
        });
    } else {
        // Anonymiser currently active, just trigger the toggle
        $(document).trigger('sres:anonymisertoggle');
    }
});
$(document).on('sres:anonymisertoggle', function(){
    let toggler = $('.sres-anonymiser-toggle[data-sres-identity-anonymiser-active]');
    $.ajax({
        url: ENV['ANONYMISER_TOGGLE_ENDPOINT'],
        method: 'PUT',
        success: function(data){
            data = JSON.parse(data);
            toggler.attr('data-sres-identity-anonymiser-active', data.anonymiser_active);
            $(document).trigger('sres:anonymiserchanged');
            if (data.anonymiser_active) {
                $.notify( {message:'Anonymiser activated. You may need to refresh the page. <a href="javascript:window.location.reload();">Refresh now</a>.'}, {type: 'info'} );
            } else {
                $.notify( {message:'Anonymiser deactivated. You may need to refresh the page. <a href="javascript:window.location.reload();">Refresh now</a>.'}, {type: 'info'} );
            }
        }
    });
});
$(document).on('sres:anonymiserchanged', function(){
    let toggler = $('.sres-anonymiser-toggle[data-sres-identity-anonymiser-active]');
    let anonymiserState = toggler.attr('data-sres-identity-anonymiser-active').toLowerCase() == 'true' ? true : false;
    let hint = '';
    if (anonymiserState) {
        toggler.addClass('text-danger');
        hint = 'Anonymiser is active - click to deactivate anonymiser';
    } else {
        toggler.removeClass('text-danger');
        hint = 'Click to activate anonymiser to hide student identities';
    }
    toggler.attr('aria-label', hint);
    //toggler.attr('title', hint);
    toggler.attr('data-tippy-content', hint);
    refreshTooltips();
});
$(document).ready(function(){
    $(document).trigger('sres:anonymiserchanged');
});