$(document).ready(function(){
    $('#search').trigger('click');
});

$(document).on('click', '#search', function(){
    let searchTerm = $('#search_term').val();
    let mineOnly = '';
    if ($('input:checked[name=show_only_my_shared_assets]').length > 0) {
        mineOnly = '&mine_only=1';
    }
    $.ajax({
        url: ENV['SEARCH_COLLECTIVE_ENDPOINT'] + '?term=' + searchTerm + '&' + $('input:checked[name=search_asset_types]').serialize() + mineOnly,
        method: 'GET',
        success: function(data){
            data = JSON.parse(data);
            let template = Handlebars.compile(document.getElementById("collective_asset_template").innerHTML);
            $('#collective_assets_container').html('');
            if (data.length) {
                data.forEach(function(asset){
                    $('#collective_assets_container').append(template({
                        asset_uuid: asset['uuid'],
                        asset_icon: asset['icon'],
                        display_type: asset['display_type'],
                        asset_type: asset['type'].charAt(0).toUpperCase() + asset['type'].slice(1),
                        name: asset['name'],
                        secret: asset['visibility'] == 'secret' ? true : false,
                        description: asset['description'],
                        url: ENV['SHOW_ASSET_ENDPOINT'].replace('__asset_uuid__', asset['uuid']),
                        preview_url: asset['type'] == 'column' ? ENV['PREVIEW_ASSET_ENDPOINT'].replace('__asset_uuid__', asset['uuid']) : '',
                        shared_by: asset['shared_by'].join(', '),
                        shared_on: moment(asset['shared_on']).fromNow(),
                        liked_by: asset['liked_by'].length,
                        liked_by_me: asset['liked_by_me']
                    }));
                });
            } else {
                $('#collective_assets_container').html('Sorry, no results.');
            }
        }
    });
});

$(document).on('click', '.sres-collective-asset-like', function(){
    let assetUuid = $(this).attr('data-sres-asset-uuid');
    $.ajax({
        url: ENV['LIKE_ASSET_ENDPOINT'].replace('__asset_uuid__', assetUuid),
        method: 'PUT',
        success: function(data){
            data = JSON.parse(data);
            $('.sres-collective-asset-like-count[data-sres-asset-uuid=' + assetUuid + ']').html(data.count);
            if (data.liked_by_me) {
                $('.sres-collective-asset-like[data-sres-asset-uuid=' + assetUuid + '] .fa-heart').addClass('text-danger');
            } else {
                $('.sres-collective-asset-like[data-sres-asset-uuid=' + assetUuid + '] .fa-heart').removeClass('text-danger');
            }
        }
    });
});
