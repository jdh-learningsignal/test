Handlebars.registerHelper('ifEquals', function(arg1, arg2, options) {
    return (arg1 == arg2) ? options.fn(this) : options.inverse(this);
});

const LOADING_SPAN_HTML = '<span><span class="fa fa-sync-alt spinning" aria-hidden="true"></span> Loading...</span>';
const LOADING_DIV_HTML = '<div class="col-sm-12">' + LOADING_SPAN_HTML + '</div>';

/**
    Dashboard control
**/
function indexShowTab(hash){
    $(hash + '-tab').trigger('shown.bs.tab');
    $(".nav-link[role=tab][href='" + hash + "']").tab('show');
}
$(document).on('click', 'a.sres-tab-trigger', function(){
    let hash = $(this).attr('href');
    indexShowTab(hash);
});
$(document).ready(function(){
    let hash = window.location.hash ? window.location.hash : "#dashboard";
    indexShowTab(hash);
});
$(document).on('click', 'a.nav-link[data-toggle=pill]', function(event){
    console.log(event);
    if (event.ctrlKey) {
        window.open(event.target.href).focus();
        event.preventDefault();
        return false;
    }
});
$(document).on('shown.bs.tab', '[role=tab]', function(event){
    //console.log('shown.bs.tab', event.target);
    let id = $(this).attr('aria-controls');
    history.pushState(null, null, '#' + id);
    switch (id) {
        case 'dashboard':
            loadRecentAssets(null, 'dashboard');
            loadPinnedAssets();
            $('#dashboard_intelligence_container').html(LOADING_SPAN_HTML);
            (async function(){
                loadRecentFeedback().finally(function(){
                    $('#dashboard_intelligence_carousel .carousel-item').first().addClass('active');
                    refreshTooltips();
                    loadRecentNotables().finally(function(){
                        if ($('#dashboard_intelligence_carousel .carousel-item').length == 0) {
                            let msg = '<p>There\'s nothing of note here for now.</p>';
                            msg += '<p>You may want to <a href="#lists" class="sres-tab-trigger">create a new list</a> to capture, curate, and work with student data. ';
                            if (ENV['SRES_USER']['admin-filter']) {
                                msg += 'Then you can use <a href="#filters" class="sres-tab-trigger">a filter</a> to send personalised messages to students.';
                            }
                            msg += '</p>';
                            msg += '<p>SRES can also watch the data in a list that you pin to this dashboard and let you know if there may be something that needs your attention - just click the thumbtack icon for any <a href="#lists" class="sres-tab-trigger">list</a>.</p>'
                            $('#dashboard_intelligence_container').html(msg);
                        }
                        refreshTooltips();
                    });
                });
            })();
            break;
        case 'lists':
            loadDataTableLists();
            loadRecentAssets('table');
            break;
        case 'filters':
            loadDataTableFilters();
            loadRecentAssets('filter');
            break;
        case 'portals':
            loadDataTablePortals();
            loadRecentAssets('portal');
            break;
        case 'insights':
            loadDataTableInsights();
            break;
        case 'collective':
            loadCollective();
            break;
        case 'tags':
            break;
        case 'data_entry':
            loadDataEntry();
            break;
        case 'search':
            $('input:checkbox[name=search_sres_haystack_types]').bootstrapToggle('destroy').bootstrapToggle().trigger('change'); // need to trigger it here otherwise width and height are off
            $('#search_sres_term').focus();
            $('input:checkbox[name=search_sres_haystack_types]').each(function(){
                $(this).siblings('.toggle-group').attr('data-tippy-content', $(this).attr('data-tippy-content'));
            });
            break;
    }
    refreshTooltips();
});
$(document).on('hidden.bs.tab', '[role=tab]', function(event){
    //console.log('hidden.bs.tab', event.target);
    let id = $(this).attr('aria-controls');
});

/**
    General things for all asset tables
**/
$(document).on('draw.dt', '.sres-asset-table', function(event){
    refreshTooltips();
});
var assetDataTables = {};

/**
    Pinning
**/
$(document).on('click', '.sres-dashboard-pin-asset, .sres-dashboard-unpin-asset', function(){
    let assetType = $(this).attr('data-sres-asset-type');
    let assetUuid = $(this).attr('data-sres-asset-uuid');
    let url = ENV['PIN_TO_DASHBOARD_ENDPOINT'].replace('__asset_type__', assetType).replace('__asset_uuid__', assetUuid);
    let method = null;
    if ($(this).hasClass('sres-dashboard-pin-asset')) {
        method = 'POST';
    } else if ($(this).hasClass('sres-dashboard-unpin-asset')) {
        method = 'DELETE';
    }
    $.ajax({
        url: url,
        method: method,
        success: function(data){
            data = JSON.parse(data);
            console.log('success', data);
            if (data.success) {
                if (method == 'POST') {
                    switch (data.action) {
                        case 'to_first':
                            $.notify({message:'Already pinned, so we moved this to the top of your pinned stack'}, {type: 'success'});
                            break;
                        case 'pinned':
                            $.notify({message:'Pinned to dashboard successfully'}, {type: 'success'});
                            break;
                    }
                } else if (method == 'DELETE') {
                    $.notify({message:'Unpinned from dashboard successfully'}, {type: 'success'});
                }
                loadPinnedAssets();
            } else {
                $.notify({message:'Error updating pin state.'}, {type: 'danger'});
            }
        },
        error: function(data){
            //console.log('error', data);
            $.notify({message:'Error updating pin state.'}, {type: 'danger'});
        }
    });
    
});
async function fetchPinnedAssets() {
    let response = await fetch(ENV['GET_PINNED_ASSETS_ENDPOINT'].replace('__asset_type__', ''), {
        method: 'GET',
        credentials: 'same-origin'
    });
    let data = await response.json();
    return data.pinned;
}
function loadPinnedAssets() {
    if (!ENV['IS_USER_ADMIN_SOMEWHERE']) {
        return;
    }
    $('#dashboard_pinned_assets_container').html(LOADING_DIV_HTML);
    fetchPinnedAssets().then(function(data){
        renderRecentsCards(
            data, 
            '#dashboard_pinned_assets_container', 
            50, 
            true, 
            'dashboard_recent_asset_card_template', 
            'pinned',
            'Nothing has been pinned yet - click a thumbtack icon to pin something here for easy access.'
        );
    });
}

/**
    Recent notables
**/
async function fetchRecentNotables(){
    let response = await fetch(ENV['GET_DASHBOARD_NOTABLES_ENDPOINT'], {
        method: 'GET',
        credentials: 'same-origin'
    });
    let data = await response.json();
    //console.log('data', data);
    return data.notables;
}
async function loadRecentNotables(){
    
    if (!ENV['IS_USER_ADMIN_SOMEWHERE']) {
        return;
    }
    
    let template = Handlebars.compile(document.getElementById("dashboard_intelligence_card_template").innerHTML);
    
    return await fetchRecentNotables().then(function(data){
        //console.log('notables', data);
        if (data.length) {
            data.forEach(function(record){
                templateConfig = {};
                //templateConfig.firstOne = $('#dashboard_intelligence_carousel .carousel-item.active').length == 0 ? true : false;
                templateConfig.title = record.table_name;
                templateConfig.titleIcon = 'info-circle';
                templateConfig.recordType = record.reason;
                templateConfig.subtitle = record.column_name;
                templateConfig.message = record.explanation;
                templateConfig.buttons = record.buttons ? record.buttons : [];
                $('#dashboard_intelligence_container').append(template(templateConfig));
            });
        }
    });
}
$(document).on('click', '.sres-dashboard-intelligence-carousel-next, .sres-dashboard-intelligence-carousel-prev', function(){
    if ($(this).hasClass('sres-dashboard-intelligence-carousel-next')) {
        $('#dashboard_intelligence_carousel').carousel('next');
    } else {
        $('#dashboard_intelligence_carousel').carousel('prev');
        
    }
});
$(document).on('click', '.sres-dashboard-intelligence-carousel-pause', function(){
    if ($(this).attr('data-sres-state') == 'cycle') {
        $('#dashboard_intelligence_carousel').carousel('pause');
        $(this).attr('data-sres-state', 'pause');
        $(this).html('<span class="fa fa-play"></span>');
        $(this).attr('aria-label', 'Resume');
    } else {
        $('#dashboard_intelligence_carousel').carousel('cycle');
        $(this).attr('data-sres-state', 'cycle');
        $(this).html('<span class="fa fa-pause"></span>');
        $(this).attr('aria-label', 'Pause');
    }
});

/**
    Recent feedback
**/
async function fetchRecentFeedback(){
    let response = await fetch(ENV['GET_RECENT_FEEDBACK_ENDPOINT'], {
        method: 'GET',
        credentials: 'same-origin'
    });
    let data = await response.json();
    //console.log('data', data);
    return data.recents;
}
async function loadRecentFeedback(){
    
    if (!ENV['IS_USER_ADMIN_SOMEWHERE']) {
        return;
    }
    
    return await fetchRecentFeedback().then(function(data){
        //console.log('datadata', data);
        
        $('#dashboard_intelligence_container').html('');
        
        let template = Handlebars.compile(document.getElementById("dashboard_intelligence_card_template").innerHTML);
        
        if (data.length) {
            data.forEach(function(record){
                templateConfig = {};
                switch (record.type) {
                    case 'positive_comments':
                    case 'many_negative_votes':
                        // buttons
                        switch (record.asset_type) {
                            case 'filter':
                                templateConfig.buttons = [
                                    {
                                        url: ENV['FILTER_VIEW_LOGS_ENDPOINT'].replace('__filter_uuid__', record.asset_uuid),
                                        linkText: 'View filter logs',
                                        icon: 'history'
                                    },
                                    {
                                        url: ENV['FILTER_CLONE_ENDPOINT'].replace('__filter_uuid__', record.asset_uuid),
                                        linkText: 'Clone this filter',
                                        icon: 'clone'
                                    }
                                ]
                                break;
                            case 'portal':
                                templateConfig.buttons = [
                                    {
                                        url: ENV['PORTAL_VIEW_LOG_ENDPOINT'].replace('__portal_uuid__', record.asset_uuid),
                                        linkText: 'View portal logs',
                                        icon: 'history'
                                    },
                                    {
                                        url: ENV['PORTAL_EDIT_ENDPOINT'].replace('__portal_uuid__', record.asset_uuid),
                                        linkText: 'Edit this portal',
                                        icon: 'pen'
                                    }
                                ]
                                break;
                        }
                        templateConfig.recordType = record.type;
                        // positive comments
                        if (record.type == 'positive_comments') {
                            templateConfig.title = "Students appreciated your " + record.asset_type + "  " + record.asset_name;
                            templateConfig.titleIcon = 'star';
                            if (record.recent_positive_feedback) {
                                let positiveComments = [];
                                for (let c = 0; c < record.recent_positive_feedback.length; c++) {
                                    let comment = record.recent_positive_feedback[c];
                                    positiveComments.push({
                                        commentFull: comment,
                                        commentShort: (comment.length > 200 ? comment.substring(0, 200) + "..." : comment)
                                    });
                                }
                                templateConfig.positiveComments = positiveComments;
                            }
                        } else if (record.type == 'many_negative_votes') {
                            templateConfig.title = "Some students are not appreciating the " + record.asset_type + "  " + record.asset_name;
                            templateConfig.titleIcon = 'exclamation-triangle';
                            templateConfig.message = record.percentage_negative_votes + '% of votes were negative for this ' + record.asset_type + ' in the last ' + record.days + ' days.';
                        }
                        break;
                }
                $('#dashboard_intelligence_container').append(template(templateConfig));
            });
        } else {
           // $('#dashboard_intelligence_container').html("We couldn't find anything to show for now.");
        }
    });
}

/**
    Recent asset accesses
**/
async function fetchRecentAssets(assetType){
    let response = await fetch(ENV['GET_RECENT_ACCESSES_ENDPOINT'].replace('__asset_type__', assetType), {
        method: 'GET',
        credentials: 'same-origin'
    });
    let data = await response.json();
    return data.recents;
}
function loadRecentAssets(assetType, renderTarget, resolveCallback) {
    
    if (!ENV['IS_USER_ADMIN_SOMEWHERE']) {
        return;
    }
    
    let targetContainerSelector = '';
    
    if (renderTarget === 'dashboard') {
        targetContainerSelector = '#dashboard_recents_container';
        $(targetContainerSelector).html(LOADING_DIV_HTML);
        let allData = [];
        fetchRecentAssets('table').then(function(data){
            allData = allData.concat(data);
            fetchRecentAssets('column').then(function(data){
                allData = allData.concat(data);
                fetchRecentAssets('filter').then(function(data){
                    allData = allData.concat(data);
                    fetchRecentAssets('portal').then(function(data){
                        allData = allData.concat(data);
                        // sort
                        allData.sort((a, b) => moment(a.most_recent_timestamp) - moment(b.most_recent_timestamp))
                        allData.reverse();
                        // display
                        renderRecentsCards(
                            allData, 
                            targetContainerSelector, 
                            20, 
                            true, 
                            'dashboard_recent_asset_card_template', 
                            'unpinned',
                            'Nothing here yet - access a few things in SRES and this will automatically populate.'
                        );
                    });
                });
            });
        });
    } else {
        switch (assetType) {
            case 'table':
                targetContainerSelector = '#recent_lists_container';
                break;
            case 'filter':
                targetContainerSelector = '#recent_filters_container';
                break;
            case 'portal':
                targetContainerSelector = '#recent_portals_container';
                break;
        }
        $(targetContainerSelector).html(LOADING_DIV_HTML);
        fetchRecentAssets(assetType).then(function(data){
            renderRecentsCards(
                data,
                targetContainerSelector,
                4,
                undefined,
                undefined,
                'unpinned',
                'Nothing here yet.'
            );
        });
    }
}
function renderRecentsCards(recentsData, targetContainerSelector, cardsToShow, onlyShowTitle, handlebarsTemplateId, assetPinState, messageIfEmpty) {    
    
    let template = null;
    if (typeof handlebarsTemplateId === 'undefined') {
        template = Handlebars.compile(document.getElementById("recent_asset_card_template").innerHTML);
    } else {
        template = Handlebars.compile(document.getElementById(handlebarsTemplateId).innerHTML);
    }
    
    if (recentsData.length == 0) {
        if (typeof messageIfEmpty === 'undefined') {
            $(targetContainerSelector).html('<div class="col-sm-12">Nothing found.</div>');
        } else {
            $(targetContainerSelector).html('<div class="col-sm-12">' + messageIfEmpty + '</div>');
        }
        return;
    }
    
    $(targetContainerSelector).html('');
    
    for (let i = 0; i < Math.min(cardsToShow, recentsData.length); i++) {
        
        let recentAsset = recentsData[i];
        let cardHtml = '';
        let templateConfig = {};
        
        if (typeof recentAsset.asset_uuid !== 'undefined') {
            templateConfig.assetUuid = recentAsset.asset_uuid;
        }
        
        if (typeof onlyShowTitle !== 'undefined' && onlyShowTitle) {
            templateConfig.onlyShowTitle = true;
        }
        
        if (typeof assetPinState !== 'undefined') {
            switch (assetPinState) {
                case 'pinned':
                    templateConfig.canBeUnpinned = true;
                    break;
                case 'unpinned':
                    templateConfig.canBePinned = true;
                    break;
            }
        }
        
        if (typeof recentAsset.most_recent_timestamp !== 'undefined') {
            templateConfig.lastAccessed = 'Last accessed ' + moment(recentAsset.most_recent_timestamp).fromNow();
        }
        
        if (typeof recentAsset.workflow_state !== 'undefined') {
            templateConfig.assetWorkflowState = recentAsset.workflow_state;
        }
        
        switch (recentAsset.asset_type) {
            case 'table':
                templateConfig.assetIsTable = true;
                templateConfig.assetType = 'table';
                templateConfig.title = recentAsset.title;
                templateConfig.subtitle = recentAsset.subtitle;
                templateConfig.activeColumnsCount = recentAsset.active_columns_count;
                templateConfig.activeStudentsCount = recentAsset.unique_identifiers_with_change_history;
                templateConfig.buttons = [
                    {
                        url: ENV['TABLE_VIEW_ENDPOINT'].replace('__table_uuid__', recentAsset.asset_uuid),
                        linkText: 'View list',
                        icon: 'table'
                    }
                ];
                break;
            case 'column':
                templateConfig.assetIsTable = true;
                templateConfig.assetType = 'column';
                templateConfig.title = recentAsset.title;
                templateConfig.subtitle = recentAsset.subtitle;
                templateConfig.buttons = [
                    {
                        url: ENV['COLUMN_EDIT_ENDPOINT'].replace('__table_uuid__', recentAsset.related_asset_uuid).replace('__column_uuid__', recentAsset.asset_uuid),
                        linkText: 'Edit column',
                        icon: 'pen'
                    },
                    {
                        url: ENV['COLUMN_SHOW_QUICK_ACCESS_ENDPOINT'].replace('__table_uuid__', recentAsset.related_asset_uuid).replace('__column_uuid__', recentAsset.asset_uuid),
                        linkText: 'Quick access links',
                        icon: 'bolt'
                    }
                ];
                break;
            case 'filter':
                templateConfig.assetIsFilter = true;
                templateConfig.assetType = 'filter';
                templateConfig.title = recentAsset.title;
                templateConfig.subtitle = recentAsset.subtitle;
                if (recentAsset.filter_sent) {
                    templateConfig.assetIsFilterSent = true;
                    templateConfig.buttons = [
                        {
                            url: ENV['FILTER_VIEW_LOGS_ENDPOINT'].replace('__filter_uuid__', recentAsset.asset_uuid),
                            linkText: 'View logs',
                            icon: 'history'
                        }
                    ]
                    templateConfig.countRecipients = recentAsset.count_recipients;
                    templateConfig.percentOpens = recentAsset.percent_opens;
                    templateConfig.feedbackSummary = recentAsset.feedback_summary;
                    if (recentAsset.feedback_recent_comments) {
                        let filterRecentComments = [];
                        for (let c = 0; c < recentAsset.feedback_recent_comments.length; c++) {
                            let comment = recentAsset.feedback_recent_comments[c];
                            filterRecentComments.push({
                                commentFull: comment,
                                commentShort: (comment.length > 200 ? comment.substring(0, 200) + "..." : comment)
                            });
                        }
                        templateConfig.filterRecentComments = filterRecentComments;
                    }
                } else {
                    templateConfig.buttons = [
                        {
                            url: ENV['FILTER_EDIT_ENDPOINT'].replace('__filter_uuid__', recentAsset.asset_uuid),
                            linkText: 'Edit filter',
                            icon: 'pen'
                        },
                        {
                            url: ENV['FILTER_PREVIEW_ENDPOINT'].replace('__filter_uuid__', recentAsset.asset_uuid),
                            linkText: 'Preview filter',
                            icon: 'play'
                        }
                    ]
                }
                break;
            case 'portal':
                templateConfig.assetIsPortal = true;
                templateConfig.assetType = 'portal';
                templateConfig.title = recentAsset.title;
                templateConfig.subtitle = recentAsset.subtitle;
                if (typeof recentAsset.availability !== 'undefined') {
                    templateConfig.available = recentAsset.availability.available;
                    templateConfig.availabilityComments = recentAsset.availability.messages.map(function(x){return x[0]}).join(' ');
                }
                templateConfig.studentsOpened = recentAsset.students_opened;
                templateConfig.timesOpened = recentAsset.times_opened;
                templateConfig.buttons = [
                    {
                        url: ENV['PORTAL_EDIT_ENDPOINT'].replace('__portal_uuid__', recentAsset.asset_uuid),
                        linkText: 'Edit portal',
                        icon: 'pen'
                    },
                    {
                        url: ENV['PORTAL_PREVIEW_ENDPOINT'].replace('__portal_uuid__', recentAsset.asset_uuid),
                        linkText: 'Preview portal',
                        icon: 'eye'
                    }
                ]
                if (templateConfig.timesOpened) {
                    templateConfig.buttons.push(
                        {
                            url: ENV['PORTAL_VIEW_LOG_ENDPOINT'].replace('__portal_uuid__', recentAsset.asset_uuid),
                            linkText: 'View logs',
                            icon: 'history'
                        }
                    )
                }
                templateConfig.feedbackSummary = recentAsset.feedback_summary;
                if (recentAsset.feedback_recent_comments) {
                    let filterRecentComments = [];
                    for (let c = 0; c < recentAsset.feedback_recent_comments.length; c++) {
                        let comment = recentAsset.feedback_recent_comments[c];
                        filterRecentComments.push({
                            commentFull: comment,
                            commentShort: (comment.length > 200 ? comment.substring(0, 200) + "..." : comment)
                        });
                    }
                    templateConfig.filterRecentComments = filterRecentComments;
                }
                break;
            case 'insight':
                templateConfig.assetType = 'insight';
                templateConfig.title = recentAsset.title;
                templateConfig.subtitle = recentAsset.subtitle;
                templateConfig.buttons = [
                    {
                        url: ENV['INSIGHT_EDIT_ENDPOINT'].replace('__insight_uuid__', recentAsset.asset_uuid),
                        linkText: 'Edit insight',
                        icon: 'pen'
                    }
                ];
                break;
        }
        // add card html to container
        cardHtml = template(templateConfig);
        $(targetContainerSelector).append(cardHtml);
        // finish
        refreshTooltips();
    }
}

/** 
    LISTS 
**/
// Load lists via AJAX
function loadDataTableLists() {
    if (typeof assetDataTables.lists !== 'undefined') {
        assetDataTables.lists.ajax.reload();
    } else {
        assetDataTables.lists = $("#table_lists").DataTable({
            ajax: function(data, callback, settings) {
                $.ajax({
                    url: ENV['TABLE_FIND_ENDPOINT'],
                    method: 'GET',
                    data: {
                        show_archived: Cookies.get('lists_show_archived')
                    },
                    success: function(data) {
                        data = JSON.parse(data);
                        let tableData = [];
                        data.forEach(function(table){
                            if (table.authorised) {
                                let rowData = {};
                                rowData.year = Handlebars.escapeExpression(table.year);
                                rowData.semester = Handlebars.escapeExpression(table.semester);
                                // see if there is a default custom view
                                let defaultViewUuid = '';
                                table.views.forEach(function(view){
                                    if (view.role == 'default' && view.authorised) {
                                        defaultViewUuid = view.view_uuid;
                                    }
                                });
                                let tableUrl = ENV['TABLE_VIEW_ENDPOINT'].replace('__table_uuid__', table.uuid);
                                let tableUrlRoot = ENV['TABLE_VIEW_ENDPOINT'].replace('__table_uuid__', table.uuid);
                                if (defaultViewUuid) {
                                    tableUrl = ENV['TABLE_VIEW_VIEW_ENDPOINT'].replace('__table_uuid__', table.uuid).replace('__view_uuid__', defaultViewUuid);
                                }
                                rowData.code = '<a href="' + tableUrl + '">' + Handlebars.escapeExpression(table.code) + '</a>';
                                // table name and extra buttons
                                let cellHtml = '<span><a href="' + tableUrl + '">' + Handlebars.escapeExpression(table.name) +'</a>' + (table.workflow_state == 'archived' ? '<span class="badge badge-secondary ml-1">Archived</span>' : '') + '</span>';
                                let rowButtons = [];
                                rowButtons.push('<a href="' + tableUrl + '" class="p-2" data-tippy-content="View list" aria-label="View list"><span class="fa fa-table"></span></a>');
                                if (table.views.length > 0) {
                                    rowButtons.push('<div class="btn-group dropleft">');
                                        rowButtons.push('<button type="button" class="btn btn-xs btn-primary dropdown-toggle p-2" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false" data-tippy-content="Select a custom view" aria-label="Select a custom view"><span class="fa fa-eye"></span></button>');
                                        rowButtons.push('<div class="dropdown-menu">');
                                            table.views.forEach(function(view){
                                                if (view.authorised) {
                                                    rowButtons.push('<a class="dropdown-item" href="' + ENV['TABLE_VIEW_VIEW_ENDPOINT'].replace('__table_uuid__', view.table_uuid).replace('__view_uuid__', view.view_uuid) + '">' + Handlebars.escapeExpression(view.name) + '</a>');
                                                }
                                            });
                                            rowButtons.push('<div class="dropdown-divider"></div>');
                                            rowButtons.push('<a class="dropdown-item" href="' + tableUrlRoot + '?view=">View all columns</a>');
                                        rowButtons.push('</div>');
                                    rowButtons.push('</div>');
                                } else {
                                    rowButtons.push('<a class="text-muted p-2" data-tippy-content="No custom views available" aria-label="No custom views available"><span class="fa fa-eye"></span></a>');
                                }
                                if (table.authorised) {
                                    rowButtons.push('<a href="' + ENV['TABLE_EDIT_ENDPOINT'].replace('__table_uuid__', table.uuid) + '" class="p-2" role="button" aria-label="Edit list options" data-tippy-content="Edit list options"><span class="fa fa-cog"></span></a>');
                                    rowButtons.push('<a href="' + ENV['TABLE_VIEW_RELATED_ASSETS_ENDPOINT'].replace('__table_uuid__', table.uuid) + '" class="p-2" role="button" aria-label="View filters and portals linked to this list" data-tippy-content="View filters and portals linked to this list"><span class="fa fa-boxes"></span></a>');
                                    rowButtons.push('<a href="' + ENV['VIEW_SUMMARIES_ENDPOINT'].replace('__table_uuid__', table.uuid) + '" class="p-2" role="button" aria-label="View and edit summaries" data-tippy-content="View and edit summaries"><span class="fa fa-layer-group"></span></a>');
                                    rowButtons.push('<a href="' + ENV['VIEW_SINGLE_STUDENT_ENDPOINT'].replace('__table_uuid__', table.uuid) + '" class="p-2" role="button" aria-label="Show information for one student" data-tippy-content="Show information for one student"><span class="fa fa-user"></span></a>');
                                    if (ENV['SRES_USER']['admin-list'] || ENV['SRES_USER']['admin-super']) {
                                        rowButtons.push('<a href="' + ENV['TABLE_CLONE_ENDPOINT'].replace('__table_uuid__', table.uuid) + '" class="p-2" role="button" aria-label="Clone list" data-tippy-content="Clone list"><span class="fa fa-clone"></span></a>');
                                    }
                                    if (table.workflow_state == 'archived') {
                                        rowButtons.push('<a href="#" class="p-2 sres-list-unarchive" aria-label="Unarchive list" data-tippy-content="Unarchive list" data-sres-tableuuid="' + table.uuid + '"><span class="fa fa-box-open"></span></a>');
                                    } else {
                                        rowButtons.push('<a href="#" class="p-2 sres-list-archive" aria-label="Archive list" data-tippy-content="Archive list" data-sres-tableuuid="' + table.uuid + '"><span class="fa fa-archive"></span></a>');
                                    }
                                    rowButtons.push('<a href="#" class="p-2 sres-dashboard-pin-asset" role="button" aria-label="Pin to dashboard and watch" data-tippy-content="Pin to dashboard and watch" data-sres-asset-type="table" data-sres-asset-uuid="' + table.uuid + '"><span class="fa fa-thumbtack"></span></a>');
                                }
                                if (ENV['SRES_USER']['admin-super']) {
                                    rowButtons.push('<a href="#" class="p-2 sres-list-delete" aria-label="Delete list" data-tippy-content="Delete list" data-sres-tableuuid="' + table.uuid + '"><span class="fa fa-trash"></span></a>');
                                }
                                cellHtml += '<span class="float-right sres-asset-row-buttons">';
                                cellHtml += rowButtons.join('');
                                cellHtml += '</span>';
                                rowData.name = cellHtml;
                                // finish
                                tableData.push(rowData);
                            }
                        });
                        //console.log('tableData', tableData);
                        callback({data:tableData});
                    }
                });
            },
            autoWidth: false,
            dom: "<'row'<'col-sm-4'f><'col-sm-4'l><'col-sm-4'p>><'row'<'col-sm-12'tr>>",
            columns: [
                {data: 'year'},
                {data: 'semester'},
                {data: 'code'},
                {data: 'name'}
            ],
            order: [
                [ 0, 'desc' ],
                [ 1, 'desc' ]
            ],
            language: {
                lengthMenu: 'Show _MENU_ lists',
                zeroRecords: 'No matching lists found. You can create a new list using the \'Create new list\' button.',
                processing: LOADING_SPAN_HTML,
            }
        });
    }
};
// Lists archiving
$(document).on('click', '.sres-list-unarchive', function(){
    let tableUuid = $(this).attr('data-sres-tableuuid');
	$.ajax({
        url: ENV['TABLE_UNARCHIVE_ENDPOINT'].replace('__table_uuid__', tableUuid),
        method: 'PUT',
        success: function(data){
            $.notify({message:'Unarchived successfully'}, {type: 'success'});
            loadDataTableLists();
        },
        error: function(){
            $.notify({message:'Unexpected error'}, {type: 'danger'});
        }
    });
});
$(document).on('click', '.sres-list-archive', function(){
    let tableUuid = $(this).attr('data-sres-tableuuid');
	$.ajax({
        url: ENV['TABLE_ARCHIVE_ENDPOINT'].replace('__table_uuid__', tableUuid),
        method: 'PUT',
        success: function(data){
            $.notify({message:'Archived successfully'}, {type: 'success'});
            loadDataTableLists();
        },
        error: function(){
            $.notify({message:'Unexpected error'}, {type: 'danger'});
        }
    });
});
$(document).on('change', "#lists_show_archived", function(){
	Cookies.set('lists_show_archived', $(this).val());
	loadDataTableLists();
});
$(document).ready(function(){
	if (Cookies.get('lists_show_archived')) {
		$("#lists_show_archived").val(Cookies.get('lists_show_archived'));
	}
});
// List deleting
$(document).on('click', '.sres-list-delete', function(){
    if (confirm("Instead of deleting, we recommend archiving a list. Deleting this list cannot be undone. You will lose all settings and data relating to this list.")) {
        let tableUuid = $(this).attr('data-sres-tableuuid');
        $.ajax({
            url: ENV['TABLE_DELETE_ENDPOINT'].replace('__table_uuid__', tableUuid),
            method: 'DELETE',
            success: function(data){
                $.notify({message:'Deleted successfully'}, {type: 'success'});
                loadDataTableLists();
            },
            error: function(){
                $.notify({message:'Unexpected error'}, {type: 'danger'});
            }
        });
    }
});

/** 
    FILTERS 
**/
// Load filters via AJAX
function loadDataTableFilters() {
    if (typeof assetDataTables.filters !== 'undefined') {
        assetDataTables.filters.ajax.reload();
    } else {
        assetDataTables.filters = $("#table_filters").DataTable({
            ajax: function(data, callback, settings) {
                $.ajax({
                    url: ENV['FILTER_LIST_ENDPOINT'],
                    method: 'GET',
                    success: function(data) {
                        data = JSON.parse(data);
                        let tableData = [];
                        data.forEach(function(filter){
                            let rowData = {};
                            let filterEditUrl = ENV['FILTER_EDIT_ENDPOINT'].replace('__filter_uuid__', filter.uuid);
                            let filterRunAlready = (filter.run_history.length > 0);
                            // filter name and extra buttons
                            let cellHtml = '<span><a href="' + filterEditUrl + '">' + Handlebars.escapeExpression(filter.name) + '</a><span class="text-muted"> ' + Handlebars.escapeExpression(filter.description) + '</span>' + (filterRunAlready ? '<span class="badge badge-success ml-1">Sent</span>' : '') +'</span>';
                            let rowButtons = [];
                            rowButtons.push('<a href="' + filterEditUrl + '" class="p-2" data-tippy-content="Edit filter" aria-label="Edit filter"><span class="fa fa-pen"></span></a>');
                            rowButtons.push('<a href="' + ENV['FILTER_PREVIEW_ENDPOINT'].replace('__filter_uuid__', filter.uuid) + '" class="p-2" role="button" aria-label="Preview run" data-tippy-content="Preview run"><span class="fa fa-play"></span></a>');
                            if (ENV['SRES_USER']['admin-filter'] || ENV['SRES_USER']['admin-super']) {
                                rowButtons.push('<a href="' + ENV['FILTER_CLONE_ENDPOINT'].replace('__filter_uuid__', filter.uuid) + '" class="p-2" role="button" aria-label="Clone filter" data-tippy-content="Clone filter"><span class="fa fa-clone"></span></a>');
                            }
                            if (filterRunAlready) {
                                rowButtons.push('<a href="' + ENV['FILTER_VIEW_LOGS_ENDPOINT'].replace('__filter_uuid__', filter.uuid) + '" class="p-2 sres-filter-view-log" aria-label="View send log" data-tippy-content="View send log"><span class="fa fa-history"></span></a>');
                                if (ENV['SRES_USER']['admin-super']) {
                                    rowButtons.push('<a class="p-2 text-muted" aria-label="Cannot delete filter that has already run" data-tippy-content="Cannot delete filter that has already run"><span class="fa fa-trash"></span></a>');
                                }
                            } else {
                                rowButtons.push('<a class="p-2 sres-filter-view-log text-muted" aria-label="No send log available, filter has not been sent yet" data-tippy-content="No send log available, filter has not been sent yet"><span class="fa fa-history"></span></a>');
                                if (ENV['SRES_USER']['admin-super']) {
                                    rowButtons.push('<a href="#" class="p-2 sres-filter-delete" aria-label="Delete filter" data-tippy-content="Delete filter" data-sres-filteruuid="' + filter.uuid + '"><span class="fa fa-trash"></span></a>');
                                }
                            }
                            rowButtons.push('<a href="#" class="p-2 sres-dashboard-pin-asset" role="button" aria-label="Pin to dashboard" data-tippy-content="Pin to dashboard" data-sres-asset-type="filter" data-sres-asset-uuid="' + filter.uuid + '"><span class="fa fa-thumbtack"></span></a>');                            cellHtml += '<span class="float-right sres-asset-row-buttons">';
                            cellHtml += rowButtons.join('');
                            cellHtml += '</span>';
                            rowData.name = cellHtml;
                            // filter dates and other stats
                            //rowData.modified_render = moment(filter.modified).fromNow();
                            rowData.modified = filter.modified;
                            rowData.run_on = filterRunAlready ? filter.run_history[0].timestamp : '';
                            // finish
                            tableData.push(rowData);
                        });
                        //console.log('tableData', tableData);
                        callback({data:tableData});
                    }
                });
            },
            autoWidth: false,
            dom: "<'row'<'col-sm-4'f><'col-sm-4'l><'col-sm-4'p>><'row'<'col-sm-12'tr>>",
            columns: [
                {data: 'name'},
                {
                    data: 'modified',
                    render: function ( data, type, row, meta ) {
                        return type === 'display' ? moment(data).fromNow() : data;
                    }
                },
                {
                    data: 'run_on',
                    render: function ( data, type, row, meta ) {
                        return (type === 'display' && data) ? moment(data).fromNow() : data;
                    }
                }
            ],
            order: [
                [ 1, 'desc' ],
                [ 0, 'asc' ]
            ],
            language: {
                lengthMenu: 'Show _MENU_ filters',
                zeroRecords: 'No matching filters found. You can create a new filter using the \'Create new filter\' button.',
                processing: LOADING_SPAN_HTML,
            }
        });
    }
};
// Filter deleting
$(document).on('click', '.sres-filter-delete', function(){
    if (confirm("Deleting this filter cannot be undone. You will lose all settings relating to this filter. Proceed?")) {
        let filterUuid = $(this).attr('data-sres-filteruuid');
        $.ajax({
            url: ENV['FILTER_DELETE_ENDPOINT'].replace('__filter_uuid__', filterUuid),
            method: 'DELETE',
            success: function(data){
                $.notify({message:'Deleted successfully'}, {type: 'success'});
                loadDataTableFilters();
            },
            error: function(){
                $.notify({message:'Unexpected error'}, {type: 'danger'});
            }
        });
    }
});

/** 
    PORTALS 
**/
// Load portals via AJAX
function loadDataTablePortals() {
    if (typeof assetDataTables.portals !== 'undefined') {
        assetDataTables.portals.ajax.reload();
    } else {
        assetDataTables.portals = $("#table_portals").DataTable({
            ajax: function(data, callback, settings) {
                $.ajax({
                    url: ENV['PORTAL_LIST_ENDPOINT'],
                    method: 'GET',
                    success: function(data) {
                        data = JSON.parse(data);
                        let tableData = [];
                        data.forEach(function(portal){
                            let rowData = {};
                            let portalEditUrl = ENV['PORTAL_EDIT_ENDPOINT'].replace('__portal_uuid__', portal.uuid);
                            // portal name and extra buttons
                            let cellHtml = '<span>';
                                cellHtml += '<a href="' + portalEditUrl + '">' + Handlebars.escapeExpression(portal.name) + '</a>';
                                cellHtml += '<span class="text-muted"> ' + Handlebars.escapeExpression(portal.description) + '</span>';
                            cellHtml += '</span>';
                            let rowButtons = [];
                            rowButtons.push('<a href="' + portalEditUrl + '" class="p-2" data-tippy-content="Edit portal" aria-label="Edit and deploy portal"><span class="fa fa-pen"></span></a>');
                            rowButtons.push('<a href="' + ENV['PORTAL_PREVIEW_ENDPOINT'].replace('__portal_uuid__', portal.uuid) + '" class="p-2" role="button" aria-label="Preview portal" data-tippy-content="Preview portal"><span class="fa fa-eye"></span></a>');
                            if (ENV['SRES_USER']['admin-list'] || ENV['SRES_USER']['admin-super']) {
                                rowButtons.push('<a href="' + ENV['PORTAL_CLONE_ENDPOINT'].replace('__portal_uuid__', portal.uuid) + '" class="p-2" role="button" aria-label="Clone portal" data-tippy-content="Clone portal"><span class="fa fa-clone"></span></a>');
                            }
                            rowButtons.push('<a href="' + ENV['PORTAL_VIEW_LOG_ENDPOINT'].replace('__portal_uuid__', portal.uuid) + '" class="p-2 sres-portal-view-log" aria-label="View log" data-tippy-content="View log"><span class="fa fa-history"></span></a>');
                            if (ENV['SRES_USER']['admin-super']) {
                                rowButtons.push('<a href="#" class="p-2 sres-portal-delete" aria-label="Delete portal" data-tippy-content="Delete portal" data-sres-portaluuid="' + portal.uuid + '"><span class="fa fa-trash"></span></a>');
                            }
                            rowButtons.push('<a href="#" class="p-2 sres-dashboard-pin-asset" role="button" aria-label="Pin to dashboard" data-tippy-content="Pin to dashboard" data-sres-asset-type="portal" data-sres-asset-uuid="' + portal.uuid + '"><span class="fa fa-thumbtack"></span></a>');
                            cellHtml += '<span class="float-right sres-asset-row-buttons">';
                            cellHtml += rowButtons.join('');
                            cellHtml += '</span>';
                            rowData.name = cellHtml;
                            // portal dates and other stats
                            rowData.modified = portal.modified;
                            let availability_comments = portal.active_now_messages.map(function(x){return x[0]});
                            if (portal.active_now) {
                                rowData.availability = 'Yes';
                            } else {
                                rowData.availability = 'No <span class="fa fa-info-circle" data-tippy-content="' + Handlebars.escapeExpression(availability_comments) + '"></span>';
                            }
                            // finish
                            tableData.push(rowData);
                        });
                        //console.log('tableData', tableData);
                        callback({data:tableData});
                    }
                });
            },
            autoWidth: false,
            dom: "<'row'<'col-sm-4'f><'col-sm-4'l><'col-sm-4'p>><'row'<'col-sm-12'tr>>",
            columns: [
                {
                    data: 'name'
                },
                {
                    data: 'modified',
                    render: function ( data, type, row, meta ) {
                        return type === 'display' ? moment(data).fromNow() : data;
                    }
                },
                {
                    data: 'availability'
                }
            ],
            order: [
                [ 1, 'desc' ],
                [ 0, 'asc' ]
            ],
            language: {
                lengthMenu: 'Show _MENU_ portals',
                zeroRecords: 'No matching portals found. You can create a new portal using the \'Create new portal\' button.',
                processing: LOADING_SPAN_HTML,
            }
        });
    }
};
// Portal deleting
$(document).on('click', '.sres-portal-delete', function(){
    if (confirm("Deleting this portal cannot be undone. You will lose all settings relating to this portal. Proceed?")) {
        let portalUuid = $(this).attr('data-sres-portaluuid');
        $.ajax({
            url: ENV['PORTAL_DELETE_ENDPOINT'].replace('__portal_uuid__', portalUuid),
            method: 'DELETE',
            success: function(data){
                $.notify({message:'Deleted successfully'}, {type: 'success'});
                loadDataTablePortals();
            },
            error: function(){
                $.notify({message:'Unexpected error'}, {type: 'danger'});
            }
        });
    }
});

/** 
    INSIGHTS/ALERTS 
**/
// Load insights via AJAX
function loadDataTableInsights() {
    if (typeof assetDataTables.insights !== 'undefined') {
        assetDataTables.insights.ajax.reload();
    } else {
        assetDataTables.insights = $("#table_insights").DataTable({
            ajax: function(data, callback, settings) {
                $.ajax({
                    url: ENV['INSIGHT_LIST_ENDPOINT'],
                    method: 'GET',
                    success: function(data) {
                        data = JSON.parse(data);
                        let tableData = [];
                        data.forEach(function(insight){
                            let rowData = {};
                            let insightEditUrl = ENV['INSIGHT_EDIT_ENDPOINT'].replace('__insight_uuid__', insight.uuid);
                            // insight name and extra buttons
                            let cellHtml = '<span><a href="' + insightEditUrl + '">' + Handlebars.escapeExpression(insight.name) + '</a><span class="text-muted"> ' + Handlebars.escapeExpression(insight.description) + '</span></span>';
                            let rowButtons = [];
                            rowButtons.push('<a href="' + insightEditUrl + '" class="p-2" data-tippy-content="Edit insight" aria-label="Edit insight"><span class="fa fa-pen"></span></a>');
                            rowButtons.push('<a href="' + ENV['INSIGHT_PREVIEW_ENDPOINT'].replace('__insight_uuid__', insight.uuid) + '" class="p-2" role="button" aria-label="Preview" data-tippy-content="Preview"><span class="fa fa-eye"></span></a>');
                            rowButtons.push('<a href="' + ENV['INSIGHT_RUN_ENDPOINT'].replace('__insight_uuid__', insight.uuid) + '" class="p-2" role="button" aria-label="Run" data-tippy-content="Run"><span class="fa fa-envelope"></span></a>');
                            if (ENV['SRES_USER']['admin-list'] || ENV['SRES_USER']['admin-super']) {
                                rowButtons.push('<a href="' + ENV['INSIGHT_CLONE_ENDPOINT'].replace('__insight_uuid__', insight.uuid) + '" class="p-2" role="button" aria-label="Clone insight" data-tippy-content="Clone insight"><span class="fa fa-clone"></span></a>');
                            }
                            if (ENV['SRES_USER']['admin-super']) {
                                rowButtons.push('<a href="#" class="p-2 sres-insight-delete" aria-label="Delete insight" data-tippy-content="Delete insight" data-sres-insightuuid="' + insight.uuid + '"><span class="fa fa-trash"></span></a>');
                            }
                            rowButtons.push('<a href="#" class="p-2 sres-dashboard-pin-asset" role="button" aria-label="Pin to dashboard" data-tippy-content="Pin to dashboard" data-sres-asset-type="insight" data-sres-asset-uuid="' + insight.uuid + '"><span class="fa fa-thumbtack"></span></a>');
                            cellHtml += '<span class="float-right sres-asset-row-buttons">';
                            cellHtml += rowButtons.join('');
                            cellHtml += '</span>';
                            rowData.name = cellHtml;
                            // insight dates and other stats
                            rowData.modified = insight.modified;
                            rowData.activeNow = insight.active_now;
                            // finish
                            tableData.push(rowData);
                        });
                        //console.log('tableData', tableData);
                        callback({data:tableData});
                    }
                });
            },
            autoWidth: false,
            dom: "<'row'<'col-sm-4'f><'col-sm-4'l><'col-sm-4'p>><'row'<'col-sm-12'tr>>",
            columns: [
                {
                    data: 'name'
                },
                {
                    data: 'modified',
                    render: function ( data, type, row, meta ) {
                        return type === 'display' ? moment(data).fromNow() : data;
                    }
                },
                {
                    data: 'activeNow'
                }
            ],
            order: [
                [ 1, 'desc' ],
                [ 0, 'asc' ]
            ],
            language: {
                lengthMenu: 'Show _MENU_ insights',
                zeroRecords: 'No matching insights found. You can create a new insight using the \'Create new insight\' button.',
                processing: LOADING_SPAN_HTML,
            }
        });
    }
};
// Portal deleting
$(document).on('click', '.sres-insight-delete', function(){
    if (confirm("Deleting this insight cannot be undone. You will lose all settings relating to this insight. Proceed?")) {
        let insightUuid = $(this).attr('data-sres-insightuuid');
        $.ajax({
            url: ENV['INSIGHT_DELETE_ENDPOINT'].replace('__insight_uuid__', insightUuid),
            method: 'DELETE',
            success: function(data){
                $.notify({message:'Deleted successfully'}, {type: 'success'});
                loadDataTableInsights();
            },
            error: function(){
                $.notify({message:'Unexpected error'}, {type: 'danger'});
            }
        });
    }
});



/**
    Confirmation dialog
**/
$(document).on('show.bs.modal', "#confirm_delete", function(event) {
    var message = $(event.relatedTarget).data('message');
    var target = $(event.relatedTarget).data('targetfunction');
    $(this).find('#confirm_delete_message').text(message);
    $(this).find('#confirm_delete_delete').off().on('click', function() {
        $('#confirm_delete').modal('hide');
        window[target]();
    });
});

/** 
    Internet Explorer 6-11 warning
**/
$(document).ready(function(){
    // Internet Explorer 6-11
    var isIE = /*@cc_on!@*/false || !!this.documentMode;
    if(isIE)
    {
        $('#explorer_warning').removeClass('d-none');
    }
});

/**
    Data entry
**/
function loadDataEntry(){
    $("#data_entry_links_container").html(LOADING_SPAN_HTML);
    if ($.ajaxq.isRunning('data_entry_loader')) {
        return;
    }
    $.ajaxq('data_entry_loader', {
        url: ENV['DATA_ENTRY_LIST_ENDPOINT'],
        method: 'GET',
        success: function(data){
            data = JSON.parse(data);
            //console.log(data);
            
            $("#data_entry_links_container").html('');
            
            if (data.length) {
                var template = Handlebars.compile(document.getElementById("data_entry_table_template").innerHTML);
                data.forEach(function(table){
                    if (table.columns.length) {
                        $("#data_entry_links_container").append(template({
                            table_uuid: table.table.uuid,
                            table_display_name: table.table.display_name,
                            columns: table.columns
                        }));
                    }
                });
            } else {
                $("#data_entry_links_container").html("No data entry links are available at this time. You may not have the right permissions to SRES.");
            }
        },
        error: function(){
            $("#data_entry_links_container").html("Sorry, couldn't load any data entry links at this time.");
        }
    });
};

/** 
    SRES COLLECTIVE 
**/
// Load collective assets via AJAX
function loadCollective() {
    $('#collective_assets_container').html(LOADING_DIV_HTML);
    $.ajax({
        url: ENV['COLLECTIVE_LIST_ASSETS'],
        data: {
            limit: 12,
            all: 1
        },
        method: 'GET',
        success: function(data) {
            data = JSON.parse(data);
            let template = Handlebars.compile(document.getElementById("collective_asset_line_template").innerHTML);
            $('#collective_assets_container').html('');
            if (data.length) {
                data.forEach(function(asset){
                    let desc = asset['description'].replace(/(<([^>]+)>)/ig, "");
                    if (desc.length > 200) {
                        desc = desc.substring(0, 200) + '...';
                    }
                    $('#collective_assets_container').append(template({
                        asset_icon: asset['icon'],
                        asset_type: asset['type'].charAt(0).toUpperCase() + asset['type'].slice(1),
                        name: asset['name'],
                        description: desc,
                        url: ENV['COLLECTIVE_SHOW_ASSET_ENDPOINT'].replace('__asset_uuid__', asset['uuid']),
                        count_likes: asset['liked_by_count']
                    }));
                });
            }
        },
        error: function(){
            $('#collective_assets_container').html("Sorry, couldn't load the Collective at this time.");
        }
    });
};

/**
    SEARCH SRES
**/
$(document).on('keypress', '#search_sres_term', function(event){
    if (event.which == 13) {
        $('#search_sres_search').trigger('click');
    }
});
$(document).on('change', 'input:checkbox[name=search_sres_haystack_types]', function(){
    if ($('input:checkbox[name=search_sres_haystack_types]:checked').length == 0) {
        $('#search_sres_search').addClass('disabled').prop('disabled', true);
    } else {
        $('#search_sres_search').removeClass('disabled').prop('disabled', false);
    }
});
$(document).on('click', '#search_sres_search', function(){
    let term = $('#search_sres_term').val();
    let haystackTypes = $('input:checkbox[name=search_sres_haystack_types]').serialize();
    
    //console.log(term, haystackTypes);
    
    $('#search_sres_results_header').removeClass('d-none');
    $('#search_sres_results_too_many').addClass('d-none');
    $('#search_sres_results_container').html(LOADING_DIV_HTML);
    
    $.ajax({
        url: ENV['SEARCH_SRES_ENDPOINT'] + '?' + haystackTypes,
        data: {
            term: term
        },
        method: 'GET',
        success: function(data) {
            data = JSON.parse(data);
            $('#search_sres_results_container').html('');
            if (data.results.length) {
                let results = data.results.slice(0, 100); // only display the first few results
                if (data.all_results_count > 100) {
                    $('#search_sres_results_too_many').removeClass('d-none');
                    $('#search_sres_results_too_many_count').html(data.all_results_count);
                }
                let template = Handlebars.compile(document.getElementById("search_sres_result_card_template").innerHTML);
                results.forEach(function(result){
                    let haystackTypeDisplay = '';
                    switch (result.haystack_type) {
                        case 'tables':
                            haystackTypeDisplay = 'List'
                            break;
                        case 'columns':
                            haystackTypeDisplay = 'Column'
                            break;
                        case 'filters':
                            haystackTypeDisplay = 'Filter'
                            break;
                        case 'portals':
                            haystackTypeDisplay = 'Portal'
                            break;
                        case 'identifiers':
                            haystackTypeDisplay = 'Student'
                            break;
                        case 'student_data':
                            haystackTypeDisplay = 'Student data'
                            break;
                    }
                    let resultConfig = {
                        haystackTypeDisplay: haystackTypeDisplay,
                        haystackType: result.haystack_type,
                        display: result.display,
                        displaySubs: result.display_subs,
                        links: result.links
                    };
                    if (result.workflow_state != 'active') {
                        resultConfig.workflowState = result.workflow_state.charAt(0).toUpperCase() + result.workflow_state.substr(1);
                    }
                    if (typeof result.search_result_preview !== 'undefined') {
                        resultConfig.searchResultPreview = result.search_result_preview;
                    }
                    let resultHtml = template(resultConfig);
                    
                    $('#search_sres_results_container').append(resultHtml);
                });
            } else {
                $('#search_sres_results_container').html('<div class="col-sm-12">No results found.</div>');
            }
        }
    });
});


