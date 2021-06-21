// Show toggler icon in top nav
$(document).ready(function(){
	$('nav li.sres-offline-nav-toggle-dropdown').removeClass('hidden');
});

/**
	Offline page lists
**/
// List the available pages from localStorage
$(document).ready(function(){
	var offlinePages = getOfflinePages();
	offlinePages.forEach(function(offlinePage){
		window.caches.match(offlinePage['url']).then(function(response){
			// Prepare variables
			var pageAvailable = false;
			if (typeof response != 'undefined') {
				pageAvailable = true;
			}
			var lastSyncDate = new Date(offlinePage['lastUpdated']).toLocaleDateString();
			var lastSyncTime = new Date(offlinePage['lastUpdated']).toLocaleTimeString();
			var authUser = offlinePage['authUser'] ? offlinePage['authUser'] : '[unknown user]';
			var daysSinceLastSync = Math.round((new Date() - new Date(offlinePage['lastUpdated'])) / (1000 * 60 * 60 * 24));
			var recordsWaiting = -1;
			var pathname = new URL(offlinePage['url']).pathname;
			try {
				var tableuuid = offlinePage['url'].match(/(?:tableuuid=)([A-F0-9a-f_]*)/)[1];
				var columnuuid = offlinePage['url'].match(/(?:columnuuid=)([A-Z0-9a-z_]*)/)[1];
				if (tableuuid && columnuuid) {
					recordsWaiting = enumerateBackups(tableuuid, columnuuid, pathname);
				}
			} catch(e) {
				// nothing
				recordsWaiting = 0;
			}
			// Show
			var li = $('<p class="sres-offline-saved-page">' 
				+ (pageAvailable ? '' : '[Unavailable] ')
				+ '<a href="' + offlinePage['url'] + '"><span class="btn btn-primary btn-sm">View</span> ' + offlinePage['title'] + '</a>'
				+ '<span class="text-success"> <span class="glyphicon glyphicon-user" aria-hidden="true"></span> ' + authUser + '</span>'
				+ '<span class="text-muted"> <span class="glyphicon glyphicon-cloud-download" aria-hidden="true"></span> page last synced ' + daysSinceLastSync + ' days ago (' + lastSyncDate + ' ' + lastSyncTime + ') </span>'
				+ '<span class="text-danger">' + (recordsWaiting > 0 ? ' <span class="glyphicon glyphicon-alert" aria-hidden="true"></span> ' + recordsWaiting + ' records saved on this device, waiting to be saved to server' : '') + '</span>'
				+ ' <button class="btn btn-default btn-sm sres-offline-page-remove"><span class="glyphicon glyphicon-trash" aria-hidden="true"></span> Remove</button>'
				+ '</p>'
			);
			li.data('sres-url', offlinePage['url']);
			li.data('sres-authuser', authUser);
			$('#offline_pages_list').append(li);
		});
	});
});
// Deleting offline pages
$(document).on('click', 'button.sres-offline-page-remove', function(){
	if (confirm('Removing this offline page will make it unavailable in offline mode. Are you sure you want to remove this page?')) {
		updateOfflinePages(
			'',
			$(this).parents('p.sres-offline-saved-page').data('sres-url'),
			$(document),
			'delete',
			$(this).parents('p.sres-offline-saved-page').data('sres-authuser')
		);
		$(this).parents('p.sres-offline-saved-page').remove();
	}
});

/**
	Check if offline mode is required
**/
$(document).ready(function(){
	forceOfflineModeIfOffline(true);
});

$(document).on('sres.offlineModeStart', function(){
	$('nav.sres-navbar-offline').removeClass('hidden');
});
$(document).on('sres.offlineModeStop', function(){
	$('nav.sres-navbar-offline').addClass('hidden');
});
