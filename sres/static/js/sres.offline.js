var offlineModeActive = false;
const OFFLINE_PAGES_KEY = 'sres.offline.pages';

function serviceWorkerSupported() {
	return ('serviceWorker' in navigator);
}

function getPingUrl() {
	return ENV['ping_url'] + "?rand=" + Math.floor((1 + Math.random()) * 0x10000);
}

function callIfConnected(style, func, ...params) {
	if (offlineModeActive) {
		return false;
	}
	$.ajax({
		method:'GET',
		url: getPingUrl(),
		success: function(data) {
			func(...params);
		},
		error: function(err) {
			showOfflineWarning(style, func, ...params);
		}
	});
}

function showOfflineWarning(style, submitAnywayFunction, ...params) {
	if (offlineModeActive) {
		return false;
	}
	var dialogId = "sresOfflineWarning_" + parseInt(Math.random() * 1000000);
	switch (style) {
		case 'jqm':
			var dialogHtml = '<div data-role="page" data-dialog="true" id="' + dialogId + '" data-overlay-theme="e"><div data-role="header"><h1>No connectivity</h1></div><div data-role="content">You appear to have lost connection to the SRES server. Reconnect to the internet and try again.<br><a href="#" data-role="button" data-rel="back" data-theme="b" data-icon="arrow-l">I\'ll try again later</a><a href="#" class="sres-offline-submitanyway" data-role="button" data-theme="e" data-icon="alert">Ignore this, submit anyway</a></div></div>';
			$('body').append(dialogHtml);
			$.mobile.changePage("#" + dialogId, {role: 'dialog'});
			break;
		case 'bs3':
			var dialogHtml = '<div class="modal" tabindex="-1" role="dialog" id="' + dialogId + '">' + 
				'<div class="modal-dialog" role="document">' + 
				'<div class="modal-content">' + 
				'<div class="modal-header"><h4 class="modal-title">No connectivity</h4><button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button></div>' + 
				'<div class="modal-body"><p><span class="glyphicon glyphicon-remove" aria-hidden="true"></span> You appear to have lost connection to the SRES server. Reconnect to the internet and try again.</p></div>' + 
				'<div class="modal-footer"><button type="button" class="btn btn-primary" data-dismiss="modal"><span class="glyphicon glyphicon-chevron-left" aria-hidden="true"></span> I\'ll try again later</button><button type="button" class="btn btn-warning sres-offline-submitanyway"><span class="glyphicon glyphicon-warning-sign" aria-hidden="true"></span> Ignore this, submit anyway</button></div>' + 
				'</div></div></div>';
			$('body').append(dialogHtml);
			$("#" + dialogId).modal().modal('show');
			break;
		default:
			// confirm box
			alert('You appear to have lost connection to the SRES server. Reconnect to the internet and try again.');
			break;
	}
	$('a.sres-offline-submitanyway, button.sres-offline-submitanyway').on('click', function(){
		submitAnywayFunction(...params);
		switch (style) {
			case 'bs3':
				$("#" + dialogId).modal('hide');
				break;
			case 'jqm':
				$("#" + dialogId).dialog('close');
				break;
		}
	});
}

/** 
	Toggle offline mode
**/
$(document).on('click', 'a.sres-offline-nav-toggle', function(){
	if (!serviceWorkerSupported()) {
		if (!confirm('Offline mode is not supported in this browser. If you continue, there may be some unexpected behaviour. Continue?')) {
			return false;
		}
	}
	if ($(this).hasClass('sres-offline-nav-toggle-godown')) {
		startOfflineMode();
	} else if ($(this).hasClass('sres-offline-nav-toggle-goup')) {
		// Go back online
		// First ping to check connectivity
		$.get(getPingUrl())
			.then(function() {
				stopOfflineMode();
			})
			.fail(function() {
				if (confirm('We can\'t seem to contact the server. You may still be experiencing connectivity issues. Continue to deactivate offline mode?')) {
					stopOfflineMode();
				}
			});
	}
});
function startOfflineMode(skipReload) {
	if (urlContainsOffline() || skipReload) {
		offlineModeActive = true;
		$(document).trigger('sres.offlineModeStart');
		$('a.sres-offline-nav-toggle-godown').addClass('d-none');
		$('a.sres-offline-nav-toggle-goup').removeClass('d-none');
	} else {
		// Force a reload to force service worker to cache things
		if (confirm('To activate offline mode, we need to reload the current page. Before proceeding, ensure that all unsaved data are saved. Continue to reload?')) {
			var u = new Url;
			if (u.path.match('/entry/offline')) {
				// Don't add &offline
			} else {
				u.query.offline = '';
			}
			window.location = u.toString();
		}
	}
}
function stopOfflineMode() {
	offlineModeActive = false;
	$(document).trigger('sres.offlineModeStop');
	$('a.sres-offline-nav-toggle-goup').addClass('d-none');
	$('a.sres-offline-nav-toggle-godown').removeClass('d-none');
	var u = new Url;
    delete u.query.offline
	window.history.pushState({}, '', u.toString());
}
function forceOfflineModeIfOffline(skipReload) {
	$.get(getPingUrl())
		.fail(function() {
			startOfflineMode(skipReload);
		})
}
function isOfflineModeActive() {
	return offlineModeActive;
}
function urlContainsOffline() {
    var u = new Url;
    return (!(typeof u.query.offline == 'undefined'));
}

/** 
	localStorage offline mode pages
**/
function getOfflinePages() {
	var offlinePages = localStorage.getItem(OFFLINE_PAGES_KEY);
	if (offlinePages == null) {
		offlinePages = [];
	} else {
		offlinePages = JSON.parse(offlinePages);
	}
	return offlinePages;
}
function updateOfflinePages(title, loc, doc, mode, authUser) {
	console.log(title, loc, doc, mode);
	// Retrieve from storage
	var offlinePages = getOfflinePages();
	// Prepare variables
	var url = loc;
	var pageIndex = -1;
	// Try and locate current url
	for (var p = 0; p < offlinePages.length; p++) {
		console.log('testing', offlinePages[p]['url'], url, authUser);
		if (offlinePages[p]['url'].toLowerCase() == url.toLowerCase() && offlinePages[p]['authUser'].toLowerCase() == authUser.toLowerCase()) {
			pageIndex = p;
			console.log('pageIndex', pageIndex);
			break;
		}
	}
	// Act
	if (pageIndex == -1 && mode != 'delete') {
		// Add
		offlinePages.push(
			{
				'url': url,
				'title': title,
				'lastUpdated': new Date(),
				'authUser': authUser
			}
		);
	} else {
		if (mode == 'delete' && pageIndex != -1) {
			// Delete
			offlinePages.splice(pageIndex, 1);
		} else {
			// Update
			offlinePages[pageIndex] = {
				'url': url,
				'title': title,
				'lastUpdated': new Date(),
				'authUser': authUser
			}
		}
	}
	// Save to storage
	localStorage.setItem(OFFLINE_PAGES_KEY, JSON.stringify(offlinePages));
}

/**
	localStorage data backups
**/
function getBackupKey(tableuuid, columnuuid, identifier, overridePath) {
	if (typeof overridePath == 'undefined' || overridePath == null) {
		overridePath = window.location.pathname;
	}
	var key = 'sres.' + overridePath + '.lastAttemptedSave.' + tableuuid + '.' + columnuuid + '.' + identifier;
	return key;
}
function saveBackup(data, tableuuid, columnuuid, identifier) {
	var key = getBackupKey(tableuuid, columnuuid, identifier);
	localStorage.setItem(key, JSON.stringify(data));
	return true;
}
function getBackup(tableuuid, columnuuid, identifier) {
	var key = getBackupKey(tableuuid, columnuuid, identifier);
	var data = localStorage.getItem(key);
	try {
		return JSON.parse(data);
	} catch(e) {
		if (typeof data == 'string') {
			return data;
		} else {
			return false;
		}
	}
}
function deleteBackup(tableuuid, columnuuid, identifier) {
	var key = getBackupKey(tableuuid, columnuuid, identifier);
	localStorage.removeItem(key);
	return true;
}
function backupExists(tableuuid, columnuuid, identifier) {
	var key = getBackupKey(tableuuid, columnuuid, identifier);
	var data = localStorage.getItem(key);
	if (typeof data == 'undefined' || data == null) {
		return false;
	} else {
		return true;
	}
}
function enumerateBackups(tableuuid, columnuuid, overridePath) {
	var count = 0;
	Object.keys(localStorage).forEach(function(key){
		if (key.indexOf(getBackupKey(tableuuid, columnuuid, '', overridePath)) != -1) {
			count++
		}
	});
	return count;
}

/**
    Offline mode
**/
// Things to do when starting or stopping offline mode
$(document).on('sres.offlineModeStart', function(){
	//console.log('offlineModeStart');
	$('.sres-backup-available-save').addClass('d-none');
	$('.sres-backup-save-all').addClass('d-none');
	$('.sres-offline-nav-notification-active').removeClass('d-none');
	// Add to offline pages list
	updateOfflinePages(
		ENV['data-sres-offline-title'],
		window.location.href,
		$(document),
		'add',
		ENV['data-sres-offline-authuser']
	);
	// Show notification
	window.caches
		.match(window.location.href)
		.then(function(response){
			if (response) {
				// Cache match, OK for offline
				$('#sres_offline_mode_notification').modal('show');
			} else {
				return Promise.reject('cache match failed');
			}
		})
		.catch(function(err) {
			// No cache match, problem going offline
			if (confirm('There was a problem caching this page for offline mode. If you proceed to offline mode, unexpected behaviour may occur. Proceed with offline mode?')) {
				$('#sres_offline_mode_notification').modal('show');
			} else {
				stopOfflineMode();
			}
		});
});
$(document).on('sres.offlineModeStop', function(){
	//console.log('offlineModeStop');
	$('.sres-backup-available-save').removeClass('d-none');
	$('.sres-backup-save-all').removeClass('d-none');
	$('.sres-offline-nav-notification-active').addClass('d-none');
});
// Activate offline mode if &offline present in URL
$(document).ready(function(){
	if (urlContainsOffline()) {
		startOfflineMode();
	}
});

/**
	Check if offline mode is required
**/
$(document).ready(function(){
	forceOfflineModeIfOffline();
});
