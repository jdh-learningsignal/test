var CACHE_NAME = 'sres-cache-v4';
var PING_URL = '';

var urlsToCache = [
];

self.addEventListener('install', function(event) {
	
    PING_URL = new URL(location).searchParams.get('pingUrl');
    
	self.skipWaiting();

	// Perform install steps
	//console.log('instaling sw...', event, self);
	event.waitUntil(
		caches
			.open(CACHE_NAME)
			.then(function(cache) {
				//console.log('Opened cache');
				var res = cache.addAll(urlsToCache) // todo try adding individually? why doesn't install trigger these??
					.then(function(){
						//console.log('urls cached', urlsToCache);
					})
					.catch(function(err){
						//console.log('cache addall err', err);
					});
				return res;
			})
			.catch(function(err){
				console.log('caching error', err);
			})
	);
});

self.addEventListener('fetch', function(event) {
	
	//console.log('fetch in sw', event.request.url);
	
	// Never return cache for ping call
	if (event.request.url.indexOf(PING_URL) !== -1) {
		//console.log('PING CALL - ONLY FETCHING');
		event.respondWith(fetch(event.request));
	} else {
		var requestClone = event.request.clone();
		event.respondWith( 
			// Try network first
			fetch(event.request)
				.then(function(response){
					// Check if we received a valid response
					if(!response || response.status !== 200 || response.type !== 'basic') {
						return response;
					}
					// Do not cache POSTs
					if (event.request.method == 'POST') {
						//console.log('not caching', event.request.url);
						return response;
					}
					// Save to cache
					var responseClone = response.clone();
					caches
						.open(CACHE_NAME)
						.then(function(cache) {
							cache.put(requestClone, responseClone);
						});
					return response;
				})
				.catch(function(err){
					// If network fails, get from cache
					return caches
						.match(event.request)
						.then(function(response){
							return response;
						});
				})
		);
	}
});

self.addEventListener('message', function(event) {
	//console.log('sw received message', event);
	
	
	
});

self.addEventListener('activate', function(event) {
	
	var cacheWhitelist = ['sres-cache-v4'];

	event.waitUntil(
		caches.keys().then(function(cacheNames) {
			return Promise.all(
				cacheNames.map(function(cacheName) {
					//console.log('checking cache', cacheName);
					if (cacheWhitelist.indexOf(cacheName) === -1) {
						//console.log('deleting cache', cacheName);
						return caches.delete(cacheName);
					}
				})
			);
		})
	);
});