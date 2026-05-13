const CACHE_NAME = 'tb-employee-portal-v1';
const CORE_URLS = [
  '/employee_portal',
  '/employee_portal/orders',
  '/static/html5-qrcode.min.js',
  '/static/employee-portal-icon.svg'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(CORE_URLS)).catch(() => null)
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
    ))
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const cloned = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, cloned)).catch(() => null);
        return response;
      })
      .catch(() => caches.match(event.request).then((cached) => cached || caches.match('/employee_portal')))
  );
});
