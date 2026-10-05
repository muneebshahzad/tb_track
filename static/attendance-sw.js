const CACHE_NAME = 'tb-attendance-portal-v3';
const CORE_URLS = [
  '/attendance',
  '/attendance-manifest.webmanifest',
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
      .catch(() => caches.match(event.request).then((cached) => cached || caches.match('/attendance')))
  );
});

self.addEventListener('message', (event) => {
  const data = event.data || {};
  if (data.type !== 'ATTENDANCE_REMINDER') return;
  self.registration.showNotification('Tick Bags Attendance', {
    body: "Don't forget to mark your attendance",
    icon: '/static/employee-portal-icon.svg',
    badge: '/static/employee-portal-icon.svg',
    tag: `attendance-reminder-${data.hour || 'now'}`,
    renotify: true,
    data: {url: '/attendance'}
  });
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = event.notification.data?.url || '/attendance';
  event.waitUntil(
    clients.matchAll({type: 'window', includeUncontrolled: true}).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes('/attendance') && 'focus' in client) return client.focus();
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
      return null;
    })
  );
});
