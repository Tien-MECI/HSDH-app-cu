// ===== public/sw.js =====
console.log('🛠️ Service Worker loaded');

self.addEventListener('install', event => {
  console.log('🔧 Service Worker installing...');
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  console.log('🚀 Service Worker activated');
  event.waitUntil(clients.claim());
});

self.addEventListener('push', event => {
  console.log('📬 Push event received!');
  
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
    console.log('📦 Push data:', data);
  } catch (e) {
    console.warn('⚠️ Push data parsing error:', e);
    data = { title: 'Thông báo', body: 'Có thông báo mới' };
  }
  
  const options = {
    body: data.body || 'Nội dung thông báo',
    icon: data.icon || '/default-icon.png',
    badge: '/badge-icon.png',
    data: data.data || {},
    requireInteraction: true,
    tag: 'appsheet-notification'
  };
  
  event.waitUntil(
    self.registration.showNotification(data.title || 'Thông báo', options)
      .then(() => console.log('✅ Notification shown successfully'))
      .catch(err => console.error('❌ Failed to show notification:', err))
  );
});

self.addEventListener('notificationclick', event => {
  console.log('👆 Notification clicked:', event.notification.data);
  event.notification.close();
  
  const urlToOpen = event.notification.data.url || 'https://hsdh-app-cu.onrender.com';
  
  event.waitUntil(
    clients.matchAll({type: 'window', includeUncontrolled: true})
      .then(windowClients => {
        for (let client of windowClients) {
          if (client.url === urlToOpen && 'focus' in client) {
            return client.focus();
          }
        }
        if (clients.openWindow) {
          return clients.openWindow(urlToOpen);
        }
      })
  );
});