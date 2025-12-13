console.log('🛠️ Service Worker loaded');

self.addEventListener('install', event => {
  console.log('🔧 Service Worker installing...');
  self.skipWaiting(); // Kích hoạt ngay lập tức
});

self.addEventListener('activate', event => {
  console.log('🚀 Service Worker activated');
  event.waitUntil(clients.claim()); // Kiểm soát tất cả clients ngay
});

self.addEventListener('push', event => {
  console.log('📬 Push event received!', event);
  
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
    requireInteraction: true, // Giữ thông báo đến khi user click
    tag: 'appsheet-notification' // Nhóm các thông báo cùng loại
  };
  
  console.log('🎨 Notification options:', options);
  
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
        // Kiểm tra nếu đã có tab mở URL này
        for (let client of windowClients) {
          if (client.url === urlToOpen && 'focus' in client) {
            return client.focus();
          }
        }
        // Nếu chưa có, mở tab mới
        if (clients.openWindow) {
          return clients.openWindow(urlToOpen);
        }
      })
  );
});