//Tạo Client-side cho Trình duyệt (Service Worker) push web
// Lắng nghe sự kiện 'push'
self.addEventListener('push', event => {
  let data = { title: 'New Notification', body: '' };
  try {
    data = event.data.json();
  } catch (e) {
    console.warn('Push event data is not JSON, using default.');
  }
  
  const options = {
    body: data.body,
    icon: data.icon || '/default-icon.png', // Đường dẫn đến icon mặc định
    badge: '/badge-icon.png',
    data: data.data || {}, // Dữ liệu tùy chỉnh để xử lý khi click
    // Có thể thêm vibrate, actions...[citation:9]
  };
  
  event.waitUntil(
    self.registration.showNotification(data.title, options)
  );
});

// (Tùy chọn) Xử lý khi người dùng click vào thông báo
self.addEventListener('notificationclick', event => {
  event.notification.close();
  
  // Ví dụ: Mở một URL cụ thể được gửi từ AppSheet
  if (event.notification.data && event.notification.data.url) {
    event.waitUntil(
      clients.openWindow(event.notification.data.url)
    );
  }
});