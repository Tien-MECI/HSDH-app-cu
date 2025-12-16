// ===== public/client.js =====
function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding)
    .replace(/-/g, '+')
    .replace(/_/g, '/');
  const rawData = window.atob(base64);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; ++i) {
    outputArray[i] = rawData.charCodeAt(i);
  }
  return outputArray;
}

// Lấy publicVapidKey từ server (sẽ được inject từ trang HTML)
let publicVapidKey = '';

async function subscribeToPushNotifications() {
  console.log('🔄 Starting push notification subscription...');
  
  if (!('serviceWorker' in navigator)) {
    console.error('❌ Service Worker not supported');
    return;
  }
  
  if (!('PushManager' in window)) {
    console.error('❌ Push API not supported');
    return;
  }
  
  try {
    // Lấy publicVapidKey từ biến toàn cục (sẽ được đặt bởi trang HTML)
    if (!publicVapidKey) {
      // Nếu chưa có, thử lấy từ server
      const response = await fetch('/get-vapid-key');
      const data = await response.json();
      publicVapidKey = data.publicKey;
    }
    
    const permission = await Notification.requestPermission();
    console.log('🔔 Notification permission:', permission);
    
    if (permission !== 'granted') {
      alert('Vui lòng cho phép thông báo trong trình duyệt!');
      return;
    }
    
    console.log('📝 Registering Service Worker...');
    const registration = await navigator.serviceWorker.register('/sw.js');
    console.log('✅ Service Worker registered');
    
    // Đợi Service Worker active
    const serviceWorker = registration.installing || registration.waiting || registration.active;
    if (serviceWorker.state !== 'activated') {
      await new Promise(resolve => {
        serviceWorker.addEventListener('statechange', () => {
          if (serviceWorker.state === 'activated') resolve();
        });
      });
    }
    
    console.log('🔐 Subscribing to push...');
    const subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(publicVapidKey)
    });
    
    console.log('📄 Subscription created');
    
    console.log('📤 Sending subscription to server...');
    const response = await fetch('/subscribe', {
      method: 'POST',
      body: JSON.stringify(subscription),
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const result = await response.json();
    console.log('📥 Server response:', result);
    
    alert('✅ Đã đăng ký nhận thông báo thành công!');
    
  } catch (error) {
    console.error('💥 Subscription error:', error);
    alert('❌ Lỗi đăng ký thông báo: ' + error.message);
  }
}

// Gắn sự kiện cho nút đăng ký
document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('subscribe-btn');
  if (btn) {
    btn.addEventListener('click', subscribeToPushNotifications);
  }
});