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
    // Kiểm tra permission trước
    const permission = await Notification.requestPermission();
    console.log('🔔 Notification permission:', permission);
    
    if (permission !== 'granted') {
      alert('Vui lòng cho phép thông báo trong trình duyệt!');
      return;
    }
    
    // Đăng ký Service Worker
    console.log('📝 Registering Service Worker...');
    const registration = await navigator.serviceWorker.register('/sw.js');
    console.log('✅ Service Worker registered:', registration);
    
    // Đợi Service Worker active
    await registration.active;
    console.log('🚀 Service Worker is active');
    
    // Subscribe với Push Manager
    console.log('🔐 Subscribing to push...');
    const subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(publicVapidKey)
    });
    
    console.log('📄 Subscription object:', JSON.stringify(subscription, null, 2));
    
    // Gửi lên server
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
    
    alert('Đã đăng ký nhận thông báo thành công!');
    
  } catch (error) {
    console.error('💥 Subscription error:', error);
    alert('Lỗi đăng ký thông báo: ' + error.message);
  }
}