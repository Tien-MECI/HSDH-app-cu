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

// Biến toàn cục
let publicVapidKey = window.publicVapidKey || '';

async function subscribeToPushNotifications() {
  console.log('🔄 Bắt đầu đăng ký push notification...');
  
  // Lấy username từ input
  const usernameInput = document.getElementById('username-input');
  const username = usernameInput ? usernameInput.value.trim().toUpperCase() : '';
  
  if (!username) {
    alert('Vui lòng nhập Tên đăng nhập (VD: MC005)');
    return;
  }
  
  // Validate username format (MC + 3 số)
  if (!/^MC\d{3}$/.test(username)) {
    alert('Tên đăng nhập phải có dạng MC + 3 số (VD: MC005, MC010, MC034)');
    return;
  }
  
  console.log(`👤 Đang đăng ký cho user: ${username}`);
  
  if (!('serviceWorker' in navigator)) {
    console.error('❌ Trình duyệt không hỗ trợ Service Worker');
    alert('Trình duyệt không hỗ trợ Service Worker. Vui lòng dùng Chrome/Edge/Firefox mới nhất.');
    return;
  }
  
  if (!('PushManager' in window)) {
    console.error('❌ Trình duyệt không hỗ trợ Push API');
    alert('Trình duyệt không hỗ trợ Push Notifications.');
    return;
  }
  
  try {
    // Lấy publicVapidKey nếu chưa có
    if (!publicVapidKey) {
      try {
        const response = await fetch('/get-vapid-key');
        const data = await response.json();
        publicVapidKey = data.publicKey;
        console.log('🔑 Đã lấy VAPID key từ server');
      } catch (err) {
        console.error('Không lấy được VAPID key:', err);
        alert('Không thể kết nối đến server. Vui lòng thử lại.');
        return;
      }
    }
    
    // Kiểm tra và xin quyền thông báo
    const permission = await Notification.requestPermission();
    console.log('🔔 Trạng thái quyền thông báo:', permission);
    
    if (permission !== 'granted') {
      alert('Bạn cần cho phép thông báo để sử dụng tính năng này!');
      return;
    }
    
    // Đăng ký Service Worker
    console.log('📝 Đang đăng ký Service Worker...');
    const registration = await navigator.serviceWorker.register('/sw.js');
    console.log('✅ Đã đăng ký Service Worker');
    
    // Đợi Service Worker active
    const serviceWorker = registration.installing || registration.waiting || registration.active;
    if (serviceWorker.state !== 'activated') {
      console.log('⏳ Đang đợi Service Worker kích hoạt...');
      await new Promise(resolve => {
        serviceWorker.addEventListener('statechange', () => {
          if (serviceWorker.state === 'activated') {
            console.log('🚀 Service Worker đã kích hoạt');
            resolve();
          }
        });
      });
    }
    
    // Subscribe với Push Manager
    console.log('🔐 Đang đăng ký nhận push...');
    const subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(publicVapidKey)
    });
    
    console.log('📄 Đã tạo subscription thành công');
    
    // Gửi subscription + username lên server
    console.log('📤 Đang gửi subscription lên server...');
    const response = await fetch('/subscribe', {
      method: 'POST',
      body: JSON.stringify({
        subscription: subscription,
        username: username
      }),
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    const result = await response.json();
    console.log('📥 Phản hồi từ server:', result);
    
    if (response.ok && result.success) {
      alert(`✅ Đã đăng ký nhận thông báo thành công cho ${username}!`);
      
      // Hiển thị thông báo test ngay sau khi đăng ký
      try {
        await registration.showNotification('Đăng ký thành công!', {
          body: `Bạn sẽ nhận thông báo khi có đơn hàng mới.`,
          icon: '/default-icon.png',
          tag: 'welcome-notification'
        });
      } catch (err) {
        console.log('Không hiển thị được thông báo chào mừng:', err);
      }
    } else {
      alert(`❌ Đăng ký thất bại: ${result.error || 'Lỗi không xác định'}`);
    }
    
  } catch (error) {
    console.error('💥 Lỗi trong quá trình đăng ký:', error);
    
    if (error.name === 'NotAllowedError') {
      alert('❌ Bạn đã từ chối quyền thông báo. Vui lòng cấp quyền trong cài đặt trình duyệt.');
    } else if (error.name === 'InvalidStateError') {
      alert('❌ Bạn đã đăng ký rồi. Nếu muốn đăng ký lại, hãy xóa cache trình duyệt.');
    } else {
      alert('❌ Lỗi đăng ký thông báo: ' + error.message);
    }
  }
}

// Gắn sự kiện cho nút đăng ký
document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('subscribe-btn');
  if (btn) {
    btn.addEventListener('click', subscribeToPushNotifications);
  }
});