/// public/client.js: File này chạy trên trang web để đăng ký Service Worker và gửi subscription lên server
// Thay thế bằng Public VAPID Key của bạn (có thể nhúng trực tiếp hoặc lấy từ server)
const publicVapidKey = 'BHApebDW1nYGCIzZVc4zgo1sLt5-acXCIEze31DCI35rVH8QguKr45DcgksFPwJS86eC6fiIuRjo_1rzJEHWaV8'; // Hoặc truy vấn từ server

// Hàm chuyển đổi Base64 URL safe sang Uint8Array[citation:1]
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

// Hàm chính đăng ký nhận thông báo
async function subscribeToPushNotifications() {
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
    console.warn('Push notifications are not supported by this browser.');
    return;
  }
  
  try {
    // 1. Đăng ký Service Worker
    const registration = await navigator.serviceWorker.register('/sw.js');
    console.log('Service Worker registered.');
    
    // 2. Yêu cầu quyền hiển thị thông báo
    const permission = await Notification.requestPermission();
    if (permission !== 'granted') {
      throw new Error('Permission not granted for Notifications');
    }
    
    // 3. Đăng ký Push với VAPID key
    const subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(publicVapidKey)
    });
    
    // 4. Gửi đối tượng subscription lên server Node.js để lưu trữ
    const response = await fetch('/subscribe', {
      method: 'POST',
      body: JSON.stringify(subscription),
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    if (response.ok) {
      console.log('Successfully subscribed to push notifications!');
    } else {
      console.error('Failed to save subscription on server.');
    }
    
  } catch (error) {
    console.error('Error during push notification setup:', error);
  }
}

// Gọi hàm đăng ký khi trang load (hoặc gọi bằng nút bấm để UX tốt hơn)
window.addEventListener('load', () => {
  // subscribeToPushNotifications(); // Có thể bật lại sau
});

// Ví dụ: Gắn vào một nút bấm trên giao diện
document.getElementById('subscribe-btn')?.addEventListener('click', subscribeToPushNotifications);