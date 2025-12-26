// config/userGroups.js
export const userGroups = {
  // Nhóm phê duyệt đơn hàng
  APPROVAL_GROUP: ['MC005', 'MC010', 'MC011'],
  
  // Nhóm tiếp nhận sản xuất
  PRODUCTION_GROUP: ['MC034', 'MC035'],
  
  // Có thể thêm các nhóm khác ở đây
  ACCOUNTING_GROUP: ['MC020', 'MC021'],
  
  // Mặc định: gửi cho tất cả (nếu cần)
  ALL_USERS: 'ALL'
};

// Ánh xạ trạng thái -> nhóm cần thông báo
export const notificationRules = {
  // Khi tạo đơn hàng mới
  'Đơn hàng': {
    titleTemplate: (data) => `Đơn hàng mới: ${data.ma_dh}`,
    bodyTemplate: (data) => `Khách hàng: ${data.ten_kh}`,
    targetGroups: ['APPROVAL_GROUP'], // Chỉ nhóm phê duyệt
    alsoNotifyCreator: false
  },
  
  // Khi phê duyệt
  'Phê duyệt': {
    titleTemplate: (data) => `Đơn hàng đã duyệt: ${data.ma_dh}`,
    bodyTemplate: (data) => `Đã được ${data.nguoi_phe_duyet} phê duyệt`,
    targetGroups: ['creator'], // Người tạo đơn
    additionalGroups: ['PRODUCTION_GROUP'] // Và nhóm sản xuất
  },
  
  // Khi tiếp nhận sản xuất
  'Kế hoạch sản xuất': {
    titleTemplate: (data) => `Đơn hàng đã tiếp nhận: ${data.ma_dh}`,
    bodyTemplate: (data) => `Đã chuyển sang sản xuất`,
    targetGroups: ['creator', 'APPROVAL_GROUP']
  },
  
  // Khi hủy đơn
  'Hủy đơn': {
    titleTemplate: (data) => `Đơn hàng đã hủy: ${data.ma_dh}`,
    bodyTemplate: (data) => `Lý do: ${data.ly_do_huy || 'Không xác định'}`,
    targetGroups: ['creator', 'APPROVAL_GROUP']
  }
};