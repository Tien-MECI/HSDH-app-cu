import dotenv from "dotenv";
import express from "express";
import { google } from "googleapis";
import path from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import ejs from "ejs";
import fetch from "node-fetch";
import { promisify } from "util";
import webPush from 'web-push';
import { prepareYcvtData } from './ycvt.js';
import { preparexkvtData } from './xuatvattu.js';
import { buildAttendanceData } from "./helpers/chamcong.js";
import fs from 'fs/promises';
import { existsSync } from 'fs';
import { userGroups, notificationRules } from './config/userGroups.js';
import { v4 as uuidv4 } from 'uuid';

const renderFileAsync = promisify(ejs.renderFile);
const app = express();

// --- CORS middleware thay vì dùng package cors ---
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});



// --- QUAN TRỌNG: Thêm middleware để parse form data ---
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.set("view engine", "ejs");
app.set("views", "./views");
app.use(express.static('public'));

// Đăng ký helper functions cho EJS
app.locals.formatNumber2 = function(num) {
    if (typeof num !== 'number') {
        num = parseFloat(num);
        if (isNaN(num)) return '0';
    }
    return num.toLocaleString('vi-VN', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    });
};

function formatNumber(num) {
  if (!num) return "0";
  num = Math.abs(num); // luôn lấy giá trị dương
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
}

app.locals.formatDate = function(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return dateStr;
    return date.toLocaleDateString('vi-VN');
};


dotenv.config();

// --- __dirname trong ESM ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// --- IDs file Drive ---
const LOGO_FILE_ID = "1Rwo4pJt222dLTXN9W6knN3A5LwJ5TDIa";
const WATERMARK_FILE_ID = "1fNROb-dRtRl2RCCDCxGPozU3oHMSIkHr";
const WATERMARK_FILEHOADON_ID = "1skm9AI1_rrx7ngZrgsyEuy_YbnOXVMIK";
const WATERMARK_FILEBAOHANH_ID = "1hwTP3Vmghybml3eT6ZGG8pGVmP6fnfvJ";

// --- ENV ---
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const SPREADSHEET_QC_TT_ID = process.env.SPREADSHEET_QC_TT_ID;
const SPREADSHEET_HC_ID = process.env.SPREADSHEET_HC_ID;
const SPREADSHEET_BOM_ID = process.env.SPREADSHEET_BOM_ID;
const SPREADSHEET_KHVT_ID = process.env.SPREADSHEET_KHVT_ID;
const GAS_WEBAPP_URL = process.env.GAS_WEBAPP_URL;
const GAS_WEBAPP_URL_BBNT = process.env.GAS_WEBAPP_URL_BBNT;
const GOOGLE_CREDENTIALS_B64 = process.env.GOOGLE_CREDENTIALS_B64;
const GAS_WEBAPP_URL_BBSV = process.env.GAS_WEBAPP_URL_BBSV;
const GAS_WEBAPP_URL_DNC = process.env.GAS_WEBAPP_URL_DNC;
const GAS_WEBAPP_URL_PYCVT = process.env.GAS_WEBAPP_URL_PYCVT;

// --- Web Push VAPID Keys ---
const publicVapidKey = process.env.PUBLIC_VAPID_KEY;
const privateVapidKey = process.env.PRIVATE_VAPID_KEY;

if (!SPREADSHEET_ID || !SPREADSHEET_HC_ID || !SPREADSHEET_QC_TT_ID || !GAS_WEBAPP_URL || !GAS_WEBAPP_URL_BBNT || !GOOGLE_CREDENTIALS_B64 || !GAS_WEBAPP_URL_BBSV || !GAS_WEBAPP_URL_DNC) {
    console.error(
        "❌ Thiếu biến môi trường: SPREADSHEET_ID / SPREADSHEET_HC_ID / GAS_WEBAPP_URL / GAS_WEBAPP_URL_BBNT / GOOGLE_CREDENTIALS_B64 / GAS_WEBAPP_URL_BBSV / GAS_WEBAPP_URL_DNC"
    );
    process.exit(1);
}

if (!publicVapidKey || !privateVapidKey) {
    console.error("❌ Thiếu biến môi trường PUBLIC_VAPID_KEY hoặc PRIVATE_VAPID_KEY");
    process.exit(1);
}

// Cần đặt email hợp lệ để liên hệ khi có sự cố[citation:1][citation:3]
webPush.setVapidDetails('mailto:tech@meci.vn', publicVapidKey, privateVapidKey);

// --- Giải mã Service Account JSON ---
const credentials = JSON.parse(
    Buffer.from(GOOGLE_CREDENTIALS_B64, "base64").toString("utf-8")
);
credentials.private_key = credentials.private_key.replace(/\\n/g, "\n").trim();

// --- Google Auth ---
const scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
];
const auth = new google.auth.JWT(
    credentials.client_email,
    null,
    credentials.private_key,
    scopes
);
const sheets = google.sheets({ version: "v4", auth });
const drive = google.drive({ version: "v3", auth });

// --- Express ---
const PORT = process.env.PORT || 3000;

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

// --- Lưu trữ subscriptions (tạm thời trong bộ nhớ, và đồng bộ với file) ---
const SUBSCRIPTIONS_FILE = './subscriptions.json';
let pushSubscriptions = []; // Mỗi subscription có: {endpoint, username, createdAt}

// Helper function: Lấy danh sách usernames từ các group
function getTargetUsernames(rule, data) {
  let usernames = [];
  
  // Xử lý các targetGroups từ rule
  rule.targetGroups.forEach(group => {
    if (group === 'creator' && data.nguoi_tao) {
      usernames.push(data.nguoi_tao);
    } else if (userGroups[group]) {
      usernames = usernames.concat(userGroups[group]);
    }
  });
  
  // Xử lý additionalGroups nếu có
  if (rule.additionalGroups) {
    rule.additionalGroups.forEach(group => {
      if (userGroups[group]) {
        usernames = usernames.concat(userGroups[group]);
      }
    });
  }
  
  // Loại bỏ trùng lặp và trả về
  return [...new Set(usernames)];
}

// Hàm load subscriptions từ file Phục vụ đăng ký nhận pushweb
async function loadSubscriptions() {
  try {
    if (existsSync(SUBSCRIPTIONS_FILE)) {
      const data = await fs.readFile(SUBSCRIPTIONS_FILE, 'utf8');
      pushSubscriptions = JSON.parse(data);
      console.log(`✅ Loaded ${pushSubscriptions.length} subscriptions from file`);
    }
  } catch (err) {
    console.error('Error loading subscriptions:', err);
  }
}

// Hàm save subscriptions (lưu Phục vụ đăng ký nhận pushweb)
async function saveSubscriptions() {
  try {
    await fs.writeFile(SUBSCRIPTIONS_FILE, JSON.stringify(pushSubscriptions, null, 2));
    console.log(`💾 Saved ${pushSubscriptions.length} subscriptions to file`);
  } catch (err) {
    console.error('Error saving subscriptions:', err);
  }
}

// Tải subscriptions khi khởi động
loadSubscriptions();
// === Hàm tải ảnh từ Google Drive về base64 (tự động xử lý export khi cần) ===
async function loadDriveImageBase64(fileId) {
  try {
    // 1️⃣ Lấy metadata để biết mimeType
    const metaRes = await drive.files.get({
      fileId,
      fields: "id, name, mimeType",
    });
    const mimeType = metaRes.data.mimeType || "";
    console.log(`📁 [Drive] File meta: ${metaRes.data.name} (${mimeType})`);

    // 2️⃣ Nếu là file ảnh gốc (PNG, JPEG, ...), tải trực tiếp
    if (mimeType.startsWith("image/")) {
      const bin = await drive.files.get(
        { fileId, alt: "media" },
        { responseType: "arraybuffer" }
      );
      const buffer = Buffer.from(bin.data);
      return `data:${mimeType};base64,${buffer.toString("base64")}`;
    }

    // 3️⃣ Nếu là file Google Docs / Slides / Drawings → export sang PNG
    if (mimeType.startsWith("application/vnd.google-apps")) {
      console.log("ℹ️ File không phải ảnh gốc — thử export sang PNG...");
      const exported = await drive.files.export(
        { fileId, mimeType: "image/png" },
        { responseType: "arraybuffer" }
      );
      const buffer = Buffer.from(exported.data);
      return `data:image/png;base64,${buffer.toString("base64")}`;
    }

    // 4️⃣ Các loại khác (PDF, ...), cũng cho phép tải nếu Drive hỗ trợ alt:media
    const bin = await drive.files.get(
      { fileId, alt: "media" },
      { responseType: "arraybuffer" }
    );
    const buffer = Buffer.from(bin.data);
    return `data:${mimeType};base64,${buffer.toString("base64")}`;
  } catch (err) {
    console.error(`❌ Không tải được file Drive ${fileId}:`, err.message);
    return "";
  }
}

app.locals.formatDateTime = function(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return dateStr;
    return date.toLocaleString('vi-VN');
};

// Helper để lấy tuần, tháng, quý từ ngày
app.locals.getWeekNumber = function(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 4 - (d.getDay() || 7));
    const yearStart = new Date(d.getFullYear(), 0, 1);
    const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    return weekNo;
};
app.locals.getQuarter = function(date) {
    const month = new Date(date).getMonth() + 1;
    return Math.ceil(month / 3);
};



// --- Routes ---
app.get("/", (_req, res) => res.send("🚀 Server chạy ổn!"));


//---gghnk------
app.get("/gghnk", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất GGHNK ...");

        // --- Lấy mã đơn hàng ---
        const gghRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "File_GGH_ct!B:B",
        });
        const colB = gghRes.data.values ? gghRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet File_GGH_ct.");

        console.log(`✔️ Mã đơn hàng: ${maDonHang} (dòng ${lastRowWithData})`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);
        if (!donHang)
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Logo ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);

        // --- Render ngay cho client ---
        res.render("gghnk", {
            donHang,
            logoBase64,
            autoPrint: false,
            maDonHang,
            pathToFile: ""
        });

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "gghnk.ejs"),
                    {
                        donHang,
                        logoBase64,
                        autoPrint: false,
                        maDonHang,
                        pathToFile: ""
                    }
                );

                // Gọi GAS webapp tương ứng (cần thêm biến môi trường GAS_WEBAPP_URL_GGH)
                const GAS_WEBAPP_URL_GGH = process.env.GAS_WEBAPP_URL_GGH;
                if (GAS_WEBAPP_URL_GGH) {
                    const resp = await fetch(GAS_WEBAPP_URL_GGH, {
                        method: "POST",
                        headers: { "Content-Type": "application/x-www-form-urlencoded" },
                        body: new URLSearchParams({
                            orderCode: maDonHang,
                            html: renderedHtml
                        })
                    });

                    const data = await resp.json();
                    console.log("✔️ AppScript trả về:", data);

                    const pathToFile = data.pathToFile || `GGH/${data.fileName}`;
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `File_GGH_ct!D${lastRowWithData}`,
                        valueInputOption: "RAW",
                        requestBody: { values: [[pathToFile]] },
                    });
                    console.log("✔️ Đã ghi đường dẫn:", pathToFile);
                } else {
                    console.log("⚠️ Chưa cấu hình GAS_WEBAPP_URL_GGH");
                }

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất GGH:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

// --- Route /dntu-<ma> ---
app.get("/dntu-:ma", async (req, res) => {
  try {
    const maTamUng = req.params.ma;
    console.log("▶️ Xuất giấy đề nghị tạm ứng:", maTamUng);

    // Lấy dữ liệu sheet
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_HC_ID,
      range: "data_tam_ung_thanh_toan!A:Z",
    });
    const rows = resp.data.values || [];
    const data = rows.slice(1);

    // Tìm dòng có cột H (index 7) == maTamUng
    const record = data.find((r) => r[7] === maTamUng);
    if (!record) {
      return res.send("❌ Không tìm thấy mã tạm ứng: " + maTamUng);
    }

    // Map dữ liệu theo form
    const formData = {
      maTamUng: record[7],     // H
      ngayTamUng: formatVietnameseDate(record[4]),   // E
      ten: record[2],          // C
      boPhan: record[3],       // D
      soTien: formatNumber(record[9]),       // J
      soTienChu: numberToWords(record[9]),
      lyDo: record[8],         // I
      taikhoannhantu: record[11], //J
      thoiHan: record[12],     // M
    };

    // Logo + watermark
    const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
    const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

    // Render EJS
    res.render("dntu", {
      formData,
      logoBase64,
      watermarkBase64,
      autoPrint: true,
    });
  } catch (err) {
    console.error("❌ Lỗi DNTU:", err.stack || err.message);
    res.status(500).send("Lỗi server: " + (err.message || err));
  }
});

// --- Route /dnhu-<ma> ---
app.get("/dnhu-:ma", async (req, res) => {
  try {
    const maTamUng = req.params.ma;
    console.log("▶️ Xuất giấy đề nghị tạm ứng:", maTamUng);

    // Lấy dữ liệu sheet
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_HC_ID,
      range: "data_tam_ung_thanh_toan!A:AF",
    });
    const rows = resp.data.values || [];
    const data = rows.slice(1);

    // Tìm dòng có cột H (index 7) == maTamUng
    const record = data.find((r) => r[7] === maTamUng);
    if (!record) {
      return res.send("❌ Không tìm thấy mã tạm ứng: " + maTamUng);
    }

    // Map dữ liệu theo form
    const formData = {
      maTamUng: record[7],     // H
      ngayhoanUng: formatVietnameseDate(record[23]),   // E
      ten: record[27],          // C
      boPhan: record[3],       // D
      soTien: formatNumber(record[9]),       // J
      soTienChu: numberToWords(record[9]),
      lyDo: record[22],         // I
    };

    // Logo + watermark
    const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
    const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

    // Render EJS
    res.render("dnhu", {
      formData,
      logoBase64,
      watermarkBase64,
      autoPrint: true,
    });
  } catch (err) {
    console.error("❌ Lỗi DNHU:", err.stack || err.message);
    res.status(500).send("Lỗi server: " + (err.message || err));
  }
});

// --- Route /dntt-<ma> ---
app.get("/dntt-:ma", async (req, res) => {
  try {
    const maTamUng = req.params.ma;
    console.log("▶️ Xuất giấy đề nghị thanh toán:", maTamUng);

    // Lấy dữ liệu sheet
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_HC_ID,
      range: "data_tam_ung_thanh_toan!A:AF",
    });
    const rows = resp.data.values || [];
    const data = rows.slice(1);

    // Tìm dòng có cột H (index 7) == maTamUng
    const record = data.find((r) => r[7] === maTamUng);
    if (!record) {
      return res.send("❌ Không tìm thấy mã tạm ứng: " + maTamUng);
    }

    // Map dữ liệu theo form
    const formData = {
      maTamUng: record[7],     // H
      ngayhoanUng: formatVietnameseDate(record[23]),   // E
      ten: record[27],          // C
      boPhan: record[3],       // D
      soTientu: formatNumber(record[9]),       // J
      soTientuChu: numberToWords(record[9]),
      soTienthucchi: formatNumber(record[24]),       // J
      soTienthucchiChu: numberToWords(record[24]),
      soTienthanhtoan: formatNumber(record[29]),       // J
      soTienthanhtoanChu: numberToWords(record[29]),
      lyDo: record[22],        // I
      sotknhantien: record[28],
    };

    // Logo + watermark
    const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
    const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

    // Render EJS
    res.render("dntt", {
      formData,
      logoBase64,
      watermarkBase64,
      autoPrint: true,
    });
  } catch (err) {
    console.error("❌ Lỗi DNTT:", err.stack || err.message);
    res.status(500).send("Lỗi server: " + (err.message || err));
  }
});

// --- Route /bbsv ---
app.get("/bbsv/madonhang-:madonhang/solan-:solan", async (req, res) => {
    try {
        const { madonhang, solan } = req.params;
        console.log(`▶️ Bắt đầu xuất BBSV với mã đơn: ${madonhang}, số lần: ${solan}`);

        // --- Lấy dữ liệu từ sheet Bien_ban_su_viec ---
        const bbsvDetailRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Bien_ban_su_viec!A:Z",
        });
        
        const bbsvRows = bbsvDetailRes.data.values || [];
        if (bbsvRows.length < 2) {
            return res.send("⚠️ Không có dữ liệu trong sheet Bien_ban_su_viec.");
        }
        
        const bbsvData = bbsvRows.slice(1);
        
        // Tìm dòng có mã đơn hàng và số lần khớp
        const bbsvRecord = bbsvData.find((r) => 
            String(r[1]) === String(madonhang) && String(r[2]) === String(solan)
        );
        
        if (!bbsvRecord) {
            return res.send(`❌ Không tìm thấy biên bản sự việc với mã: ${madonhang} và số lần: ${solan}`);
        }

        const maBBSV = bbsvRecord[1];
        const rowIndex = bbsvData.indexOf(bbsvRecord) + 2; // +2 vì bỏ header (+1) và index bắt đầu từ 0 (+1)
        
        console.log(`✔️ Mã biên bản sự việc: ${maBBSV} (dòng ${rowIndex})`);

        // --- Lấy dữ liệu từ sheet Don_hang ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A:Z",
        });
        
        const donHangRows = donHangRes.data.values || [];
        const donHangData = donHangRows.slice(1);
        const donHangRecord = donHangData.find((r) => r[5] === maBBSV || r[6] === maBBSV);

        // Xử lý ngày lập biên bản
        let ngayLapBB = bbsvRecord[9] || ''; // Cột J (index 9)
        if (ngayLapBB) {
            if (typeof ngayLapBB === 'string' && ngayLapBB.includes('/')) {
                const parts = ngayLapBB.split('/');
                if (parts.length === 3) {
                    ngayLapBB = `ngày ${parts[0]} tháng ${parts[1]} năm ${parts[2]}`;
                }
            } else if (ngayLapBB instanceof Date) {
                ngayLapBB = `ngày ${ngayLapBB.getDate()} tháng ${ngayLapBB.getMonth() + 1} năm ${ngayLapBB.getFullYear()}`;
            }
        }

        // Xử lý ngày yêu cầu xử lý
        let ngayYeuCauXuLy = bbsvRecord[8] || ''; // Cột I (index 8)
        if (ngayYeuCauXuLy) {
            if (typeof ngayYeuCauXuLy === 'string' && ngayYeuCauXuLy.includes('/')) {
                // Giữ nguyên định dạng dd/mm/yyyy
            } else if (ngayYeuCauXuLy instanceof Date) {
                const day = String(ngayYeuCauXuLy.getDate()).padStart(2, '0');
                const month = String(ngayYeuCauXuLy.getMonth() + 1).padStart(2, '0');
                const year = ngayYeuCauXuLy.getFullYear();
                ngayYeuCauXuLy = `${day}/${month}/${year}`;
            }
        }

        // Tách danh sách người liên quan
        const nguoiLienQuanList = (bbsvRecord[5] || '').split(',').map(name => name.trim());

        // Logo & Watermark
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // Render ngay cho client
        res.render("bbsv", {
            maBBSV,
            ngayLapBB,
            donHang: donHangRecord ? donHangRecord[6] : '', // Cột G (index 6)
            nguoiLapBB: bbsvRecord[3] || '', // Cột D (index 3)
            boPhanLienQuan: bbsvRecord[4] || '', // Cột E (index 4)
            nguoiLienQuanList,
            suViec: bbsvRecord[6] || '', // Cột G (index 6)
            xuLy: bbsvRecord[7] || '', // Cột H (index 7)
            ngayYeuCauXuLy,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            pathToFile: ""
        });

        // Sau khi render xong thì gọi AppScript ngầm
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbsv.ejs"),
                    {
                        maBBSV,
                        ngayLapBB,
                        donHang: donHangRecord ? donHangRecord[6] : '',
                        nguoiLapBB: bbsvRecord[3] || '',
                        boPhanLienQuan: bbsvRecord[4] || '',
                        nguoiLienQuanList,
                        suViec: bbsvRecord[6] || '',
                        xuLy: bbsvRecord[7] || '',
                        ngayYeuCauXuLy,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        pathToFile: ""
                    }
                );

                // Gọi Google Apps Script web app để tạo PDF
                const resp = await fetch(GAS_WEBAPP_URL_BBSV, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        maBBSV: maBBSV,
                        html: renderedHtml
                    })
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                // Cập nhật đường dẫn file vào sheet - sử dụng rowIndex đã xác định
                const pathToFile = data.pathToFile || `BBSV/${data.fileName}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `Bien_ban_su_viec!K${rowIndex}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log("✔️ Đã ghi đường dẫn:", pathToFile, "vào dòng", rowIndex);

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBSV:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

// --- Route /dnc ---
app.get("/dnc", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Đề Nghị Chung ...");

        // --- Lấy mã đơn hàng từ sheet De_nghi_chung ---
        const dncRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "De_nghi_chung!B:B",
        });
        const colB = dncRes.data.values ? dncRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet De_nghi_chung.");

        console.log(`✔️ Mã đơn hàng: ${maDonHang} (dòng ${lastRowWithData})`);

        // --- Lấy dữ liệu từ sheet De_nghi_chung ---
        const dncDetailRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "De_nghi_chung!A:Z",
        });
        const dncRows = dncDetailRes.data.values || [];
        const dncData = dncRows.slice(1);
        const dncRecords = dncData.filter((r) => r[1] === maDonHang);
        
        if (dncRecords.length === 0)
            return res.send("❌ Không tìm thấy đề nghị chung với mã: " + maDonHang);

        // --- Lấy dữ liệu từ sheet Don_hang ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A:Z",
        });
        const donHangRows = donHangRes.data.values || [];
        const donHangData = donHangRows.slice(1);
        const donHangRecord = donHangData.find((r) => r[6] === maDonHang);

        if (!donHangRecord)
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // Xử lý ngày lập
        let ngayLap = donHangRecord[1] || ''; // Cột B (index 1)
        if (ngayLap && ngayLap instanceof Date) {
            ngayLap = Utilities.formatDate(ngayLap, Session.getScriptTimeZone(), 'dd/MM/yyyy');
        }

        // Xử lý ngày yêu cầu thực hiện
        let ngayYeuCauThucHien = '';
        for (const record of dncRecords) {
            if (record[9]) { // Cột J (index 9)
                ngayYeuCauThucHien = record[9];
                if (ngayYeuCauThucHien instanceof Date) {
                    ngayYeuCauThucHien = Utilities.formatDate(ngayYeuCauThucHien, Session.getScriptTimeZone(), 'dd/MM/yyyy');
                }
                break;
            }
        }

        // Xác định các cột có dữ liệu
        const columns = [5, 6, 7, 8, 14, 9, 11]; // Cột F, G, H, I, O, J, L
        const headers = [
            "Mã ĐH chi tiết", "Tên sản phẩm nhập lại", "Số lượng nhập lại", "Đơn vị tính",
            "Lý do hủy", "Địa điểm lấy hàng", "Hình thức xử lý sau nhập kho"
        ];

        // Lọc các cột có dữ liệu
        const filteredColumns = [];
        const filteredHeaders = [];
        
        for (let i = 0; i < columns.length; i++) {
            const colIndex = columns[i];
            const hasData = dncRecords.some(record => record[colIndex - 1] && record[colIndex - 1] !== '');
            
            if (hasData) {
                filteredColumns.push(colIndex);
                filteredHeaders.push(headers[i]);
            }
        }

        // Logo & Watermark
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // Render ngay cho client
        res.render("dnc", {
            maDonHang,
            donHangRecord,
            dncRecords,
            filteredColumns,
            filteredHeaders,
            ngayLap,
            ngayYeuCauThucHien,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            pathToFile: ""
        });

        // Sau khi render xong thì gọi AppScript ngầm
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "dnc.ejs"),
                    {
                        maDonHang,
                        donHangRecord,
                        dncRecords,
                        filteredColumns,
                        filteredHeaders,
                        ngayLap,
                        ngayYeuCauThucHien,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        pathToFile: ""
                    }
                );

                // Gọi Google Apps Script web app để tạo PDF
                const resp = await fetch(GAS_WEBAPP_URL_DNC, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                // Cập nhật đường dẫn file vào sheet
                const pathToFile = data.pathToFile || `DNC/${data.fileName}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `De_nghi_chung!O${lastRowWithData}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log("✔️ Đã ghi đường dẫn:", pathToFile);

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất Đề Nghị Chung:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//---YCVT-BOM---

app.get('/ycvt', async (req, res) => {
    try {
        console.log('▶️ Bắt đầu xuất YCVT ...');
        await new Promise(resolve => setTimeout(resolve, 2500));
        // Lấy logo và watermark
        const [logoBase64, watermarkBase64] = await Promise.all([
            loadDriveImageBase64(LOGO_FILE_ID),
            loadDriveImageBase64(WATERMARK_FILE_ID)
        ]);

        // Chuẩn bị dữ liệu
        const data = await prepareYcvtData(auth, SPREADSHEET_ID, SPREADSHEET_BOM_ID);
        const { d4Value, lastRowWithData } = data;

        // Render cho client
        res.render('ycvt', {
            ...data,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang: d4Value,
            pathToFile: ''
        });

        // Gọi Apps Script ngầm
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, 'views', 'ycvt.ejs'),
                    {
                        ...data,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang: d4Value,
                        pathToFile: ''
                    }
                );

                const resp = await fetch(GAS_WEBAPP_URL_PYCVT, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: new URLSearchParams({
                        orderCode: d4Value,
                        html: renderedHtml
                    })
                });

                const result = await resp.json();
                console.log('✔️ AppScript trả về:', result);

                if (!result.ok) {
                    throw new Error(result.error || 'Lỗi khi gọi Apps Script');
                }

                const pathToFile = result.pathToFile || `YCVT/${result.fileName}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_BOM_ct!D${lastRowWithData}`,
                    valueInputOption: 'RAW',
                    requestBody: { values: [[pathToFile]] }
                });
                console.log('✔️ Đã ghi đường dẫn:', pathToFile);
            } catch (err) {
                console.error('❌ Lỗi gọi AppScript:', err);
            }
        })();
    } catch (err) {
        console.error('❌ Lỗi khi xuất YCVT:', err.stack || err.message);
        res.status(500).send('Lỗi server: ' + (err.message || err));
    }
});

//---YCXKTP---

app.get('/ycxktp', async (req, res) => {
    try {
        console.log('▶️ Bắt đầu xuất YCXKTP ...');

        // 1) Lấy logo & watermark
        const [logoBase64, watermarkBase64] = await Promise.all([
            loadDriveImageBase64(LOGO_FILE_ID),
            loadDriveImageBase64(WATERMARK_FILE_ID)
        ]);

        // 2) Đọc dữ liệu 2 sheet: File_YC_XK_TP (để lấy last row) và Ke_hoach_thuc_hien (để lọc)
        const [ycxRes, keHoachRes] = await Promise.all([
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: 'File_YC_XK_TP',
                valueRenderOption: 'FORMATTED_VALUE'
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: 'Ke_hoach_thuc_hien',
                valueRenderOption: 'FORMATTED_VALUE'
            })
        ]);

        const ycxValues = ycxRes.data.values || [];
        const keHoachValues = keHoachRes.data.values || [];

        if (ycxValues.length <= 1) {
            console.warn('⚠️ File_YC_XK_TP không có dữ liệu (chỉ header).');
            // render một trang rỗng / thông báo
            return res.render('ycxktp', {
                ngayYC: '',
                tenNSTHValue: '',
                phuongTienValue: '',
                giaTriE: '',
                tableData: [],
                tongDon: 0,
                tongTaiTrong: 0,
                logoBase64,
                watermarkBase64,
                autoPrint: false,
                pathToFile: ''
            });
        }

        // last row index (1-based)
        const lastRowIndex = ycxValues.length;
        const lastRow = ycxValues[lastRowIndex - 1];

        // lấy giá trị từ File_YC_XK_TP (cột B, C, D, E tương ứng index 1..4)
        const ngayYC_raw = lastRow[1];
        const tenNSTHValue = lastRow[2] || '';
        const phuongTienValue = lastRow[3] || '';
        const giaTriE = lastRow[4] || '';

        // helper parse date string/serial -> Date
        function parseSheetDate(val) {
            if (val === null || val === undefined || val === '') return null;
            if (typeof val === 'number') {
                const epoch = new Date(Date.UTC(1899, 11, 30));
                return new Date(epoch.getTime() + Math.round(val * 24 * 60 * 60 * 1000));
            }
            const s = String(val).trim();
            const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
            if (m) {
                let [, dd, mm, yyyy, hh = '0', min = '0', ss = '0'] = m;
                if (yyyy.length === 2) yyyy = '20' + yyyy;
                return new Date(+yyyy, +mm - 1, +dd, +hh, +min, +ss);
            }
            const d = new Date(s);
            return isNaN(d) ? null : d;
        }

        const ngayYCObj = parseSheetDate(ngayYC_raw);
        const ngayYC = ngayYCObj ? ngayYCObj.toLocaleDateString('vi-VN') : String(ngayYC_raw || '');

        // 3) Filter dữ liệu từ Ke_hoach_thuc_hien giống Apps Script gốc
        // - so sánh ngày (dd/MM/yyyy), tenNSTH, phuong tien, và pxk === ""
        const filteredData = []; // mảng các rowToCopy
        let tongTaiTrong = 0;

        for (let i = 1; i < keHoachValues.length; i++) {
            const row = keHoachValues[i];
            if (!row) continue;

            const ngayTH_raw = row[1];    // cột B (index 1)
            const pxk = row[23];          // cột X (index 23) phải rỗng
            const phuongTien_kehoach = row[30]; // giữ index 35 giống AppScript gốc
            const tenNSTH_kehoach = row[36];

            const ngayTHObj = parseSheetDate(ngayTH_raw);
            if (!ngayTHObj) continue;
            const formattedNgayTH = ngayTHObj.toLocaleDateString('vi-VN');

            const condDate = formattedNgayTH === ngayYC;
            const condTen = String(tenNSTH_kehoach || '').toString() === String(tenNSTHValue || '').toString();
            const condPT = String(phuongTien_kehoach || '').toString() === String(phuongTienValue || '').toString();
            const condPXKEmpty = (pxk === '' || pxk === undefined || pxk === null);

            if (condDate && condTen && condPT && condPXKEmpty) {
                // dataToCopy giống AppScript: row[5], row[11], row[9], row[10], row[8], row[13], row[14], row[15]
                const dataToCopy = [
                    row[5],  // index 5
                    row[11], // index 11
                    row[9],  // index 9
                    row[10], // index 10
                    row[8],  // index 8
                    row[13], // index 13
                    row[14], // index 14
                    row[15]  // index 15 (tải trọng)
                ];
                filteredData.push(dataToCopy);

                const t = parseFloat(row[15]) || 0;
                tongTaiTrong += t;
            }
        }

        const tongDon = filteredData.length;

        // 4) Render cho client ngay (autoPrint: true)
        const renderForClientData = {
            ngayYC,
            tenNSTHValue,
            phuongTienValue,
            giaTriE,
            tableData: filteredData,
            tongDon,
            tongTaiTrong,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            pathToFile: ''
        };

        res.render('ycxktp', renderForClientData);

        // 5) Gọi GAS WebApp ngầm (IIFE) để convert HTML -> PDF, sau đó ghi đường dẫn vào sheet
        (async () => {
            try {
                // render HTML server-side bằng cùng template nhưng autoPrint: false
                const htmlToSend = await renderFileAsync(
                    path.join(__dirname, 'views', 'ycxktp.ejs'),
                    {
                        ...renderForClientData,
                        autoPrint: false,
                        pathToFile: ''
                    }
                );

                // file name chuẩn giống Apps Script
                const yyyy = ngayYCObj ? String(ngayYCObj.getFullYear()) : 'na';
                const mm = ngayYCObj ? String(ngayYCObj.getMonth() + 1).padStart(2, '0') : '00';
                const dd = ngayYCObj ? String(ngayYCObj.getDate()).padStart(2, '0') : '00';
                const ngayYCTEN = `${yyyy}-${mm}-${dd}`;
                const safeTen = String(tenNSTHValue || '').replace(/[\/\\:\*\?"<>\|]/g, '_').slice(0, 80);
                const safePT = String(phuongTienValue || '').replace(/[\/\\:\*\?"<>\|]/g, '_').slice(0, 60);
                const suggestedFileName = `${ngayYCTEN}_${safeTen}_${safePT}_Lần_${String(giaTriE || '')}.pdf`;

                const gasUrl = process.env.GAS_WEBAPP_URL_YCXKTP || process.env.GAS_WEBAPP_URL_PYCVT;
                if (!gasUrl) {
                    console.warn('⚠️ GAS_WEBAPP_URL_YCXKTP (hoặc GAS_WEBAPP_URL_PYCVT) chưa cấu hình - bỏ qua gửi Apps Script.');
                    return;
                }

                console.log('➡️ Gửi HTML tới GAS WebApp:', gasUrl);
                const resp = await fetch(gasUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: new URLSearchParams({
                        orderCode: suggestedFileName,
                        html: htmlToSend
                    })
                });

                const result = await resp.json();
                console.log('✔️ AppScript trả về:', result);

                if (!result || !result.ok) {
                    throw new Error(result?.error || 'Apps Script trả về lỗi hoặc không ok');
                }

                const pathToFile = result.pathToFile || (result.fileName ? `YCXKTP/${result.fileName}` : suggestedFileName);

                // Ghi đường dẫn file vào cột F của last row
                const updateRange = `File_YC_XK_TP!F${lastRowIndex}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: updateRange,
                    valueInputOption: 'RAW',
                    requestBody: { values: [[pathToFile]] }
                });

                console.log('✔️ Đã ghi đường dẫn:', pathToFile, 'vào', updateRange);
            } catch (err) {
                console.error('❌ Lỗi gọi AppScript (YCXKTP):', err.stack || err.message || err);
            }
        })();

    } catch (err) {
        console.error('❌ Lỗi khi xuất YCXKTP:', err.stack || err.message || err);
        res.status(500).send('Lỗi server: ' + (err.message || err));
    }
});

//---- KHNS ----

app.get('/khns/:ngayYC/:tenNSTH/:phuongTien/:sofile/:id', async (req, res) => {
  try {
    console.log('▶️ Bắt đầu xuất KHNS (theo URL params)...');

    // 1. LẤY PARAM TỪ URL VÀ DECODE
    const ngayYC = decodeURIComponent(req.params.ngayYC);       // dd_mm_yyyy
    const tenNSTHValue = decodeURIComponent(req.params.tenNSTH);
    const phuongTienValue = decodeURIComponent(req.params.phuongTien);
    const giaTriE = decodeURIComponent(req.params.sofile);
    const id = decodeURIComponent(req.params.id);

    console.log("📌 Params (decoded):", { ngayYC, tenNSTHValue, phuongTienValue, giaTriE, id });

    // 2. LẤY LOGO & WATERMARK
    const [logoBase64, watermarkBase64] = await Promise.all([
      loadDriveImageBase64(LOGO_FILE_ID),
      loadDriveImageBase64(WATERMARK_FILE_ID)
    ]);

    // 3. ĐỌC SHEET KẾ HOẠCH
    const keHoachRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Ke_hoach_thuc_hien',
      valueRenderOption: 'FORMATTED_VALUE'
    });

    const keHoachValues = keHoachRes.data.values || [];

    // 4. HÀM PARSE DATE - HỖ TRỢ dd/mm/yyyy, dd-mm-yyyy, dd_mm_yyyy
    function parseSheetDate(val) {
      if (!val) return null;

      if (typeof val === "number") {
        const epoch = new Date(Date.UTC(1899, 11, 30));
        return new Date(epoch.getTime() + Math.round(val * 86400000));
      }

      const s = String(val).trim();

      // match dd/mm/yyyy, dd-mm-yyyy, dd_mm_yyyy
      const m = s.match(/^(\d{1,2})[\/\-_](\d{1,2})[\/\-_](\d{2,4})$/);
      if (m) {
        let [, dd, mm, yyyy] = m;
        if (yyyy.length === 2) yyyy = "20" + yyyy;
        return new Date(+yyyy, +mm - 1, +dd);
      }

      const d = new Date(s);
      return isNaN(d) ? null : d;
    }

    // 5. PARSE NGÀY YC TỪ PARAM
    const ngayYCObj = parseSheetDate(ngayYC);
    const ngayYC_fmt = ngayYCObj ? ngayYCObj.toLocaleDateString("vi-VN") : ngayYC;

    // 6. LỌC DỮ LIỆU
    const filteredData = [];
    let tongTaiTrong = 0;
    let NSHotroArr = [];

    for (let i = 1; i < keHoachValues.length; i++) {
      const row = keHoachValues[i];
      if (!row) continue;

      const ngayTHObj = parseSheetDate(row[1]);
      if (!ngayTHObj) continue;

      // So sánh bằng object Date
      const condDate = ngayYCObj && ngayTHObj &&
        ngayYCObj.getFullYear() === ngayTHObj.getFullYear() &&
        ngayYCObj.getMonth() === ngayTHObj.getMonth() &&
        ngayYCObj.getDate() === ngayTHObj.getDate();

      const condTen = (row[26] || '') === tenNSTHValue;
      const condPT = (row[30] || '') === phuongTienValue;

      if (condDate && condTen && condPT) {
        const dataToCopy = [
          row[29] || '',
          row[5] || '',
          row[11] || '',
          row[9] || '',
          row[10] || '',
          row[8] || '',
          row[13] || '',
          row[14] || '',
          row[15] || '',
          ""
        ];

        filteredData.push(dataToCopy);
        tongTaiTrong += parseFloat(row[15]) || 0;

        if (row[28]) {
          const names = row[28].split(/[,;]/).map(n => n.trim()).filter(Boolean);
          NSHotroArr.push(...names);
        }
      }
    }

    const tongDon = filteredData.length;

    const groupedData = {};
    filteredData.forEach(r => {
      const loai = r[4] || "Không xác định";
      if (!groupedData[loai]) groupedData[loai] = [];
      groupedData[loai].push(r);
    });

    const NSHotroStr = [...new Set(NSHotroArr)].join(" , ");

    // 7. RENDER CHO CLIENT
    const renderForClientData = {
      ngayYC: ngayYC_fmt,
      tenNSTHValue,
      phuongTienValue,
      giaTriE,
      groupedData,
      tableData: filteredData,
      tongDon,
      tongTaiTrong,
      logoBase64,
      watermarkBase64,
      NSHotro: NSHotroStr,
      autoPrint: true,
      pathToFile: ''
    };

    res.render('khns', renderForClientData);

    // 8. GỌI GAS → TẠO PDF → SAU ĐÓ MỚI ĐỌC SHEET → GHI ID
    (async () => {
      try {
        const htmlToSend = await renderFileAsync(
          path.join(__dirname, 'views', 'khns.ejs'),
          { ...renderForClientData, autoPrint: false }
        );

        const yyyy = ngayYCObj?.getFullYear();
        const mm = String(ngayYCObj?.getMonth() + 1).padStart(2, '0');
        const dd = String(ngayYCObj?.getDate()).padStart(2, '0');
        const ngayYCTEN = `${yyyy}-${mm}-${dd}`;

        const gasUrl = process.env.GAS_WEBAPP_URL_KHNS;

        const resp = await fetch(gasUrl, {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            html: htmlToSend,
            ngayYCTEN,
            tenNSTHValue,
            phuongtienvanchuyenValue: phuongTienValue,
            giaTriE
          })
        });

        const result = await resp.json();
        if (!result?.ok) throw new Error(result?.error || "GAS trả về lỗi");

        const pathToFile = result.pathToFile || `KHNS/${result.fileName}`;
        console.log("📌 File đã tạo:", pathToFile);

        // SAU KHI CÓ pathToFile → ĐỌC LẠI SHEET → TÌM ID
        const fileRes2 = await sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: "File_KH_thuc_hien_NS",
          valueRenderOption: 'FORMATTED_VALUE'
        });

        const fileValues2 = fileRes2.data.values || [];

        let foundRow = -1;
        for (let i = 1; i < fileValues2.length; i++) {
          if (String(fileValues2[i][0]).trim() === id) {
            foundRow = i + 1;
            break;
          }
        }

        if (foundRow === -1) {
          console.error("❌ Không tìm thấy ID:", id);
          return;
        }

        const updateRange = `File_KH_thuc_hien_NS!F${foundRow}`;

        await sheets.spreadsheets.values.update({
          spreadsheetId: SPREADSHEET_ID,
          range: updateRange,
          valueInputOption: "RAW",
          requestBody: { values: [[pathToFile]] }
        });

        console.log(`✔️ Đã ghi path vào ${updateRange}`);

      } catch (err) {
        console.error("❌ Lỗi GAS KHNS:", err);
      }
    })();

  } catch (err) {
    console.error("❌ Lỗi server:", err);
    res.status(500).send("Lỗi server: " + err.message);
  }
});



// --- Route Dashboard ---

import { format } from "date-fns";

app.get("/dashboard", async (req, res) => {
  try {
    console.log("📊 Bắt đầu lấy dữ liệu Dashboard...");

    // range filter months from query: startMonth, endMonth (1..12)
    const startMonth = req.query.startMonth ? parseInt(req.query.startMonth, 10) : null;
    const endMonth = req.query.endMonth ? parseInt(req.query.endMonth, 10) : null;

    // Lọc nhân viên và phân trang cho phần bài đăng
    const baidangNhanVien = req.query.baidangNhanVien || 'all';
    const baidangPage = parseInt(req.query.baidangPage) || 1;
    const baidangPerPage = 10; // Mặc định 10 dòng/trang

    // load watermark (bạn đã có hàm loadDriveImageBase64)
    const [watermarkBase64] = await Promise.all([
      loadDriveImageBase64(WATERMARK_FILE_ID)
    ]);

    // ------------------ Helpers ------------------
    function parseMoney(value) {
      if (value === null || value === undefined || value === "") return 0;
      const s = String(value).trim();
      const hasDot = s.includes(".");
      const hasComma = s.includes(",");
      if (hasDot && hasComma) {
        // decide which is decimal by last occurrence
        return s.lastIndexOf(",") > s.lastIndexOf(".")
          ? parseFloat(s.replace(/\./g, "").replace(/,/g, ".")) || 0
          : parseFloat(s.replace(/,/g, "")) || 0;
      }
      if (hasDot && !hasComma) {
        const afterDot = s.split(".")[1] || "";
        return afterDot.length === 3
          ? parseFloat(s.replace(/\./g, "")) || 0
          : parseFloat(s) || 0;
      }
      if (!hasDot && hasComma) {
        const afterComma = s.split(",")[1] || "";
        return afterComma.length === 3
          ? parseFloat(s.replace(/,/g, "")) || 0
          : parseFloat(s.replace(",", ".")) || 0;
      }
      return parseFloat(s) || 0;
    }

    function parseNumber(value) {
      // for quantities: allow "1.000" or "1,000" or "1000"
      if (value === null || value === undefined || value === "") return 0;
      const s = String(value).trim();
      return parseFloat(s.replace(/\./g, "").replace(/,/g, ".")) || 0;
    }

    function parseSheetDate(val) {
      if (!val && val !== 0) return null;
      if (typeof val === "number") {
        // sheet serial -> JS Date
        const epoch = new Date(Date.UTC(1899, 11, 30));
        return new Date(epoch.getTime() + Math.round(val * 24 * 3600 * 1000));
      }
      const s = String(val).trim();
      // dd/mm/yyyy hh:mm:ss or dd/mm/yyyy
      const re1 = /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;
      const m1 = s.match(re1);
      if (m1) {
        let [, dd, mm, yyyy, hh = "0", mi = "0", ss = "0"] = m1;
        if (yyyy.length === 2) yyyy = '20' + yyyy;
        return new Date(+yyyy, +mm - 1, +dd, +hh, +mi, +ss);
      }
      // ISO fallback
      const d = new Date(s);
      if (!isNaN(d)) return d;
      return null;
    }

    // ------------------ Don_hang (doanh số theo NV) ------------------
    const donHangRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Don_hang",
      valueRenderOption: "FORMATTED_VALUE"
    });

    const donHangValues = donHangRes.data.values || [];
    const donHangRows = donHangValues.slice(1); // drop header

    const salesByNV = {};
    let soDonChot = 0, soDonHuy = 0;

    donHangRows.forEach(row => {
      const nhanVien = row[2] || "Không xác định";        // C
      const ngayDuyetRaw = row[49] || "";                 // AX
      const trangThai = String(row[43] || "").trim();     // AR (giữ nguyên)
      const baoGia = String(row[46] || "").trim();        // AU (giữ nguyên)
      // parseMoney trả về number (nếu NaN thì xem như 0)
      let giaTriDonHang = parseMoney(row[64]);
      if (!isFinite(giaTriDonHang)) giaTriDonHang = 0;

      const ngayObj = parseSheetDate(ngayDuyetRaw);
      if (startMonth && endMonth && ngayObj) {
        const th = ngayObj.getMonth() + 1;
        if (th < startMonth || th > endMonth) return;
      }

      if (!salesByNV[nhanVien]) {
        salesByNV[nhanVien] = {
          nhanVien,
          // 2 tổng phụ theo yêu cầu
          doanhSoKeHoach: 0,     // trạng thái === "Kế hoạch sản xuất"
          doanhSoSuaBanVe: 0,    // trạng thái === "Sửa bản vẽ"
          // tổng hợp = 1 + 2 (luôn cập nhật)
          tongDoanhSo: 0,

          // counters
          tongDon: 0,
          soDonChot: 0,       // count "Kế hoạch sản xuất"
          doanhSoChot: 0,     // giá trị chốt (tương tự doanhSoKeHoach)
          soDonHuy: 0,
          doanhSoHuy: 0,
          soBaoGia: 0
        };
      }

      const nv = salesByNV[nhanVien];
      nv.tongDon++;

      // Nếu trạng thái chính xác là "Kế hoạch sản xuất"
      if (trangThai === "Kế hoạch sản xuất") {
        nv.doanhSoKeHoach += giaTriDonHang;
        nv.soDonChot++;
        nv.doanhSoChot += giaTriDonHang;
        soDonChot++;
      }

      // Nếu trạng thái chính xác là "Sửa bản vẽ"
      if (trangThai === "Sửa bản vẽ") {
        nv.doanhSoSuaBanVe += giaTriDonHang;
      }

      // Đơn hủy
      if (trangThai === "Hủy đơn") {
        nv.soDonHuy++;
        nv.doanhSoHuy += giaTriDonHang;
        soDonHuy++;
      }

      // Báo giá (so sánh chính xác)
      if (baoGia === "Báo giá") {
        nv.soBaoGia++;
      }

      // Cập nhật tổng hợp = tổng 2 loại (kehoach + suabanve)
      nv.tongDoanhSo = (nv.doanhSoKeHoach || 0) + (nv.doanhSoSuaBanVe || 0);
    });

    const sales = Object.values(salesByNV).sort((a,b) => b.tongDoanhSo - a.tongDoanhSo);

    // ------------------ Don_hang_PVC_ct (top products by doanh so) ------------------
    const pvcRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Don_hang_PVC_ct",
      valueRenderOption: "FORMATTED_VALUE"
    });

    const pvcValues = pvcRes.data.values || [];
    const pvcRows = pvcValues.slice(1);

    const productsMap = {};
    pvcRows.forEach(row => {
      const ngayTaoRaw = row[29] || "";   // AD index 29 (user said AD is mm/dd/yyyy)
      const ngayObj = parseSheetDate(ngayTaoRaw);
      if (startMonth && endMonth && ngayObj) {
        const th = ngayObj.getMonth() + 1;
        if (th < startMonth || th > endMonth) return;
      }

      const maSP = row[7] || "N/A";       // H index 7
      const tenSP = row[8] || "Không tên"; // I index 8
      const soLuong = parseNumber(row[21]); // V index 21
      const donVi = row[22] || "";        // W index 22
      const giaTriPVC = parseMoney(row[27]); // AB index 27

      const key = maSP + "|" + tenSP;
      if (!productsMap[key]) productsMap[key] = { maSP, tenSP, soLuong: 0, donVi, doanhSo: 0 };
      productsMap[key].soLuong += soLuong;
      productsMap[key].doanhSo += giaTriPVC;
    });

    const topProducts = Object.values(productsMap)
      .sort((a,b) => b.doanhSo - a.doanhSo)
      .slice(0,10);

    // ------------------ Cham_soc_khach_hang (Báo cáo CSKH) ------------------
    const cskhRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Cham_soc_khach_hang",
      valueRenderOption: "FORMATTED_VALUE"
    });

    const cskhValues = cskhRes.data.values || [];
    const cskhRows = cskhValues.slice(1);

    const cskhMap = {}; // { nhanVien: { hinhThuc1: count, hinhThuc2: count, total: count } }
    const allHinhThuc = new Set();

    cskhRows.forEach(row => {
      const nhanVien = row[7] || "Không xác định";  // H cột nhân viên KD
      const ngayTao = row[5] || "";                 // F ngày tạo
      const hinhThuc = row[3] || "Không rõ";        // D hình thức liên hệ

      const ngayObj = parseSheetDate(ngayTao);
      if (startMonth && endMonth && ngayObj) {
        const th = ngayObj.getMonth() + 1;
        if (th < startMonth || th > endMonth) return;
      }

      allHinhThuc.add(hinhThuc);

      if (!cskhMap[nhanVien]) cskhMap[nhanVien] = { total: 0 };
      cskhMap[nhanVien][hinhThuc] = (cskhMap[nhanVien][hinhThuc] || 0) + 1;
      cskhMap[nhanVien].total++;
    });

    const cskhData = Object.entries(cskhMap).map(([nhanVien, data]) => ({
      nhanVien,
      ...data
    }));

    // Lưu danh sách tất cả hình thức để vẽ stacked chart
    const hinhThucList = Array.from(allHinhThuc);

    // ------------------ Bao_cao_bai_dang_ban_hang (Báo cáo đăng bài MXH) ------------------
    const baidangRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Bao_cao_bai_dang_ban_hang",
      valueRenderOption: "FORMATTED_VALUE"
    });

    const baidangValues = baidangRes.data.values || [];
    const baidangRows = baidangValues.slice(1);

    // Tổng hợp dữ liệu
    const baidangMap = {};
    const kenhBaiList = new Set();
    const allLinkList = [];
    const baidangNhanVienList = new Set();

    baidangRows.forEach(row => {
      const nhanVien = row[2] || "Không xác định"; // C
      const ngayTao = row[3] || "";               // D
      const kenhBai = row[4] || "Không rõ";       // E
      const link = row[5] || "";                  // F

      const ngayObj = parseSheetDate(ngayTao);
      if (startMonth && endMonth && ngayObj) {
        const th = ngayObj.getMonth() + 1;
        if (th < startMonth || th > endMonth) return;
      }

      baidangNhanVienList.add(nhanVien);
      kenhBaiList.add(kenhBai);

      if (!baidangMap[nhanVien]) {
        baidangMap[nhanVien] = { total: 0 };
      }
      baidangMap[nhanVien][kenhBai] = (baidangMap[nhanVien][kenhBai] || 0) + 1;
      baidangMap[nhanVien].total++;

      if (link) {
        allLinkList.push({ 
          nhanVien, 
          kenhBai, 
          link,
          ngayTao 
        });
      }
    });

    // Lọc theo nhân viên nếu có
    let filteredLinkList = allLinkList;
    if (baidangNhanVien !== 'all') {
      filteredLinkList = allLinkList.filter(item => item.nhanVien === baidangNhanVien);
    }

    // Phân trang
    const totalBaidangItems = filteredLinkList.length;
    const totalBaidangPages = Math.ceil(totalBaidangItems / baidangPerPage);
    const startIndex = (baidangPage - 1) * baidangPerPage;
    const endIndex = startIndex + baidangPerPage;
    
    const paginatedLinkList = filteredLinkList.slice(startIndex, endIndex);

    // Chuẩn bị dữ liệu tổng hợp
    const baidangData = Object.entries(baidangMap).map(([nv, data]) => {
      const result = { nhanVien: nv };
      const kenhBaiArray = Array.from(kenhBaiList);
      
      kenhBaiArray.forEach(kenh => {
        result[kenh] = data[kenh] || 0;
      });
      result.total = data.total;
      
      return result;
    });

    // Lọc baidangData nếu chọn nhân viên cụ thể
    let filteredBaidangData = baidangData;
    if (baidangNhanVien !== 'all') {
      filteredBaidangData = baidangData.filter(item => item.nhanVien === baidangNhanVien);
    }

    const kenhBaiArray = Array.from(kenhBaiList);

    // ------------------ Data_khach_hang (Báo cáo khách hàng mới) ------------------
    const dataKHRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Data_khach_hang",
      valueRenderOption: "FORMATTED_VALUE"
    });

    const dataKHValues = dataKHRes.data.values || [];
    const khRows = dataKHValues.slice(1);

    const khMapByNguoiTao = {}; // đếm số khách theo người tạo
    const nguonKHMap = {};      // đếm theo nguồn khách
    const loaiKHMap = {};       // đếm theo loại khách

    khRows.forEach(row => {
      const loaiKH = row[3] || "Không xác định";  // Cột D
      const nguonKH = row[28] || "Không rõ";      // Cột AC
      const ngayTao = row[32] || "";              // Cột AG
      const nguoiTao = row[33] || "Không xác định"; // Cột AH

      const ngayObj = parseSheetDate(ngayTao);
      if (startMonth && endMonth && ngayObj) {
        const th = ngayObj.getMonth() + 1;
        if (th < startMonth || th > endMonth) return;
      }

      // Đếm theo người tạo
      khMapByNguoiTao[nguoiTao] = (khMapByNguoiTao[nguoiTao] || 0) + 1;

      // Đếm theo nguồn khách
      nguonKHMap[nguonKH] = (nguonKHMap[nguonKH] || 0) + 1;

      // Đếm theo loại khách
      loaiKHMap[loaiKH] = (loaiKHMap[loaiKH] || 0) + 1;
    });

    // Chuyển thành mảng để vẽ chart
    const khNguoiTaoData = Object.entries(khMapByNguoiTao).map(([nguoi, count]) => ({ nguoi, count }));
    const khNguonData = Object.entries(nguonKHMap).map(([nguon, count]) => ({ nguon, count }));
    const khLoaiData = Object.entries(loaiKHMap).map(([loai, count]) => ({ loai, count }));

    // Kiểm tra nếu có yêu cầu xuất Excel
    if (req.query.export === 'baidang') {
      return await exportBaiDangToExcel(res, baidangMap, allLinkList, kenhBaiList, baidangNhanVien);
    }

    // render view: sales (NV), topProducts, watermarkBase64, months
    res.render("dashboard", {
      sales,
      startMonth,
      endMonth,
      soDonChot,
      soDonHuy,
      topProducts,
      cskhData,
      hinhThucList,
      baidangData: filteredBaidangData,
      kenhBaiArray,
      linkList: paginatedLinkList,
      khNguoiTaoData,
      khNguonData,
      khLoaiData,
      watermarkBase64,
      // Thêm dữ liệu cho phân trang và lọc
      baidangNhanVien,
      baidangPage,
      baidangPerPage,
      totalBaidangPages,
      totalBaidangItems,
      baidangNhanVienList: Array.from(baidangNhanVienList).sort()
    });

  } catch (err) {
    console.error("❌ Lỗi khi xử lý Dashboard:", err);
    res.status(500).send("Lỗi khi tạo Dashboard");
  }
});

// Hàm xuất Excel cho bài đăng
async function exportBaiDangToExcel(res, baidangMap, allLinkList, kenhBaiList, filterNhanVien) {
  try {
    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.Workbook();
    
    // Sheet 1: Tổng hợp theo nhân viên
    const summarySheet = workbook.addWorksheet('Tổng hợp');
    
    // Tạo header
    const headers = ['Nhân viên', 'Tổng số bài'];
    const kenhBaiArray = Array.from(kenhBaiList);
    kenhBaiArray.forEach(kenh => {
      headers.push(kenh);
    });
    
    summarySheet.addRow(headers);
    
    // Lọc dữ liệu nếu có
    let dataToExport = baidangMap;
    if (filterNhanVien !== 'all') {
      dataToExport = { [filterNhanVien]: baidangMap[filterNhanVien] || {} };
    }
    
    // Thêm dữ liệu
    Object.entries(dataToExport).forEach(([nv, data]) => {
      const row = [nv, data.total || 0];
      kenhBaiArray.forEach(kenh => {
        row.push(data[kenh] || 0);
      });
      summarySheet.addRow(row);
    });
    
    // Định dạng header
    const headerRow = summarySheet.getRow(1);
    headerRow.font = { bold: true, color: { argb: 'FFFFFF' } };
    headerRow.fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: '4F81BD' }
    };
    headerRow.alignment = { horizontal: 'center' };
    
    // Đặt độ rộng cột
    summarySheet.columns = [
      { width: 25 }, // Nhân viên
      { width: 15 }, // Tổng số bài
      ...kenhBaiArray.map(() => ({ width: 20 })) // Các kênh
    ];
    
    // Sheet 2: Danh sách chi tiết
    const detailSheet = workbook.addWorksheet('Chi tiết');
    detailSheet.addRow(['STT', 'Nhân viên', 'Ngày tạo', 'Kênh - Bài', 'Link']);
    
    // Lọc danh sách link nếu cần
    let linkListToExport = allLinkList;
    if (filterNhanVien !== 'all') {
      linkListToExport = allLinkList.filter(item => item.nhanVien === filterNhanVien);
    }
    
    // Thêm dữ liệu chi tiết
    linkListToExport.forEach((item, index) => {
      detailSheet.addRow([
        index + 1,
        item.nhanVien,
        item.ngayTao,
        item.kenhBai,
        item.link
      ]);
    });
    
    // Định dạng header sheet chi tiết
    const detailHeaderRow = detailSheet.getRow(1);
    detailHeaderRow.font = { bold: true, color: { argb: 'FFFFFF' } };
    detailHeaderRow.fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: '8064A2' }
    };
    detailHeaderRow.alignment = { horizontal: 'center' };
    
    // Đặt độ rộng cột cho sheet chi tiết
    detailSheet.columns = [
      { width: 8 },  // STT
      { width: 25 }, // Nhân viên
      { width: 20 }, // Ngày tạo
      { width: 25 }, // Kênh - Bài
      { width: 50 }  // Link
    ];
    
    // Tạo link có hyperlink
    detailSheet.eachRow((row, rowNumber) => {
      if (rowNumber > 1) { // Bỏ qua header
        const linkCell = row.getCell(5); // Cột link
        const linkValue = linkCell.value;
        if (linkValue && (linkValue.startsWith('http://') || linkValue.startsWith('https://'))) {
          linkCell.value = {
            text: 'Xem bài',
            hyperlink: linkValue,
            tooltip: linkValue
          };
          linkCell.font = { color: { argb: '0000FF' }, underline: true };
        }
      }
    });
    
    // Đặt tên file
    const fileName = filterNhanVien !== 'all' 
      ? `Bao-cao-bai-dang-${filterNhanVien}.xlsx`
      : 'Bao-cao-bai-dang-tat-ca.xlsx';
    
    // Gửi file về client
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${fileName}"`
    );
    
    await workbook.xlsx.write(res);
    res.end();
    
  } catch (error) {
    console.error('❌ Lỗi xuất Excel:', error);
    res.status(500).send('Lỗi khi xuất file Excel');
  }
}


// xuatkhovt.js (đã cập nhật cho /xuatkhovt-mã đơn hàng)
app.get('/xuatkhovt-:maDonHang', async (req, res) => {
try {
const maDonHang = req.params.maDonHang;
console.log('▶️ Bắt đầu xuất kho VT cho mã đơn hàng:', maDonHang);
if (!maDonHang) {
return res.status(400).send('Thiếu mã đơn hàng trong URL');
}
// Chuẩn bị dữ liệu (sử dụng maDonHang được cung cấp)
const result = await preparexkvtData(auth, SPREADSHEET_ID, SPREADSHEET_BOM_ID, SPREADSHEET_KHVT_ID, maDonHang);
console.log('✔️ Hoàn tất xử lý xuất kho VT cho:', maDonHang);
// Trả về phản hồi cho client
res.json({
status: 'success',
message: 'Xử lý hoàn tất',
result
});
} catch (err) {
console.error('❌ Lỗi khi xuất kho VT:', err.stack || err.message);
res.status(500).send('Lỗi server: ' + (err.message || err));
}
});

// === Sao chép đơn hàng chi tiết ===
app.get("/copy-:madh", async (req, res) => {
    const { madh } = req.params;

    try {
        console.log(`🔍 Đang tìm đơn hàng có mã: ${madh}`);
        const sheetNamePVC = "Don_hang_PVC_ct";
        const sheetNameDH = "Don_hang";

        // === 1️⃣ Lấy toàn bộ dữ liệu từ sheet Don_hang_PVC_ct ===
        const getPVC = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${sheetNamePVC}!A:AG`,
        });
        const rowsPVC = getPVC.data.values || [];
        if (rowsPVC.length === 0) {
            return res.send("❌ Sheet Don_hang_PVC_ct không có dữ liệu!");
        }

        // === 2️⃣ Lọc các dòng có cột B = madh ===
        const madhIndex = 1; // cột B
        const matchedRows = rowsPVC.filter((r, i) => i > 0 && r[madhIndex] === madh);

        if (matchedRows.length === 0) {
            return res.send(`❌ Không tìm thấy đơn hàng nào có mã ${madh}`);
        }

        console.log(`✅ Tìm thấy ${matchedRows.length} dòng cần sao chép.`);

        // === 3️⃣ Tạo mã đơn hàng mới ===
        const yearNow = new Date().getFullYear().toString().slice(-2); // "25"
        const matchParts = madh.split("-");
        if (matchParts.length !== 3) {
            return res.send("❌ Mã đơn hàng không hợp lệ (phải dạng MC25-0-1453)");
        }

        const codePrefix = matchParts[0].substring(0, 2); // "MC"
        const kinhdoanhCode = matchParts[1]; // "0"

        // Lấy dữ liệu Don_hang để tìm MAX trong E theo F = kinhdoanhCode và năm
        const getDH = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${sheetNameDH}!A:F`,
        });
        const rowsDH = getDH.data.values || [];
        const colBIndex = 1; // ngày tạo
        const colEIndex = 4; // số đơn hàng
        const colFIndex = 5; // mã kinh doanh

        // Lọc theo năm hiện tại và mã kinh doanh
        const rowsFiltered = rowsDH.filter((r, i) => {
            if (i === 0) return false;
            const fVal = r[colFIndex];
            const dateVal = r[colBIndex];
            if (!fVal || !dateVal) return false;
            // Kiểm tra có chứa năm hiện tại (vd: "2025" hoặc "25")
            return fVal == kinhdoanhCode && (dateVal.includes(yearNow) || dateVal.includes("20" + yearNow));
        });

        const numbers = rowsFiltered
            .map((r) => parseInt(r[colEIndex]))
            .filter((n) => !isNaN(n));

        const maxNum = numbers.length > 0 ? Math.max(...numbers) : 0;
        const newNum = maxNum + 1;

        const newNumStr = String(newNum).padStart(4, "0");

        const madhNew = `${codePrefix}${yearNow}-${kinhdoanhCode}-${newNumStr}`;
        console.log(`🔢 Mã đơn hàng mới: ${madhNew}`);

        // === 4️⃣ Tạo dữ liệu mới ===
        const today = new Date();
        const dd = String(today.getDate()).padStart(2, "0");
        const mm = String(today.getMonth() + 1).padStart(2, "0");
        const yyyy = today.getFullYear();
        const hh = String(today.getHours()).padStart(2, "0");
        const mi = String(today.getMinutes()).padStart(2, "0");
        const ss = String(today.getSeconds()).padStart(2, "0");

        const ddmmyyyy = `${dd}/${mm}/${yyyy}`;
        const nowFull = `${dd}/${mm}/${yyyy} ${hh}:${mi}:${ss}`;

        // Hàm sinh UNIQUE ID ngẫu nhiên 8 ký tự
        function randomUID() {
            const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
            return Array.from({ length: 8 }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
        }

        // Tạo mảng dữ liệu mới
        const newRows = matchedRows.map((r) => {
            const row = [...r];
            row[0] = randomUID(); // A = UNIQUE ID
            row[1] = madhNew; // B = mã đơn hàng mới
            if (row[2]) row[2] = madhNew + row[2].substring(11); // C: thay 11 ký tự đầu
            row[29] = ddmmyyyy; // AD
            row[32] = nowFull; // AG
            return row;
        });

        // === 5️⃣ Ghi vào cuối sheet ===
        await sheets.spreadsheets.values.append({
            spreadsheetId: SPREADSHEET_ID,
            range: `${sheetNamePVC}!A:AG`,
            valueInputOption: "USER_ENTERED",
            insertDataOption: "INSERT_ROWS",
            requestBody: { values: newRows },
        });

        console.log(`✅ Đã sao chép xong đơn hàng ${madh} → ${madhNew}`);

        // === 6️⃣ Trả về HTML tự đóng sau 2 giây ===
        res.send(`
          <html lang="vi">
            <head>
              <meta charset="UTF-8" />
              <title>Đã sao chép xong đơn hàng</title>
              <style>
                body {
                  font-family: sans-serif;
                  text-align: center;
                  margin-top: 100px;
                }
                h2 { color: #2ecc71; }
              </style>
              <script>
                setTimeout(() => {
                  try { window.close(); } catch(e) {}
                }, 2000);
              </script>
            </head>
            <body>
              <h2>✅ Đã sao chép xong đơn hàng!</h2>
              <p>Mã mới: <b>${madhNew}</b></p>
              <p>Tab này sẽ tự đóng sau 2 giây...</p>
            </body>
          </html>
        `);

    } catch (error) {
        console.error("❌ Lỗi khi sao chép đơn hàng:", error);
        res.status(500).send(`
          <html lang="vi">
            <head><meta charset="UTF-8" /><title>Lỗi sao chép</title></head>
            <body style="font-family:sans-serif;text-align:center;margin-top:100px;color:red;">
              <h2>❌ Lỗi khi sao chép đơn hàng</h2>
              <p>${error.message}</p>
              <p>Vui lòng giữ tab này để kiểm tra lỗi.</p>
            </body>
          </html>
        `);
    }
});


//===TẠO NHÁP HÓA ĐƠN====

app.get("/taohoadon-:madh", async (req, res) => {
  try {
    const { madh } = req.params;
    console.log("➡️ Nhận yêu cầu tạo hóa đơn cho mã:", madh);

    if (!madh) return res.status(400).send("Thiếu mã đơn hàng (madh)");

    // === 1️⃣ Lấy dữ liệu đơn hàng ===
    console.log("📄 Đang lấy sheet Don_hang...");
    const donhangRes = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.SPREADSHEET_ID,
      range: "Don_hang!A1:Z",
    });

    const donhangData = donhangRes.data.values;
    if (!donhangData || donhangData.length < 2) {
      console.error("❌ Sheet Don_hang trống hoặc không có dữ liệu.");
      return res.status(404).send("Không có dữ liệu đơn hàng");
    }

    // === Hàm chuyển cột sang index ===
    const colToIndex = (col) =>
      col
        .toUpperCase()
        .split("")
        .reduce((acc, c) => acc * 26 + (c.charCodeAt(0) - 65 + 1), 0) - 1;

    const madhIndex = colToIndex("G"); // Mã đơn hàng
    const companyNameIndex = colToIndex("J"); // Tên công ty
    const taxCodeIndex = colToIndex("K"); // Mã số thuế
    const addressIndex = colToIndex("L"); // Địa chỉ

    console.log("📊 Tìm đơn hàng có mã:", madh);
    const orderRow = donhangData.find(
      (r) => (r[madhIndex] || "").trim() === madh.trim()
    );

    if (!orderRow) {
      console.error("❌ Không tìm thấy đơn hàng:", madh);
      return res.status(404).send("Không tìm thấy đơn hàng");
    }
    console.log("✅ Đã tìm thấy đơn hàng:", orderRow);

    // === 2️⃣ Lấy chi tiết đơn hàng ===
    console.log("📄 Đang lấy sheet Don_hang_PVC_ct...");
    const detailRes = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.SPREADSHEET_ID,
      range: "Don_hang_PVC_ct!A1:AB",
    });

    const detailData = detailRes.data.values;
    if (!detailData || detailData.length < 2) {
      console.error("❌ Sheet Don_hang_PVC_ct trống hoặc không có dữ liệu.");
      return res.status(404).send("Không có dữ liệu chi tiết đơn hàng");
    }

    const madhDetailIndex = colToIndex("B"); // Mã đơn hàng
    const descriptionIndex = colToIndex("J"); // Diễn giải
    const quantityIndex = colToIndex("V"); // Số lượng
    const unitIndex = colToIndex("W"); // ĐVT
    const unitPriceIndex = colToIndex("Z"); // Đơn giá (có thể là giá sau thuế)
    const taxRateIndex = colToIndex("AA"); // Thuế suất %
    const totalAfterTaxIndex = colToIndex("AB"); // Thành tiền sau thuế

    const orderDetails = detailData.filter(
      (r) => (r[madhDetailIndex] || "").trim() === madh.trim()
    );

    if (orderDetails.length === 0) {
      console.error("⚠️ Không có chi tiết cho đơn hàng:", madh);
      return res.status(404).send("Không có chi tiết cho đơn hàng này");
    }

    console.log(`✅ Có ${orderDetails.length} dòng chi tiết đơn hàng.`);

    // === 3️⃣ Xử lý dữ liệu sản phẩm ===
    const products = orderDetails.map((row, i) => {
      const quantity = parseFloat(row[quantityIndex]) || 0;          // Số lượng
      const amountchuathue = parseFloat(row[unitPriceIndex]) || 0; 
      const taxRate = parseFloat(row[taxRateIndex]) || 0;            // Thuế suất
      const totalAfterTax = parseFloat(row[totalAfterTaxIndex]) || 0;// Thành tiền sau thuế

      // 👉 Tính toán lại theo chuẩn kế toán
      const amount = amountchuathue/ (1 + taxRate / 100);            // đơn giá chưa thuế
      const unitPrice = amount * quantity;        // thành tiền chưa thuế
      const taxAmount = unitPrice * (taxRate / 100);                    // Tiền thuế GTGT

      return {
        stt: i + 1,
        description: row[descriptionIndex] || "",
        unit: row[unitIndex] || "",
        quantity,
        unitPrice,             // Thành tiền chưa thuế
        amount,             // đơn giá
        taxRate,
        taxAmount,
        totalAmount: totalAfterTax, // Tổng sau thuế
      };
    });

    // === 4️⃣ Tính tổng ===
    const summary = {
      totalAmount0: 0,
      totalAmount8: 0,
      totalTax8: 0,
      totalAmount10: 0,
      totalTax10: 0,
    };

    products.forEach((p) => {
      if (p.taxRate === 8) {
        summary.totalAmount8 += p.unitPrice;
        summary.totalTax8 += p.taxAmount;
      } else if (p.taxRate === 10) {
        summary.totalAmount10 += p.unitPrice;
        summary.totalTax10 += p.taxAmount;
      } else {
        summary.totalAmount0 += p.unitPrice;
      }
    });

    const totalAmountBeforeTax =
      summary.totalAmount0 + summary.totalAmount8 + summary.totalAmount10;
    const totalTax = summary.totalTax8 + summary.totalTax10;
    const totalAmount = totalAmountBeforeTax + totalTax;

    // === 5️⃣ Load Logo & Watermark ===
    let logoBase64 = "";
    let watermarkBase64 = "";
    try {
      logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
      watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILEHOADON_ID);
    } catch (err) {
      console.warn("⚠️ Không thể tải logo hoặc watermark:", err.message);
    }

    // === 6️⃣ Render EJS ===
    console.log("🧾 Đang render hóa đơn EJS...");
    res.render("hoadon", {
      products,
      summary,
      totalAmountBeforeTax,
      totalTax,
      totalAmount,
      order: {
        madh,
        companyName: orderRow[companyNameIndex] || "",
        address: orderRow[addressIndex] || "",
        taxCode: orderRow[taxCodeIndex] || "",
      },
      today: new Date(),
      formatNumber1,
      numberToWords1,
      logoBase64,
      watermarkBase64,
    });

  } catch (err) {
    console.error("❌ Lỗi khi tạo hóa đơn:", err);
    res.status(500).send("Internal Server Error");
  }
});

export default app;

//// Tạo phiếu bảo hành

app.get("/phieubaohanh-:madh", async (req, res) => {
  try {
    const { madh } = req.params;
    console.log("➡️ Nhận yêu cầu tạo phiếu bảo hành cho mã:", madh);

    if (!madh) return res.status(400).send("Thiếu mã đơn hàng (madh)");

    // === 1️⃣ Lấy dữ liệu đơn hàng ===
    console.log("📄 Đang lấy sheet Don_hang...");
    const donhangRes = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.SPREADSHEET_ID,
      range: "Don_hang!A1:AD",
    });

    const donhangData = donhangRes.data.values;
    if (!donhangData || donhangData.length < 2) {
      console.error("❌ Sheet Don_hang trống hoặc không có dữ liệu.");
      return res.status(404).send("Không có dữ liệu đơn hàng");
    }

    // === Hàm chuyển cột sang index ===
    const colToIndex = (col) =>
      col
        .toUpperCase()
        .split("")
        .reduce((acc, c) => acc * 26 + (c.charCodeAt(0) - 65 + 1), 0) - 1;

    const madhIndex = colToIndex("G");
    const companyNameIndex = colToIndex("J");
    const addressIndex = colToIndex("L");
    const phoneIndex = colToIndex("H");
    const diadiem1Index = colToIndex("U");
    const diadiem2Index = colToIndex("AA");
    const diadiem3Index = colToIndex("AC");
    const loaiDiaChiIndex = colToIndex("X");

    console.log("📊 Tìm đơn hàng có mã:", madh);
    const orderRow = donhangData.find(
      (r) => (r[madhIndex] || "").trim() === madh.trim()
    );

    if (!orderRow) {
      console.error("❌ Không tìm thấy đơn hàng:", madh);
      return res.status(404).send("Không tìm thấy đơn hàng");
    }

    // === Xác định địa chỉ lắp đặt ===
    let diaChiLapDat = "";
    const loaiDiaChi = orderRow[loaiDiaChiIndex] || "";
    
    if (loaiDiaChi === "1") {
      diaChiLapDat = orderRow[diadiem1Index] || "";
    } else if (loaiDiaChi === "2") {
      diaChiLapDat = orderRow[diadiem2Index] || "";
    } else if (loaiDiaChi === "3") {
      diaChiLapDat = orderRow[diadiem3Index] || "";
    }

    // === 2️⃣ Lấy chi tiết sản phẩm ===
    console.log("📄 Đang lấy sheet Don_hang_PVC_ct...");
    const detailRes = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.SPREADSHEET_ID,
      range: "Don_hang_PVC_ct!A1:AB",
    });

    const detailData = detailRes.data.values;
    if (!detailData || detailData.length < 2) {
      console.error("❌ Sheet Don_hang_PVC_ct trống hoặc không có dữ liệu.");
      return res.status(404).send("Không có dữ liệu chi tiết đơn hàng");
    }

    const madhDetailIndex = colToIndex("B");
    const descriptionIndex = colToIndex("J");
    const quantityIndex = colToIndex("V");
    const unitIndex = colToIndex("W");

    // Lấy tất cả chi tiết đơn hàng
    const allOrderDetails = detailData.filter(
      (r) => (r[madhDetailIndex] || "").trim() === madh.trim()
    );

    if (allOrderDetails.length === 0) {
      console.error("⚠️ Không có chi tiết cho đơn hàng:", madh);
      return res.status(404).send("Không có chi tiết cho đơn hàng này");
    }

    // === 3️⃣ Xử lý dữ liệu sản phẩm - LỌC BỎ NHÂN CÔNG VÀ VẬN CHUYỂN ===
    console.log("🔍 Đang lọc sản phẩm (bỏ qua nhân công và vận chuyển)...");
    
    // Lọc bỏ các mục không phải sản phẩm
    const filteredDetails = allOrderDetails.filter(row => {
      const description = (row[descriptionIndex] || "").toLowerCase().trim();
      const excludedKeywords = ["nhân công lắp đặt", "vận chuyển", "nhân công"];
      
      // Kiểm tra xem mô tả có chứa từ khóa loại trừ không
      return !excludedKeywords.some(keyword => description.includes(keyword));
    });

    console.log(`✅ Tổng số dòng chi tiết: ${allOrderDetails.length}`);
    console.log(`✅ Sau khi lọc: ${filteredDetails.length} sản phẩm hợp lệ`);

    // === 4️⃣ Xử lý dữ liệu sản phẩm ===
    const products = filteredDetails.map((row, i) => {
      return {
        stt: i + 1,
        description: row[descriptionIndex] || "",
        unit: row[unitIndex] || "",
        quantity: parseFloat(row[quantityIndex]) || 0,
      };
    });

    // Log danh sách sản phẩm đã lọc
    if (products.length > 0) {
      console.log("📋 Danh sách sản phẩm sẽ hiển thị:");
      products.forEach(p => {
        console.log(`   - ${p.description} (${p.quantity} ${p.unit})`);
      });
    } else {
      console.warn("⚠️ Cảnh báo: Không có sản phẩm nào để hiển thị sau khi lọc!");
      // Bạn có thể thêm logic xử lý ở đây nếu muốn
    }

    // === 5️⃣ Load Logo & Watermark ===
    let logoBase64 = "";
    let watermarkBase64 = "";
    try {
      logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
      watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILEBAOHANH_ID);
    } catch (err) {
      console.warn("⚠️ Không thể tải logo hoặc watermark:", err.message);
    }

    // === 6️⃣ Render EJS ===
    console.log("🧾 Đang render phiếu bảo hành EJS...");
    res.render("phieubaohanh", {
      products,
      order: {
        madh,
        companyName: orderRow[companyNameIndex] || "",
        address: orderRow[addressIndex] || "",
        phone: orderRow[phoneIndex] || "",
        diaChiLapDat: diaChiLapDat,
      },
      logoBase64,
      watermarkBase64,
    });

  } catch (err) {
    console.error("❌ Lỗi khi tạo phiếu bảo hành:", err);
    res.status(500).send("Internal Server Error");
  }
});

//// === TẠO BẢNG CHẤM CÔNG
app.get("/bangchamcong", async (req, res) => {
  try {
    console.log("=== 🔹 [BẮT ĐẦU] Lấy báo cáo bảng chấm công ===");
    const month = parseInt(req.query.month) || new Date().getMonth() + 1;
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const phong = req.query.phong?.trim() || "Tất cả";
    console.log(`🗓️ Tháng: ${month}, Năm: ${year}, Phòng: ${phong}`);

    // --- Lấy dữ liệu từ Google Sheets ---
    const [chamCongRes, nhanVienRes] = await Promise.all([
      sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_HC_ID,
        range: "Cham_cong!A:T",
      }),
      sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_HC_ID,
        range: "Nhan_vien!A:AH",
      }),
    ]);

    const chamCongRows = chamCongRes.data.values || [];
    const nhanVienRows = nhanVienRes.data.values || [];

    // === Danh sách phòng ===
    let danhSachPhong = [...new Set(nhanVienRows.slice(1).map(r => r[6] || ""))];
    danhSachPhong = danhSachPhong.filter(p => p.trim() !== "").sort();
    danhSachPhong.unshift("Tất cả");

    // --- Lọc nhân viên đang hoạt động ---
    let activeStaff = nhanVienRows
      .filter(r => r && r[33] === "Đang hoạt động")
      .map(r => ({
        maNV: (r[0] || "").trim(),
        hoTen: (r[1] || "").trim(),
        phong: (r[6] || "").trim(),
        nhom: (r[8] || "").trim(),
        chucVu: (r[9] || "").trim(),
      }))
      .filter(nv => nv.maNV);

    if (phong !== "Tất cả") {
      activeStaff = activeStaff.filter(nv => nv.phong === phong);
    }
    console.log("Số nhân viên sau lọc:", activeStaff.length);

    // === Tạo mảng ngày trong tháng ===
    const numDays = new Date(year, month, 0).getDate();
    const days = [];
    for (let i = 1; i <= numDays; i++) {
      const date = new Date(year, month - 1, i);
      days.push({ day: i, weekday: date.getDay(), date });
    }

    // === Chức vụ đặc biệt (tự động 26 công) ===
    const specialRoles = [
      "Chủ tịch hội đồng quản trị",
      "Tổng giám đốc",
      "Trưởng phòng kế hoạch tài chính",
      "Trưởng phòng HCNS",
      "Quản đốc",
      "NV kế hoạch dịch vụ",
      "Trưởng phòng kinh doanh",
    ];

    // === Ngày lễ (hiển thị L) ===
    const ngayLeVN = ["01-01", "04-30", "05-01", "09-02"];

    // === HÀM PARSE CÔNG NGÀY - SỬA LỖI DẤU PHẨY ===
    function parseCongNgay(value) {
      if (!value) return 0;
      // Thay thế dấu phẩy bằng dấu chấm để parse số thập phân
      const cleanValue = value.toString().trim().replace(',', '.');
      const parsed = parseFloat(cleanValue);
      return isNaN(parsed) ? 0 : parsed;
    }

    // === Gom dữ liệu chấm công ===
    const chamCongMap = new Map();

    chamCongRows.slice(1).forEach(r => {
      const ngayStr = r[1];
      const trangThai = r[2];
      const maNV = r[12];

      if (!ngayStr || !maNV) return;

      const [d, m, y] = ngayStr.split("/").map(Number);
      if (m !== month || y !== year) return;

      // SỬA: Dùng hàm parseCongNgay mới
      const congNgay = parseCongNgay(r[16]);
      const tangCa = parseCongNgay(r[19]);

      const key = `${maNV}_${d}`;

      if (chamCongMap.has(key)) {
        const existing = chamCongMap.get(key);
        existing.congNgay += congNgay;
        existing.tangCa += tangCa;
        if (["Nghỉ việc riêng", "Nghỉ phép"].includes(trangThai)) {
          existing.trangThai = trangThai;
        }
      } else {
        chamCongMap.set(key, { trangThai, congNgay, tangCa });
      }
    });

    // === Xử lý từng nhân viên - LOGIC ĐÃ SỬA ===
    const records = activeStaff.map(nv => {
      const ngayCong = Array(numDays).fill(null).map(() => ["", ""]);
      let tongTangCa = 0;
      let tongGioLe = 0;

      // Chức vụ đặc biệt → cố định 26 công
      if (specialRoles.includes(nv.chucVu?.trim())) {
        for (let i = 0; i < numDays; i++) {
          ngayCong[i] = ["V", "V"];
        }
        return {
          ...nv,
          ngayCong,
          soNgayCong: "26.0",
          tongTangCa: "0.0",
        };
      }

      days.forEach((d, idx) => {
        const key = `${nv.maNV}_${d.day}`;
        const item = chamCongMap.get(key);

        if (item) {
          const { trangThai, congNgay, tangCa } = item;
          tongTangCa += tangCa;

          // Xử lý trạng thái nghỉ trước
          if (trangThai === "Nghỉ việc riêng") {
            ngayCong[idx] = ["X", "X"];
          } else if (trangThai === "Nghỉ phép") {
            ngayCong[idx] = ["P", "P"];
          } 
          // Xử lý công ngày - LOGIC ĐÃ SỬA
          else {
            console.log(`DEBUG XỬ LÝ: ${nv.maNV} ngày ${d.day} - congNgay=${congNgay}`);
            
            if (congNgay >= 1) {
              ngayCong[idx] = ["V", "V"];
            } else if (congNgay === 0.5) {
              ngayCong[idx] = ["V", "X"];
            } else if (congNgay > 0.5 && congNgay < 1) {
              // 0.93 công -> V sáng + giờ chiều
              const gioChieu = ((congNgay - 0.5) * 8).toFixed(1);
              ngayCong[idx] = ["V", `${gioChieu}`];
              tongGioLe += (congNgay - 0.5) * 8;
              console.log(`  -> V ${gioChieu} (${congNgay} công = V sáng + ${gioChieu} chiều)`);
            } else if (congNgay > 0 && congNgay < 0.5) {
              // Dưới 0.5 công -> chỉ làm buổi sáng
              const gioSang = (congNgay * 8).toFixed(1);
              ngayCong[idx] = [`${gioSang}`, ""];
              tongGioLe += congNgay * 8;
              console.log(`  -> ${gioSang} "" (${congNgay} công = ${gioSang} sáng)`);
            } else if (congNgay === 0) {
              // Công = 0 -> X hoặc L
              const dayStr = `${String(month).padStart(2, "0")}-${String(d.day).padStart(2, "0")}`;
              const isLe = ngayLeVN.some(le => dayStr.includes(le));
              ngayCong[idx] = isLe ? ["L", "L"] : ["X", "X"];
            } else {
              ngayCong[idx] = ["X", "X"];
            }
          }
        } else {
          // Không chấm công
          const dayStr = `${String(month).padStart(2, "0")}-${String(d.day).padStart(2, "0")}`;
          const isLe = ngayLeVN.some(le => dayStr.includes(le));
          ngayCong[idx] = isLe ? ["L", "L"] : ["X", "X"];
        }
      });

      // === Tính tổng ngày công ===
      let soBuoiV = 0;
      ngayCong.forEach(ca => {
        if (ca[0] === "V") soBuoiV++;
        if (ca[1] === "V") soBuoiV++;
      });

      const congTuBuoi = soBuoiV / 2;
      const congTuGioLe = tongGioLe / 8;
      const tongNgayCong = congTuBuoi + congTuGioLe;

      console.log(`TỔNG KẾT ${nv.maNV}: ${soBuoiV} buổi V = ${congTuBuoi} công, ${tongGioLe.toFixed(1)} giờ lẻ = ${congTuGioLe.toFixed(1)} công -> Tổng: ${tongNgayCong.toFixed(1)} công`);

      return {
        ...nv,
        ngayCong,
        soNgayCong: tongNgayCong.toFixed(1),
        tongTangCa: tongTangCa.toFixed(1),
      };
    });

    // Render view
    res.render("bangchamcong", {
      month,
      year,
      phong,
      danhSachPhong,
      days,
      records,
    });

  } catch (err) {
    console.error("❌ Lỗi khi lấy dữ liệu bảng chấm công:", err);
    res.status(500).send("Lỗi khi xử lý dữ liệu bảng chấm công!");
  }
});

import ExcelJS from "exceljs";

app.get("/bangchamcong/export-excel", async (req, res) => {
  try {
    const month = parseInt(req.query.month);
    const year = parseInt(req.query.year);
    const phong = req.query.phong?.trim() || "Tất cả";

    const [chamCongRes, nhanVienRes] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_HC_ID, range: "Cham_cong!A:T" }),
      sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_HC_ID, range: "Nhan_vien!A:AH" }),
    ]);

    const chamCongRows = chamCongRes.data.values || [];
    const nhanVienRows = nhanVienRes.data.values || [];

    // --- HÀM PARSE CÔNG NGÀY - GIỐNG NHƯ WEB ---
    function parseCongNgay(value) {
      if (!value) return 0;
      const cleanValue = value.toString().trim().replace(',', '.');
      const parsed = parseFloat(cleanValue);
      return isNaN(parsed) ? 0 : parsed;
    }

    // --- Lấy danh sách phòng ---
    let danhSachPhong = [...new Set(nhanVienRows.slice(1).map(r => r[6] || ""))].filter(p => p.trim() !== "");
    danhSachPhong.sort();
    danhSachPhong.unshift("Tất cả");

    // --- Lọc nhân viên ---
    let activeStaff = nhanVienRows
      .filter(r => r[33] === "Đang hoạt động")
      .map(r => ({
        maNV: r[0],
        hoTen: r[1],
        phong: r[6],
        nhom: r[8],
        chucVu: r[9],
      }));

    if (phong !== "Tất cả") activeStaff = activeStaff.filter(nv => nv.phong === phong);

    // --- Map chấm công - SỬA DÙNG HÀM PARSE MỚI ---
    const chamCongMap = new Map();
    chamCongRows.slice(1).forEach(r => {
      const ngayStr = r[1];
      const trangThai = r[2];
      const maNV = r[12];
      
      // SỬA: Dùng hàm parseCongNgay
      const congNgay = parseCongNgay(r[16]);
      const tangCa = parseCongNgay(r[19]);
      
      if (!ngayStr || !maNV) return;
      const [d, m, y] = ngayStr.split("/").map(Number);
      if (m === month && y === year) chamCongMap.set(`${maNV}_${d}`, { trangThai, congNgay, tangCa });
    });

    // --- Ngày trong tháng ---
    const numDays = new Date(year, month, 0).getDate();
    const days = Array.from({ length: numDays }, (_, i) => i + 1);

    const ngayLeVN = ["01-01","04-30","05-01","09-02"];
    const specialRoles = [
      "Chủ tịch hội đồng quản trị",
      "Tổng giám đốc",
      "Trưởng phòng kế hoạch tài chính",
      "Trưởng phòng HCNS",
      "Quản đốc",
      "NV kế hoạch dịch vụ",
      "Trưởng phòng kinh doanh",
    ];

    // --- Tính dữ liệu chấm công - SỬA LOGIC GIỐNG WEB ---
    const records = activeStaff.map(nv => {
      const ngayCong = Array(numDays).fill(null).map(() => ["", ""]);
      let tongTangCa = 0;
      let tongGioLe = 0; // THÊM: Để tính giờ lẻ

      // Chức vụ đặc biệt → cố định 26 công
      if (specialRoles.includes(nv.chucVu?.trim())) {
        for (let i = 0; i < numDays; i++) {
          ngayCong[i] = ["V", "V"];
        }
        return { 
          ...nv, 
          ngayCong, 
          soNgayCong: 26, 
          tongTangCa: 0 
        };
      }

      for (let d = 1; d <= numDays; d++) {
        const key = `${nv.maNV}_${d}`;
        const item = chamCongMap.get(key);
        
        if (item) {
          const { trangThai, congNgay, tangCa } = item;
          tongTangCa += tangCa;

          // Xử lý trạng thái nghỉ trước
          if (trangThai === "Nghỉ việc riêng") {
            ngayCong[d-1] = ["X", "X"];
          } else if (trangThai === "Nghỉ phép") {
            ngayCong[d-1] = ["P", "P"];
          } 
          // Xử lý công ngày - LOGIC GIỐNG WEB
          else {
            if (congNgay >= 1) {
              ngayCong[d-1] = ["V", "V"];
            } else if (congNgay === 0.5) {
              ngayCong[d-1] = ["V", "X"];
            } else if (congNgay > 0.5 && congNgay < 1) {
              // 0.93 công -> V sáng + giờ chiều
              const gioChieu = ((congNgay - 0.5) * 8).toFixed(1);
              ngayCong[d-1] = ["V", `${gioChieu}`];
              tongGioLe += (congNgay - 0.5) * 8; // CỘNG GIỜ LẺ
            } else if (congNgay > 0 && congNgay < 0.5) {
              // Dưới 0.5 công -> chỉ làm buổi sáng
              const gioSang = (congNgay * 8).toFixed(1);
              ngayCong[d-1] = [`${gioSang}`, ""];
              tongGioLe += congNgay * 8; // CỘNG GIỜ LẺ
            } else {
              // Công = 0 -> X hoặc L
              const dayStr = `${String(month).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
              if (ngayLeVN.includes(dayStr)) {
                ngayCong[d-1] = ["L", "L"];
              } else {
                ngayCong[d-1] = ["X", "X"];
              }
            }
          }
        } else {
          // Không chấm công
          const dayStr = `${String(month).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
          if (ngayLeVN.includes(dayStr)) {
            ngayCong[d-1] = ["L", "L"];
          } else {
            ngayCong[d-1] = ["X", "X"];
          }
        }
      }

      // TÍNH SỐ NGÀY CÔNG CHÍNH XÁC - GIỐNG WEB
      let soBuoiV = 0;
      ngayCong.forEach(ca => {
        if (ca[0] === "V") soBuoiV++;
        if (ca[1] === "V") soBuoiV++;
      });

      const congTuBuoi = soBuoiV / 2;
      const congTuGioLe = tongGioLe / 8;
      const soNgayCong = congTuBuoi + congTuGioLe;

      return {
        ...nv, 
        ngayCong, 
        soNgayCong, 
        tongTangCa
      };
    });

    // --- Tạo workbook & worksheet ---
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("Bang cham cong");

    const totalCols = 4 + numDays * 2 + 2;

    // --- Tiêu đề lớn ---
    ws.mergeCells(1, 1, 1, totalCols);
    const titleCell = ws.getCell(1, 1);
    titleCell.value = "BẢNG CHẤM CÔNG";
    titleCell.alignment = { horizontal: "center", vertical: "middle" };
    titleCell.font = { size: 16, bold: true };

    // --- Thông tin Tháng/Năm/Phòng ---
    ws.mergeCells(2, 1, 2, totalCols);
    const infoCell = ws.getCell(2, 1);
    infoCell.value = `Tháng: ${month} / Năm: ${year} / Phòng: ${phong}`;
    infoCell.alignment = { horizontal: "center" };
    infoCell.font = { bold: true };

    // --- Header 1 ---
    let headerRow1 = ["STT", "Mã NV", "Họ tên", "Chức vụ"];
    days.forEach(d => { headerRow1.push(`${d}`, "") });
    headerRow1.push("Số ngày công", "Tăng ca");
    const hr1 = ws.addRow(headerRow1);

    // --- Header 2 ---
    let headerRow2 = ["", "", "", ""];
    days.forEach(() => { headerRow2.push("S", "C") });
    headerRow2.push("", "");

    const hr2 = ws.addRow(headerRow2);

    // Gộp ô cho header
    ws.mergeCells(hr1.number, 1, hr2.number, 1);
    ws.mergeCells(hr1.number, 2, hr2.number, 2);
    ws.mergeCells(hr1.number, 3, hr2.number, 3);
    ws.mergeCells(hr1.number, 4, hr2.number, 4);

    let colIdx = 5;
    days.forEach(() => {
      ws.mergeCells(hr1.number, colIdx, hr1.number, colIdx + 1);
      colIdx += 2;
    });
    ws.mergeCells(hr1.number, colIdx, hr2.number, colIdx);
    ws.mergeCells(hr1.number, colIdx + 1, hr2.number, colIdx + 1);

    // --- Style cho header ---
    [hr1, hr2].forEach(r => {
      r.eachCell(cell => {
        cell.font = { bold: true };
        cell.alignment = { horizontal: "center", vertical: "middle" };
        cell.border = {
          top: { style: "thin" }, 
          left: { style: "thin" }, 
          bottom: { style: "thin" }, 
          right: { style: "thin" }
        };
        cell.fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: 'FFE6E6E6' }
        };
      });
    });

    // --- Ghi dữ liệu + màu sắc + border ---
    records.forEach((r, idx) => {
      const row = [idx + 1, r.maNV, r.hoTen, r.chucVu];
      r.ngayCong.forEach(ca => row.push(ca[0], ca[1]));
      row.push(r.soNgayCong.toFixed(1), r.tongTangCa.toFixed(1)); // SỬA: Format số
      const rw = ws.addRow(row);

      rw.eachCell({ includeEmpty: true }, (cell, colNumber) => {
        cell.border = {
          top: { style: "thin" }, 
          left: { style: "thin" }, 
          bottom: { style: "thin" }, 
          right: { style: "thin" }
        };
        
        // Màu theo giá trị - GIỐNG WEB
        if (typeof cell.value === "string") {
          if (cell.value === "V") {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFFFFF' } };
          } else if (cell.value === "X") {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF8B3' } };
          } else if (cell.value === "L") {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFC999' } };
          } else if (cell.value === "P") {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFA3D2FF' } };
          } else if (cell.value.includes('h')) {
            // Ô có giờ (ví dụ: "3.4h") - màu xanh nhạt
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE8F5E8' } };
          }
        }
        
        // Căn giữa cho các cột ngày
        if (colNumber >= 5 && colNumber <= 4 + numDays * 2) {
          cell.alignment = { horizontal: "center" };
        }
      });
    });

    // --- Căn chỉnh cột ---
    ws.columns.forEach(column => {
      let maxLength = 0;
      column.eachCell({ includeEmpty: true }, cell => {
        const length = cell.value ? cell.value.toString().length : 0;
        if (length > maxLength) {
          maxLength = length;
        }
      });
      column.width = Math.max(8, maxLength + 2);
    });

    // --- Xuất file ---
    res.setHeader("Content-Disposition", `attachment; filename="bang_cham_cong_${month}_${year}.xlsx"`);
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    await wb.xlsx.write(res);
    res.end();

  } catch (err) {
    console.error("EXPORT EXCEL ERROR:", err);
    res.status(500).send("Lỗi khi xuất Excel!");
  }
});


//Lộ trình xe

app.get("/baocaolotrinh", async (req, res) => {
  try {
    const { thang, nam } = req.query;

    console.log(`\n=== BÁO CÁO LỘ TRÌNH - THÁNG ${thang}/${nam} ===`);

    if (!thang || !nam) {
      return res.render("baocaolotrinh", {
        data: null,
        logo: await loadDriveImageBase64(LOGO_FILE_ID),
        watermark: await loadDriveImageBase64(WATERMARK_FILE_ID),
      });
    }

    const month = parseInt(thang);
    const year = parseInt(nam);
    console.log(`Tìm kiếm: Tháng ${month}, Năm ${year}`);

    // Lấy dữ liệu 3 sheet
    const [loTrinhRes, ptRes, xangRes] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_HC_ID, range: "Lo_trinh_xe!A:Z" }),
      sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_HC_ID, range: "Data_phuong_tien!A:Z" }),
      sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_HC_ID, range: "QL_ly_xang_dau!A:Z" }),
    ]);

    const loTrinhAll = loTrinhRes.data.values || [];
    const loTrinhRows = loTrinhAll.slice(1);
    const xangAll = xangRes.data.values || [];
    const xangRows = xangAll.slice(1);

    // LOG 1: 3 dòng cuối cùng sheet Lộ trình xe
    console.log("\n1. 3 dòng cuối cùng trong sheet 'Lo_trinh_xe':");
    const last3LoTrinh = loTrinhRows.slice(-3);
    last3LoTrinh.forEach((row, i) => {
      console.log(`   ${loTrinhAll.length - 3 + i}: [Ngày] ${row[1] || row[0]} | [Xe] ${row[2]} | [Mục đích] ${row[7]} | [Km] ${row[9]} | [Người SD] ${row[12]}`);
    });

    // LOG 2: 3 dòng cuối cùng sheet Xăng dầu
    console.log("\n2. 3 dòng cuối cùng trong sheet 'QL_ly_xang_dau':");
    const last3Xang = xangRows.slice(-3);
    last3Xang.forEach((row, i) => {
      console.log(`   ${xangAll.length - 3 + i}: [Ngày đổ] ${row[14]} | [Phương tiện] ${row[7]} | [Số lít] ${row[10]} | [Đơn giá] ${row[11]} | [Loại] ${row[9]}`);
    });

    // Hàm parseDate
    function parseDate(str) {
      if (!str) return null;
      const s = str.toString().trim();
      const parts = s.split(/[-\/]/);
      if (parts.length !== 3) return null;
      let d, m, y;
      if (s.includes('/')) {
        [d, m, y] = parts;
      } else {
        [y, m, d] = parts;
      }
      d = d.padStart(2, '0');
      m = m.padStart(2, '0');
      const date = new Date(y, m - 1, d);
      return isNaN(date.getTime()) ? null : date;
    }

    // Map phương tiện
    const ptRows = (ptRes.data.values || []).slice(1);
    const phuongTienInfo = {};
    ptRows.forEach(row => {
      if (row[2]) {
        const ten = row[2].trim();
        phuongTienInfo[ten] = {
          tenXe: ten,
          dinhMucNL: parseFloat(row[6]) || 0,
          dinhMucKH: parseFloat(row[7]) || 0,
          loaiNhienLieu: (row[5] || "").trim().toUpperCase() || "DO",
        };
      }
    });

    // TÍNH ĐƠN GIÁ TRUNG BÌNH RIÊNG CHO TỪNG LOẠI NHIÊN LIỆU (DO & RON)
    const giaTB_TheoLoai = { DO: { lit: 0, tien: 0 }, RON: { lit: 0, tien: 0 } };
    const start = new Date(year, month - 1, 1);
    const end = new Date(year, month, 0);

    xangRows.forEach(row => {
      const ngayDo = parseDate(row[14]);
      if (!ngayDo || ngayDo < start || ngayDo > end) return;

      const lit = parseFloat(row[10]) || 0;
      const gia = parseFloat(row[11]) || 0;
      const loaiRaw = (row[9] || "").toString().trim().toUpperCase();
      const loai = loaiRaw === "RON" ? "RON" : "DO"; // Chỉ nhận DO hoặc RON

      if (lit > 0 && gia > 0) {
        giaTB_TheoLoai[loai].lit += lit;
        giaTB_TheoLoai[loai].tien += lit * gia;
      }
    });

    // Tính đơn giá trung bình riêng
    const donGiaTB = {
      DO: giaTB_TheoLoai.DO.lit > 0 ? Math.round(giaTB_TheoLoai.DO.tien / giaTB_TheoLoai.DO.lit) : 0,
      RON: giaTB_TheoLoai.RON.lit > 0 ? Math.round(giaTB_TheoLoai.RON.tien / giaTB_TheoLoai.RON.lit) : 0,
    };

    console.log(`\nĐơn giá nhiên liệu trung bình tháng ${month}/${year}:`);
    console.log(`   → Dầu DO : ${donGiaTB.DO.toLocaleString()} đ/lít (tổng ${giaTB_TheoLoai.DO.lit.toFixed(1)} lít)`);
    console.log(`   → Xăng RON: ${donGiaTB.RON.toLocaleString()} đ/lít (tổng ${giaTB_TheoLoai.RON.lit.toFixed(1)} lít)`);

    // Lọc dữ liệu lộ trình trong tháng
    const records = loTrinhRows
      .map(row => {
        const ngayRaw = row[1] || row[0];
        if (!ngayRaw) return null;
        const ngay = parseDate(ngayRaw);
        if (!ngay || ngay.getMonth() + 1 !== month || ngay.getFullYear() !== year) return null;

        return {
          phuongTien: row[2]?.trim() || "",
          mucDich: row[7]?.trim() || "",
          soKm: parseFloat(row[9]) || 0,
          nguoiSD: row[12]?.trim() || "",
          tienEpass: parseFloat(row[14]) || 0,
        };
      })
      .filter(Boolean);

    console.log(`\nTổng số bản ghi lộ trình thỏa tháng ${month}/${year}: ${records.length} dòng`);

    // Xử lý dữ liệu xe (giữ nguyên logic cũ)
    const danhSachXe = [...new Set(records.map(r => r.phuongTien))].filter(Boolean);
    const dataXe = {};

    danhSachXe.forEach(tenXe => {
      const info = phuongTienInfo[tenXe] || { dinhMucNL: 0, dinhMucKH: 0, loaiNhienLieu: "DO" };
      dataXe[tenXe] = {
        tenXe,
        dinhMucNL: info.dinhMucNL,
        dinhMucKH: info.dinhMucKH,
        loaiNhienLieu: info.loaiNhienLieu,
        kmQuangMinh: 0,
        kmCaNhan: 0,
        nguoiSD_QuangMinh: new Set(),
        nguoiSD_CaNhan: new Set(),
        tienEpass: 0,
      };
    });

    records.forEach(r => {
      if (!dataXe[r.phuongTien]) return;
      const xe = dataXe[r.phuongTien];
      xe.tienEpass += r.tienEpass;

      if (r.phuongTien === "Xe Quang Minh") {
        xe.kmQuangMinh += r.soKm;
        if (r.nguoiSD) xe.nguoiSD_QuangMinh.add(r.nguoiSD);
      }
      if (r.mucDich === "Cá nhân") {
        xe.kmCaNhan += r.soKm;
        if (r.nguoiSD) xe.nguoiSD_CaNhan.add(r.nguoiSD);
      }
    });

    // TÍNH TIỀN NHIÊN LIỆU DỰA ĐÚNG LOẠI CỦA XE
    Object.values(dataXe).forEach(xe => {
      const kmCaNhan = xe.kmCaNhan;
      xe.tienKhauHao = Math.round(kmCaNhan * xe.dinhMucKH);

      const giaNL = donGiaTB[xe.loaiNhienLieu] || 0;
      xe.tienNhienLieu = Math.round((kmCaNhan * xe.dinhMucNL / 100) * giaNL);
      xe.thanhTien = xe.tienKhauHao + xe.tienNhienLieu;
    });

    const xeArray = Object.values(dataXe);

    // Tổng kết
    const tongKmCaNhan = xeArray.reduce((s, x) => s + x.kmCaNhan, 0);
    const tongTienKhauHao = xeArray.reduce((s, x) => s + x.tienKhauHao, 0);
    const tongTienNhienLieu = xeArray.reduce((s, x) => s + x.tienNhienLieu, 0);
    const tongThanhTien = tongTienKhauHao + tongTienNhienLieu;
    const tongEpass = xeArray.reduce((s, x) => s + x.tienEpass, 0);
    const tongCuoi = tongThanhTien + tongEpass;

    console.log(`\nTổng kết: ${tongKmCaNhan} km cá nhân → Thành tiền: ${tongThanhTien.toLocaleString()} + Epass ${tongEpass.toLocaleString()} = ${tongCuoi.toLocaleString()}đ\n`);

    res.render("baocaolotrinh", {
      data: {
        thang: month,
        nam: year,
        donGiaTB, // ← BÂY GIỜ LÀ OBJECT { DO: ..., RON: ... }
        xeArray,
        tongKmCaNhan,
        tongTienKhauHao,
        tongTienNhienLieu,
        tongThanhTien,
        tongEpass,
        tongCuoi,
        coXeQuangMinh: dataXe['Xe Quang Minh']?.kmQuangMinh > 0,
      },
      logo: await loadDriveImageBase64(LOGO_FILE_ID),
      watermark: await loadDriveImageBase64(WATERMARK_FILE_ID),
    });

  } catch (err) {
    console.error("LỖI TO: ", err);
    res.status(500).send("Lỗi server: " + err.message);
  }
});




app.use(express.static(path.join(__dirname, 'public')));
// --- Debug ---
app.get("/debug", (_req, res) => {
    res.json({ spreadsheetId: SPREADSHEET_ID, clientEmail: credentials.client_email, gasWebappUrl: GAS_WEBAPP_URL });
});



// Hàm chuyển số thành chữ (thêm vào app.js)
function numberToWords(number) {
    const units = ['', 'một', 'hai', 'ba', 'bốn', 'năm', 'sáu', 'bảy', 'tám', 'chín'];
    const positions = ['', 'nghìn', 'triệu', 'tỷ', 'nghìn tỷ', 'triệu tỷ'];

    if (number === 0) return 'không đồng';

    let words = '';
    let position = 0;

    do {
        const block = number % 1000;
        if (block !== 0) {
            let blockWords = readBlock(block).trim();
            if (positions[position]) {
                blockWords += ' ' + positions[position];
            }
            words = blockWords + ' ' + words;
        }
        position++;
        number = Math.floor(number / 1000);
    } while (number > 0);

    return words.trim() + ' đồng';

    function readBlock(number) {
        let str = '';
        const hundreds = Math.floor(number / 100);
        const tens = Math.floor((number % 100) / 10);
        const ones = number % 10;

        if (hundreds > 0) {
            str += units[hundreds] + ' trăm ';
        }

        if (tens === 0) {
            if (ones > 0 && hundreds > 0) {
                str += 'lẻ ';
            }
        } else if (tens === 1) {
            str += 'mười ';
        } else {
            str += units[tens] + ' mươi ';
        }

        if (ones > 0) {
            if (tens > 1 && ones === 1) {
                str += 'mốt';
            } else if (tens > 0 && ones === 5) {
                str += 'lăm';
            } else {
                str += units[ones];
            }
        }

        return str;
    }
}

// Hàm đọc số thành chữ tiếng Việt (chuẩn hóa)
function numberToWords1(number) {
  if (number === null || number === undefined || isNaN(number)) return '';

  number = Math.floor(Number(number)); // Đảm bảo là số nguyên
  if (number === 0) return 'Không đồng';

  const units = ['không', 'một', 'hai', 'ba', 'bốn', 'năm', 'sáu', 'bảy', 'tám', 'chín'];
  const scales = ['', 'nghìn', 'triệu', 'tỷ', 'nghìn tỷ', 'triệu tỷ'];

  let words = '';
  let scaleIndex = 0;

  while (number > 0) {
    const block = number % 1000;
    if (block > 0) {
      const blockWords = readBlock(block);
      words = blockWords + (scales[scaleIndex] ? ' ' + scales[scaleIndex] + ' ' : ' ') + words;
    }
    number = Math.floor(number / 1000);
    scaleIndex++;
  }

  return words.trim().replace(/\s+/g, ' ') + ' đồng chẵn';

  // ---- HÀM PHỤ ----
  function readBlock(num) {
    let result = '';
    const hundreds = Math.floor(num / 100);
    const tens = Math.floor((num % 100) / 10);
    const ones = num % 10;

    if (hundreds > 0) {
      result += units[hundreds] + ' trăm ';
      if (tens === 0 && ones > 0) result += 'lẻ ';
    }

    if (tens > 1) {
      result += units[tens] + ' mươi ';
      if (ones === 1) result += 'mốt';
      else if (ones === 5) result += 'lăm';
      else if (ones > 0) result += units[ones];
    } else if (tens === 1) {
      result += 'mười ';
      if (ones === 5) result += 'lăm';
      else if (ones > 0) result += units[ones];
    } else if (tens === 0 && hundreds === 0 && ones > 0) {
      result += units[ones];
    } else if (ones > 0) {
      result += units[ones];
    }

    return result.trim();
  }
}


// Hàm chuyển định dạng ngày tháng năm
function formatVietnameseDate(dateStr) {
      try {
        const d = new Date(dateStr);
        if (isNaN(d)) return dateStr; // Nếu không parse được thì trả nguyên
        const day = ("0" + d.getDate()).slice(-2);
        const month = ("0" + (d.getMonth() + 1)).slice(-2);
        const year = d.getFullYear();
        return `Ngày ${day} tháng ${month} năm ${year}`;
      } catch (e) {
        return dateStr;
      }
}
function formatNumber1(num) {
  if (num == null || isNaN(num)) return "0";
  num = Math.abs(Number(num));
  const [int, dec] = num.toFixed(2).split(".");
  const formattedInt = int.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  return dec === "00" ? formattedInt : `${formattedInt},${dec}`;
}
// HÀM CHẠY LỆNH PVC ứng với mã đơn và số lần
app.get("/lenhpvc/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Lệnh PVC ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }

        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);
        if (!donHang)
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Lấy chi tiết sản phẩm PVC ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_PVC_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        // Lọc và map dữ liệu theo cấu trúc của lệnh sản xuất
        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r) => ({
                maDonHangChiTiet: r[2],
                tenThuongMai: r[8],
                dai: r[16],
                rong: r[17],
                cao: r[18],
                slSoi: r[19],
                soLuong: r[21],
                donViTinh: r[22],
                tongSoLuong: r[20],
                tongSLSoi: r[23],
                ghiChuSanXuat: r[28]
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Xác định loại lệnh từ cột S (index 36) ---
        const lenhValue = donHang[28] || '';

        // --- Render ra client (ngay, không chặn UI) ---
        res.render("lenhpvc", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: false,
            maDonHang,
            lenhValue,
            pathToFile: ""
        });

        // ===== PHẦN MỚI: tìm dòng bằng retry/polling =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "File_lenh_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => (r[1] === orderCode) && (r[2] === soLan));
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc File_lenh_ct (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // ===== PHẦN NỀN: Gọi AppScript và ghi đường dẫn =====
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling File_lenh_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "lenhpvc.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        lenhValue,
                        pathToFile: ""
                    }
                );

                const GAS_WEBAPP_URL_LENHPVC = process.env.GAS_WEBAPP_URL_LENHPVC;
                if (!GAS_WEBAPP_URL_LENHPVC) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_LENHPVC - bỏ qua bước gọi GAS");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_LENHPVC, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const result = await resp.json();
                console.log("✔️ AppScript trả về:", result);

                const pathToFile = result.pathToFile || `LENH_PVC/${result.fileName}`;

                // --- Ghi đường dẫn vào đúng dòng ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_lenh_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript hoặc ghi link:", err);
            }
        })().catch(err => console.error("❌ Async background error:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất Lệnh PVC:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

// HÀM CHẠY LỆNH NK ỨNG VỚI MÃ VÀ SỐ LẦN
app.get("/lenhnk/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Lệnh Nhôm Kính ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);
        if (!donHang)
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Lấy chi tiết sản phẩm Nhôm Kính ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_nk_ct!A1:U",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r) => ({
                maDonHangChiTiet: r[2],
                tenThuongMai: r[7],
                dai: r[9],
                rong: r[10],
                cao: r[11],
                dienTich: r[12],
                donViTinh: r[13],
                slBo: r[14],
                tongSoLuong: r[15],
                ghiChuSanXuat: r[20]
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Xác định loại lệnh từ cột S (index 36) ---
        const lenhValue = donHang[28] || '';

        // --- Render ra client (ngay lập tức) ---
        res.render("lenhnk", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            lenhValue,
            pathToFile: ""
        });

        // ===== HÀM MỚI: tìm dòng có retry =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "File_lenh_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => (r[1] === orderCode) && (r[2] === soLan));
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc File_lenh_ct (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // ===== Gọi Apps Script ngầm =====
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling File_lenh_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "lenhnk.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        lenhValue,
                        pathToFile: ""
                    }
                );

                const GAS_WEBAPP_URL_LENHNK = process.env.GAS_WEBAPP_URL_LENHNK;
                if (!GAS_WEBAPP_URL_LENHNK) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_LENHNK - bỏ qua bước gọi GAS");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_LENHNK, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const result = await resp.json();
                console.log("✔️ AppScript trả về:", result);

                const pathToFile = result.pathToFile || `LENH_NK/${result.fileName}`;

                // --- Ghi đường dẫn vào đúng dòng ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_lenh_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);

            } catch (err) {
                console.error("❌ Lỗi khi gọi Apps Script hoặc ghi đường dẫn:", err);
            }
        })().catch(err => console.error("❌ Async background error:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất Lệnh Nhôm Kính:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});


// HÀM CHẠY BÁO GIÁ PVC ỨNG VỚI MÃ ĐƠN VÀ SỐ LẦN
app.get("/baogiapvc/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Báo Giá PVC ...");
        console.log("📘 SPREADSHEET_ID:", process.env.SPREADSHEET_ID);

        // --- Nhận tham số từ URL ---
        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);
        if (!donHang) return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Lấy chi tiết sản phẩm PVC ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_PVC_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        // --- Lọc và map dữ liệu (giữ nguyên logic của bạn) ---
        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r) => ({
                maDonHangChiTiet: r[2],
                tenHangHoa: r[9],
                quyCach: r[10],
                dai: r[16],
                rong: r[17],
                cao: r[18],
                soLuong: r[21],
                donViTinh: r[22],
                tongSoLuong: r[20],
                donGia: r[25],
                vat: r[26] ? parseFloat(r[26]) : null,
                thanhTien: r[27]
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Tính tổng (GIỮ NGUYÊN LOGIC) ---
        let tongTien = 0;
        let chietKhauValue = donHang[32] || "0";
        let chietKhauPercent = parseFloat(chietKhauValue.toString().replace('%', '')) || 0;
        let tamUngValue = donHang[33] || 0;
        let tamUngPercent =parseFloat(tamUngValue.toString().replace('%', '')) || 0;

        products.forEach(product => {
            tongTien += parseFloat(product.thanhTien) || 0;
        });
        let tamUng = (tongTien * tamUngPercent) / 100;
        let chietKhau = (tongTien * chietKhauPercent) / 100;
        let tongThanhTien = tongTien - chietKhau - tamUng;

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render cho client ngay (không chặn UI) ---
        res.render("baogiapvc", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: false,
            maDonHang,
            tongTien,
            chietKhau,
            tamUng,
            tongThanhTien,
            numberToWords,
            pathToFile: ""
        });

        // ----- HÀM HỖ TRỢ: tìm dòng trong File_bao_gia_ct với retry/polling -----
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            // attemptLimit: số lần đọc tối đa
            // initialDelayMs: delay ban đầu giữa các lần (sẽ tăng nhẹ nếu cần)
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "File_bao_gia_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => (r[1] === orderCode) && (r[2] === soLan));
                    if (idx !== -1) {
                        return idx + 1; // trả về số dòng thực tế
                    }
                } catch (e) {
                    console.warn(`⚠️ Lỗi khi đọc File_bao_gia_ct (attempt ${attempt}):`, e.message || e);
                    // tiếp tục retry
                }
                // chờ rồi retry
                await new Promise(r => setTimeout(r, delay));
                // nhẹ tăng dần delay để giảm load
                delay = Math.min(delay + 300, 2000);
            }
            return null; // không tìm được sau attempts
        }

        // ----- Chạy phần nền: tìm dòng (poll) rồi gọi GAS và ghi đường dẫn -----
        (async () => {
            try {
                // cấu hình: cho phép override bằng biến môi trường
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling File_bao_gia_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ... (max ${MAX_ATTEMPTS} attempts)`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);

                if (!rowNumber) {
                    // Không tìm thấy sau nhiều lần retry
                    // KHÔNG gọi res.send() vì đã render rồi — chỉ log rõ để bạn xử lý thủ công
                    console.error(`❌ Sau ${MAX_ATTEMPTS} lần, không tìm thấy dòng cho ${maDonHang} - ${soLan} trong File_bao_gia_ct. Bỏ qua ghi đường dẫn.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng để ghi: ${rowNumber} (sẽ gọi GAS và ghi đường dẫn)`);

                // --- render html (same as before) ---
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "baogiapvc.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        tongTien,
                        chietKhau,
                        tamUng,
                        tongThanhTien,
                        numberToWords,
                        pathToFile: ""
                    }
                );

                const GAS_WEBAPP_URL_BAOGIAPVC = process.env.GAS_WEBAPP_URL_BAOGIAPVC;
                if (!GAS_WEBAPP_URL_BAOGIAPVC) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BAOGIAPVC - bỏ qua bước gọi GAS");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_BAOGIAPVC, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);
                const pathToFile = data.pathToFile || `BAO_GIA_PVC/${data.fileName}`;

                // --- Ghi đường dẫn vào đúng dòng ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_bao_gia_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });

                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);
            } catch (err) {
                console.error("❌ Lỗi gọi AppScript (nền):", err);
            }
        })().catch(err => console.error("❌ Async background error:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất Báo Giá PVC:", err.stack || err.message);
        if (!res.headersSent) {
            res.status(500).send("Lỗi server: " + (err.message || err));
        }
    }
});


/// HÀM BÁO GIÁ NK ỨNG VỚI MÃ ĐƠN VÀ SỐ LẦN
app.get("/baogiank/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Báo Giá Nhôm Kính ...");

        // --- Nhận tham số ---
        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

         // --- HÀM ĐỊNH DẠNG SỐ DUY NHẤT (Fix lỗi dấu ,) ---
        const formatNumber = (num, decimals = 2) => {
            if (num == null || num === undefined || num === '') return "0";
            
            // Nếu là chuỗi có dấu phẩy thập phân (0,20), chuyển thành số
            let numberValue;
            if (typeof num === 'string') {
                // Loại bỏ dấu chấm phân cách hàng nghìn, thay dấu phẩy bằng dấu chấm
                let str = num.trim().replace(/\./g, '').replace(/,/g, '.');
                numberValue = parseFloat(str);
                if (isNaN(numberValue)) return "0";
            } else {
                numberValue = Number(num);
                if (isNaN(numberValue)) return "0";
            }
            
            // Làm tròn và định dạng
            const rounded = Math.abs(numberValue).toFixed(decimals);
            const [intPart, decPart] = rounded.split('.');
            
            // Định dạng phần nguyên với dấu chấm phân cách hàng nghìn
            const formattedInt = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
            
            if (decPart === '00' || decimals === 0) {
                return formattedInt;
            }
            
            return `${formattedInt},${decPart}`;
        };

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BW",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);
        if (!donHang)
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Lấy chi tiết sản phẩm Nhôm Kính ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_nk_ct!A1:U",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r) => ({
                kyHieu: r[5],
                tenHangHoa: r[8],
                dai: r[9],
                rong: r[10],
                cao: r[11],
                dienTich: r[12],
                soLuong: r[14],
                donViTinh: r[13],
                donGia: r[17],
                giaPK: r[16],
                thanhTien: r[19]
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        
        // --- Hàm chuyển đổi chuỗi số Việt Nam thành số ---
        const parseVietNumber = (str) => {
            if (!str && str !== 0) return 0;
            if (typeof str === 'number') return str;
            
            let s = str.toString().trim();
            // Loại bỏ dấu chấm phân cách hàng nghìn
            s = s.replace(/\./g, '');
            // Thay dấu phẩy thập phân bằng dấu chấm
            s = s.replace(/,/g, '.');
            
            const num = parseFloat(s);
            return isNaN(num) ? 0 : num;
        };

        // --- Tính tổng ---
        let tongTien = 0;
        products.forEach(p => tongTien += parseVietNumber(p.thanhTien) || 0);

        // --- Xử lý chiết khấu ---
        let chietKhauValue = donHang[32] || "0";
        let chietKhauPercent = parseVietNumber(chietKhauValue.toString().replace('%', '')) || 0;
        let chietKhau = chietKhauValue.toString().includes('%')
            ? (tongTien * chietKhauPercent) / 100
            : chietKhauPercent;

        let tamUng = parseVietNumber(donHang[33]) || 0;
        let tongThanhTien = tongTien - chietKhau - tamUng;

        // --- Tính tổng diện tích và số lượng ---
        let tongDienTich = 0, tongSoLuong = 0;
        products.forEach(p => {
            const dienTich = parseVietNumber(p.dienTich) || 0;
            const soLuong = parseVietNumber(p.soLuong) || 0;
            tongDienTich += dienTich * soLuong;
            tongSoLuong += soLuong;
        });
        tongDienTich = parseFloat(tongDienTich.toFixed(2));

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64("1766zFeBWPEmjTGQGrrtM34QFbV8fHryb");

        // --- Render cho client ---
        res.render("baogiank", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            tongTien,
            chietKhau,
            tamUng,
            tongThanhTien,
            tongDienTich,
            tongSoLuong,
            numberToWords,
            formatNumber,
            pathToFile: ""
        });

        // ===== PHẦN MỚI: tìm dòng bằng retry/polling =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "File_bao_gia_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => (r[1] === orderCode) && (r[2] === soLan));
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc File_bao_gia_ct (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // ===== Gọi AppScript & ghi đường dẫn (nền, không chặn UI) =====
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling File_bao_gia_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi GAS ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "baogiank.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        tongTien,
                        chietKhau,
                        tamUng,
                        tongThanhTien,
                        tongDienTich,
                        tongSoLuong,
                        numberToWords,
                        formatNumber,
                        pathToFile: ""
                    }
                );

                const GAS_WEBAPP_URL_BAOGIANK = process.env.GAS_WEBAPP_URL_BAOGIANK;
                if (!GAS_WEBAPP_URL_BAOGIANK) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BAOGIANK - bỏ qua bước gọi GAS");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_BAOGIANK, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `BAO_GIA_NK/${data.fileName}`;

                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_bao_gia_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);

            } catch (err) {
                console.error("❌ Lỗi khi gọi AppScript hoặc ghi link:", err);
            }
        })().catch(err => console.error("❌ Async background error:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất Báo Giá Nhôm Kính:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});


////HÀM YCVT KÈM MÃ ĐƠN HÀNG VÀ SỐ LẦN
app.get('/ycvt/:maDonHang-:soLan', async (req, res) => {
    try {
        console.log('▶️ Bắt đầu xuất YCVT ...');

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }

        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy logo và watermark ---
        const [logoBase64, watermarkBase64] = await Promise.all([
            loadDriveImageBase64(LOGO_FILE_ID),
            loadDriveImageBase64(WATERMARK_FILE_ID)
        ]);

        // --- Chuẩn bị dữ liệu (giữ nguyên logic cũ) ---
        const data = await prepareYcvtData(auth, SPREADSHEET_ID, SPREADSHEET_BOM_ID, maDonHang);
        const d4Value = maDonHang;

        // --- Render cho client (ngay, không chặn UI) ---
        res.render('ycvt', {
            ...data,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang: d4Value,
            formatNumber1,
            pathToFile: ''
        });

        // ===== PHẦN MỚI: hàm tìm dòng có retry/polling =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "File_BOM_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => (r[1] === orderCode) && (r[2] === soLan));
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc File_BOM_ct (attempt ${attempt}):`, e.message || e);
                }
                // chờ rồi thử lại
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // ===== PHẦN NỀN: Gọi AppScript và ghi đường dẫn sau khi tìm thấy dòng =====
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling File_BOM_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, 'views', 'ycvt.ejs'),
                    {
                        ...data,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang: d4Value,
                        formatNumber1,
                        pathToFile: ''
                    }
                );

                const GAS_WEBAPP_URL_PYCVT = process.env.GAS_WEBAPP_URL_PYCVT;
                if (!GAS_WEBAPP_URL_PYCVT) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_PYCVT - bỏ qua bước gọi GAS");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_PYCVT, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: new URLSearchParams({
                        orderCode: d4Value,
                        html: renderedHtml
                    })
                });

                const result = await resp.json();
                console.log('✔️ AppScript trả về:', result);

                if (!result.ok) {
                    throw new Error(result.error || 'Lỗi khi gọi Apps Script');
                }

                const pathToFile = result.pathToFile || `YCVT/${result.fileName}`;

                // --- Ghi đường dẫn vào đúng dòng ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_BOM_ct!D${rowNumber}`,
                    valueInputOption: 'RAW',
                    requestBody: { values: [[pathToFile]] }
                });
                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);

            } catch (err) {
                console.error('❌ Lỗi gọi AppScript hoặc ghi link:', err);
            }
        })().catch(err => console.error("❌ Async background error:", err));

    } catch (err) {
        console.error('❌ Lỗi khi xuất YCVT:', err.stack || err.message);
        if (!res.headersSent) {
            res.status(500).send('Lỗi server: ' + (err.message || err));
        }
    }
});


////HÀM BBGN PVC KÈM MÃ ĐƠN HÀNG VÀO SỐ LẦN

app.get("/bbgn/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBGN ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }

        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);

        if (!donHang) {
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);
        }

        // --- Chi tiết sản phẩm ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_PVC_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r, i) => ({
                stt: i + 1,
                tenSanPham: r[9],
                soLuong: r[23],
                donVi: r[22],
                tongSoLuong: r[21],
                ghiChu: "",
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render ra client trước ---
        res.render("bbgn", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            soLan,
            pathToFile: ""
        });

        // ===== HÀM MỚI: tìm dòng có retry (polling) =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "file_BBGN_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => (r[1] === orderCode) && (r[2] === soLan));
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc file_BBGN_ct (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // --- Phần chạy nền (không chặn client) ---
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling file_BBGN_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbgn.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        soLan,
                        pathToFile: ""
                    }
                );

                // --- Gọi AppScript để tạo file PDF / lưu Google Drive ---
                const resp = await fetch(GAS_WEBAPP_URL, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `BBGN/${data.fileName}`;

                // --- Ghi đường dẫn vào đúng dòng ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `file_BBGN_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });

                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);
            } catch (err) {
                console.error("❌ Lỗi chạy nền khi gọi AppScript:", err);
            }
        })().catch(err => console.error("❌ Async IIFE BBGN lỗi:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBGN:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});


///HÀM BBGN NK KÈM MÃ ĐƠN HÀNG VÀ SỐ LẦN
app.get("/bbgnnk/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBGN Nhôm Kính ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);

        if (!donHang) {
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);
        }

        // --- Chi tiết sản phẩm ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_nk_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r, i) => ({
                stt: i + 1,
                tenSanPham: r[8],
                soLuong: r[14],
                donVi: r[13],
                tongSoLuong: r[15],
                ghiChu: "",
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render cho client ngay ---
        res.render("bbgnnk", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            pathToFile: "",
        });

        // ===== Hàm tìm dòng với retry/polling =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "file_BBGN_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => r[1] === orderCode && r[2] === soLan);
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc file_BBGN_ct (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // --- Chạy nền, không chặn client ---
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling file_BBGN_ct để tìm (maDonHang=${maDonHang}, soLan=${soLan}) ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbgnnk.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        pathToFile: "",
                    }
                );

                const GAS_WEBAPP_URL_BBGNNK = process.env.GAS_WEBAPP_URL_BBGNNK;
                if (!GAS_WEBAPP_URL_BBGNNK) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BBGNNK");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_BBGNNK, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml,
                    }),
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `BBGNNK/${data.fileName}`;

                // --- Ghi đường dẫn vào sheet ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `file_BBGN_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });

                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);
            } catch (err) {
                console.error("❌ Lỗi chạy nền khi gọi AppScript:", err);
            }
        })().catch(err => console.error("❌ Async IIFE BBGNNK lỗi:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBGN Nhôm Kính:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//HÀM BBNT PVC KÈM MÃ ĐƠN VÀ SỐ LẦN
app.get("/bbnt/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Biên Bản Nghiệm Thu ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);

        if (!donHang) {
            return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);
        }

        // --- Chi tiết sản phẩm ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_PVC_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r, i) => ({
                stt: i + 1,
                tenSanPham: r[9],
                soLuong: r[23],
                donVi: r[22],
                tongSoLuong: r[21],
                ghiChu: "",
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render ngay cho client ---
        res.render("bbnt", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            pathToFile: "",
        });

        // ===== Hàm tìm dòng với retry/polling =====
        async function findRowWithRetry(orderCode, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: "File_BBNT_ct!A:D",
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => r[1] === orderCode && r[2] === soLan);
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc File_BBNT_ct (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // --- Chạy nền (không chặn client) ---
        (async () => {
            try {
                const MAX_ATTEMPTS = parseInt(process.env.SHEET_POLL_ATTEMPTS, 10) || 20;
                const INITIAL_DELAY_MS = parseInt(process.env.SHEET_POLL_DELAY_MS, 10) || 500;

                console.log(`⏳ Polling File_BBNT_ct để tìm dòng cho ${maDonHang} - ${soLan} ...`);

                const rowNumber = await findRowWithRetry(maDonHang, MAX_ATTEMPTS, INITIAL_DELAY_MS);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau ${MAX_ATTEMPTS} lần.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbnt.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        pathToFile: "",
                    }
                );

                const GAS_WEBAPP_URL_BBNT = process.env.GAS_WEBAPP_URL_BBNT;
                if (!GAS_WEBAPP_URL_BBNT) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BBNT");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_BBNT, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml,
                    }),
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `BBNT/${data.fileName}`;

                // --- Ghi đường dẫn vào sheet ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_BBNT_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });

                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);
            } catch (err) {
                console.error("❌ Lỗi chạy nền khi gọi AppScript:", err);
            }
        })().catch(err => console.error("❌ Async IIFE BBNT lỗi:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBNT:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});


//HÀM BBNT NK KÈM MÃ ĐƠN VÀ SỐ LẦN
app.get("/bbntnk/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBNTNK ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) {
            return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        }
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang =
            data.find((r) => r[5] === maDonHang) ||
            data.find((r) => r[6] === maDonHang);
        if (!donHang) return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Chi tiết sản phẩm ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_nk_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r, i) => ({
                stt: i + 1,
                tenSanPham: r[8],
                soLuong: r[14],
                donVi: r[13],
                tongSoLuong: r[15],
                ghiChu: "",
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render ngay cho client ---
        res.render("bbntnk", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            pathToFile: "",
        });

        // ===== Hàm tìm dòng với retry/polling =====
        async function findRowWithRetry(sheetName, orderCode, soLan, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `${sheetName}!A:D`,
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => r[1] === orderCode && r[2] === soLan);
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc ${sheetName} (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // --- Chạy nền gọi AppScript ---
        (async () => {
            try {
                const rowNumber = await findRowWithRetry("File_BBNT_ct", maDonHang, soLan);
                if (!rowNumber) {
                    console.error(`❌ Không tìm thấy dòng cho ${maDonHang} - ${soLan} sau polling.`);
                    return;
                }

                console.log(`✔️ Đã tìm thấy dòng ${rowNumber}, chuẩn bị gọi Apps Script ...`);

                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbntnk.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        pathToFile: "",
                    }
                );

                const GAS_WEBAPP_URL_BBNTNK = process.env.GAS_WEBAPP_URL_BBNTNK;
                if (!GAS_WEBAPP_URL_BBNTNK) {
                    console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BBNTNK");
                    return;
                }

                const resp = await fetch(GAS_WEBAPP_URL_BBNTNK, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml,
                    }),
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `BBNTNK/${data.fileName}`;

                // --- Ghi đường dẫn vào đúng dòng ---
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_BBNT_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });

                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);
            } catch (err) {
                console.error("❌ Lỗi chạy nền khi gọi AppScript BBNTNK:", err);
            }
        })().catch(err => console.error("❌ Async IIFE BBNTNK lỗi:", err));

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBNTNK:", err.stack || err.message);
        if (!res.headersSent)
            res.status(500).send("Lỗi server: " + (err.message || err));
    }
});


// HÀM GGH KÈM MÃ ĐƠN VÀ SỐ LẦN
app.get("/ggh/:maDonHang-:soLan", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Giấy Giao Hàng ...");

        const { maDonHang, soLan } = req.params;
        if (!maDonHang || !soLan) return res.status(400).send("⚠️ Thiếu tham số mã đơn hàng hoặc số lần.");
        console.log(`✔️ Mã đơn hàng: ${maDonHang}, số lần: ${soLan}`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang = data.find(r => r[5] === maDonHang) || data.find(r => r[6] === maDonHang);
        if (!donHang) return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // --- Logo ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);

        // --- Render ngay cho client ---
        res.render("ggh", {
            donHang,
            logoBase64,
            autoPrint: true,
            maDonHang,
            soLan,
            pathToFile: "",
        });

        // --- Hàm tìm dòng với retry/polling ---
        async function findRowWithRetry(sheetName, orderCode, soLan, attemptLimit = 20, initialDelayMs = 500) {
            let attempt = 0;
            let delay = initialDelayMs;
            while (attempt < attemptLimit) {
                attempt++;
                try {
                    const resp = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `${sheetName}!A:D`,
                    });
                    const rows = resp.data.values || [];
                    const idx = rows.findIndex(r => r[1] === orderCode && r[2] === soLan);
                    if (idx !== -1) return idx + 1;
                } catch (e) {
                    console.warn(`⚠️ Lỗi đọc ${sheetName} (attempt ${attempt}):`, e.message || e);
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay + 300, 2000);
            }
            return null;
        }

        // --- Chạy nền: gọi GAS và ghi đường dẫn ---
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "ggh.ejs"),
                    { donHang, logoBase64, autoPrint: false, maDonHang, soLan, pathToFile: "" }
                );

                const GAS_WEBAPP_URL_GGH = process.env.GAS_WEBAPP_URL_GGH;
                if (!GAS_WEBAPP_URL_GGH) return console.warn("⚠️ Chưa cấu hình GAS_WEBAPP_URL_GGH");

                const resp = await fetch(GAS_WEBAPP_URL_GGH, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({ orderCode: maDonHang, html: renderedHtml }),
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `GGH/${data.fileName}`;

                // --- Polling/retry để tìm dòng trước khi ghi ---
                const rowNumber = await findRowWithRetry("File_GGH_ct", maDonHang, soLan);
                if (!rowNumber) {
                    return console.error(`❌ Không tìm thấy dòng File_GGH_ct cho ${maDonHang} - ${soLan} sau nhiều lần retry.`);
                }

                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_GGH_ct!D${rowNumber}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });

                console.log(`✔️ Đã ghi đường dẫn vào dòng ${rowNumber}: ${pathToFile}`);
            } catch (err) {
                console.error("❌ Lỗi chạy nền GGH:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất GGH:", err.stack || err.message);
        if (!res.headersSent) res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

/// Tạo endpoint /subscribe cho trình duyệt đăng ký
///Tạo endpoint /webhook-from-appsheet để nhận yêu cầu từ AppSheet:
// Route để client lấy public VAPID key
app.get('/get-vapid-key', (req, res) => {
  res.json({ publicKey: publicVapidKey });
});

// Endpoint subscribe mới với username
app.post('/subscribe', async (req, res) => {
  const { subscription, username } = req.body;
  
  if (!subscription || !username) {
    return res.status(400).json({ 
      error: 'Subscription và Username là bắt buộc.' 
    });
  }
  
  // Kiểm tra subscription đã tồn tại chưa
  const existsIndex = pushSubscriptions.findIndex(
    sub => sub.endpoint === subscription.endpoint
  );
  
  const userSubscription = {
    ...subscription,
    username: username.trim().toUpperCase(), // Chuẩn hóa username
    createdAt: new Date().toISOString()
  };
  
  if (existsIndex > -1) {
    // Cập nhật subscription cũ
    pushSubscriptions[existsIndex] = userSubscription;
    console.log(`🔄 Updated subscription for: ${username}`);
  } else {
    // Thêm mới
    pushSubscriptions.push(userSubscription);
    console.log(`✅ New subscription for: ${username}`);
  }
  
  await saveSubscriptions();
  res.json({ 
    success: true, 
    message: `Đã lưu subscription cho ${username}` 
  });
});

// Endpoint webhook mới với logic thông minh
app.post('/webhook-from-appsheet', async (req, res) => {
  try {
    const orderData = req.body;
    console.log('📨 Nhận webhook:', orderData.ma_dh);
    
    // 1. Xác định trạng thái cần xử lý
    let statusField = null;
    let statusValue = null;
    
    // Kiểm tra các trường trạng thái theo thứ tự ưu tiên
    const statusFields = ['tiep_nhan_don_hang', 'Phe_duyet', 'tinh_trang_tao_don'];
    for (const field of statusFields) {
      if (orderData[field]) {
        statusField = field;
        statusValue = orderData[field];
        break;
      }
    }
    
    if (!statusValue) {
      console.log('⚠️ Không xác định được trạng thái');
      return res.json({ success: false, message: 'Không xác định được trạng thái' });
    }
    
    // 2. Tìm rule phù hợp
    const rule = notificationRules[statusValue];
    if (!rule) {
      console.log(`⚠️ Không có rule cho trạng thái: ${statusValue}`);
      return res.json({ success: false, message: 'Không có rule phù hợp' });
    }
    
    // 3. Lấy danh sách người nhận
    const targetUsernames = getTargetUsernames(rule, orderData);
    console.log(`🎯 Người nhận: ${targetUsernames.join(', ')}`);
    
    // 4. Lọc subscriptions
    const subscriptionsToNotify = pushSubscriptions.filter(sub => 
      targetUsernames.includes(sub.username)
    );
    
    console.log(`📋 Tìm thấy ${subscriptionsToNotify.length} subscriptions`);
    
    if (subscriptionsToNotify.length === 0) {
      return res.json({ success: true, message: 'Không có người nhận phù hợp' });
    }
    
    // 5. Tạo thông báo
    const notificationPayload = {
      title: rule.titleTemplate(orderData),
      body: rule.bodyTemplate(orderData),
      data: {
        url: orderData.url || `https://appsheet.com/start/YourAppID#view=OrderDetail&row=${orderData.id}`,
        orderId: orderData.ma_dh,
        status: statusValue
      }
    };
    
    // 6. Gửi thông báo (giữ nguyên logic gửi cũ)
    const payload = JSON.stringify(notificationPayload);
    const results = [];
    
    for (let i = 0; i < subscriptionsToNotify.length; i++) {
      const sub = subscriptionsToNotify[i];
      try {
        await webPush.sendNotification(sub, payload);
        results.push({ username: sub.username, status: 'success' });
      } catch (err) {
        console.error(`❌ Lỗi gửi cho ${sub.username}:`, err.message);
        // Xử lý subscription hết hạn (410)
        if (err.statusCode === 410) {
          pushSubscriptions = pushSubscriptions.filter(s => s.endpoint !== sub.endpoint);
        }
        results.push({ username: sub.username, status: 'failed', error: err.message });
      }
    }
    
    // 7. Lưu subscriptions (nếu có thay đổi)
    await saveSubscriptions();
    
    res.json({
      success: true,
      message: `Đã xử lý thông báo cho ${statusValue}`,
      details: {
        order: orderData.ma_dh,
        status: statusValue,
        targetCount: targetUsernames.length,
        sentCount: results.filter(r => r.status === 'success').length,
        failedCount: results.filter(r => r.status === 'failed').length
      }
    });
    
  } catch (error) {
    console.error('💥 Lỗi xử lý webhook:', error);
    res.status(500).json({ error: 'Lỗi server', details: error.message });
  }
});

// Endpoint quản lý subscriptions
app.get('/admin/subscriptions', (req, res) => {
  const summary = {};
  pushSubscriptions.forEach(sub => {
    if (!summary[sub.username]) {
      summary[sub.username] = { count: 0, devices: [] };
    }
    summary[sub.username].count++;
    summary[sub.username].devices.push({
      endpoint: sub.endpoint.substring(0, 50) + '...',
      created: sub.createdAt
    });
  });
  
  res.json({
    totalSubscriptions: pushSubscriptions.length,
    totalUsers: Object.keys(summary).length,
    users: summary
  });
});


///KHOÁN DỊCH VỤ

import exceljs from 'exceljs';

// Thêm route mới sau các route khác
app.get("/baoluongkhoan", async (req, res) => {
    try {
        const { monthYear, page = 1, exportExcel } = req.query;
        const currentPage = parseInt(page);
        const perPage = 10;

        if (!monthYear) {
            // Nếu không có tháng/năm, chỉ render form
            return res.render("baocaoluongkhoan", {
                monthYear: "",
                data: null,
                currentPage: 1,
                totalPages: 0,
                table1Data: [],
                table2Data: [],
                table3Data: [],
                table4Data: [],
                table5Data: [],
                totalRecords: 0,
                totalAmount: 0
            });
        }

        // Parse tháng/năm từ định dạng MM/YYYY
        const [month, year] = monthYear.split('/').map(num => parseInt(num));

        // Lấy dữ liệu từ sheet Danh_sach_don_tra_khoan_giao_van
        const sheet1Range = 'Danh_sach_don_tra_khoan_giao_van!A2:Z';
        const sheet1Response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: sheet1Range,
        });
        
        const sheet1Data = sheet1Response.data.values || [];
        
        // Lấy dữ liệu từ sheet TT_khoan_lap_dat
        const sheet2Range = 'TT_khoan_lap_dat!A2:Z';
        const sheet2Response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: sheet2Range,
        });
        
        const sheet2Data = sheet2Response.data.values || [];

        // Lấy dữ liệu từ sheet Bang_luong_khoan_theo_thang
        const sheet3Range = 'Bang_luong_khoan_theo_thang!A2:Z';
        const sheet3Response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: sheet3Range,
        });
        
        const sheet3Data = sheet3Response.data.values || [];

        // Hàm chuyển đổi chuỗi ngày tháng
        const parseDate = (dateString) => {
            if (!dateString) return null;
            
            // Thử parse từ dd/mm/yyyy
            if (typeof dateString === 'string' && dateString.includes('/')) {
                const parts = dateString.split('/');
                if (parts.length === 3) {
                    const day = parseInt(parts[0]);
                    const month = parseInt(parts[1]);
                    const year = parseInt(parts[2]);
                    return new Date(year, month - 1, day);
                }
            }
            
            // Thử parse từ serial date của Google Sheets
            if (typeof dateString === 'number') {
                // Google Sheets date serial (days since Dec 30, 1899)
                const date = new Date((dateString - 25569) * 86400 * 1000);
                return isNaN(date.getTime()) ? null : date;
            }
            
            // Thử parse từ Date object string
            const date = new Date(dateString);
            return isNaN(date.getTime()) ? null : date;
        };

        // Lọc dữ liệu sheet1 theo tháng/năm
        const filteredSheet1Data = sheet1Data.filter(row => {
            if (!row[1]) return false; // Cột B (index 1)
            
            const date = parseDate(row[1]);
            if (!date) return false;
            
            return date.getMonth() + 1 === month && date.getFullYear() === year;
        });

        // Lọc dữ liệu sheet2 theo tháng/năm
        const filteredSheet2Data = sheet2Data.filter(row => {
            if (!row[12]) return false; // Cột M (index 12)
            
            const date = parseDate(row[12]);
            if (!date) return false;
            
            return date.getMonth() + 1 === month && date.getFullYear() === year;
        });

        // Xử lý bảng 1: DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN
        const table1Data = filteredSheet1Data.map((row, index) => ({
            stt: index + 1,
            maDonHang: row[3] || '', // D
            nhomSanPham: row[4] || '', // E
            loaiDonHang: row[5] || '', // F
            taiTrong: row[6] || '', // G
            nhanSu: row[9] || '', // J
            donGia: parseFloat(row[10] || 0), // K
            thanhTien: parseFloat(row[11] || 0) // L
        }));

        // Tính tổng
        const totalRecords = table1Data.length;
        const totalAmount = table1Data.reduce((sum, item) => sum + item.thanhTien, 0);
        
        // Phân trang cho bảng 1
        const startIndex = (currentPage - 1) * perPage;
        const endIndex = startIndex + perPage;
        const paginatedTable1Data = table1Data.slice(startIndex, endIndex);
        const totalPages = Math.ceil(totalRecords / perPage);

        // Xử lý bảng 2: TỔNG HỢP KHOÁN GIAO VẬN THEO NHÂN SỰ/LOẠI ĐƠN HÀNG
        const groupMap = new Map();
        
        filteredSheet1Data.forEach(row => {
            const nhanSu = row[9] || 'Không xác định';
            const loaiDonHang = row[5] || 'Không xác định';
            const thanhTien = parseFloat(row[11] || 0);
            
            const key = `${nhanSu}|${loaiDonHang}`;
            
            if (groupMap.has(key)) {
                const existing = groupMap.get(key);
                existing.thanhTien += thanhTien;
            } else {
                groupMap.set(key, {
                    nhanSu,
                    loaiDonHang,
                    thanhTien
                });
            }
        });
        
        const table2Data = Array.from(groupMap.values()).map((item, index) => ({
            stt: index + 1,
            ...item
        }));

        // Xử lý bảng 3: TỔNG HỢP CHI TRẢ KHOÁN GIAO VẬN
        const staffSummary = new Map();
        
        table2Data.forEach(item => {
            if (staffSummary.has(item.nhanSu)) {
                staffSummary.set(item.nhanSu, staffSummary.get(item.nhanSu) + item.thanhTien);
            } else {
                staffSummary.set(item.nhanSu, item.thanhTien);
            }
        });
        
        const table3Data = Array.from(staffSummary.entries()).map(([nhanSu, thanhTien], index) => ({
            stt: index + 1,
            nhanSu,
            thanhTien,
            ghiChu: ''
        }));

        // Xử lý bảng 4: Danh sách đơn hàng trả khoán lắp đặt
        const orderMap = new Map();
        
        filteredSheet2Data.forEach(row => {
            const maDonHang = row[3] || ''; // D
            const thanhTien = parseFloat(row[9] || 0); // J
            
            if (maDonHang && !orderMap.has(maDonHang)) {
                orderMap.set(maDonHang, {
                    maDonHang,
                    thanhTien,
                    thucChi: thanhTien,
                    ghiChu: ''
                });
            }
        });
        
        const table4Data = Array.from(orderMap.values()).map((item, index) => ({
            stt: index + 1,
            ...item
        }));

        // Xử lý bảng 5: TỔNG LƯƠNG KHOÁN DỊCH VỤ
        function parseNumberFromSheet(value) {
            if (value === null || value === undefined || value === '') return 0;

            // Nếu đã là số, trả về
            if (typeof value === 'number') return value;

            // Nếu là chuỗi, xử lý
            const str = String(value).trim();

            // Loại bỏ tất cả dấu chấm, dấu phẩy và khoảng trắng
            let cleaned = str.replace(/\./g, '') // Loại bỏ dấu chấm phân cách nghìn
                .replace(/,/g, '.') // Thay dấu phẩy thành dấu chấm (nếu có phần thập phân)
                .replace(/\s/g, ''); // Loại bỏ khoảng trắng


            // Parse thành số
            const num = parseFloat(cleaned);
            return isNaN(num) ? 0 : num;
        }

const table5Data = sheet3Data
    .filter(row => {
        // Kiểm tra xem có mã nhân viên và không phải hàng trống
        return row[1] && row[1].toString().trim() !== '';
    })
    .map((row, index) => {
        // Parse các giá trị số
        const thanhTienGiaoVan = parseNumberFromSheet(row[5]); // F
        const thanhTienLapDat = parseNumberFromSheet(row[6]); // G
        const tongThanhTien = parseNumberFromSheet(row[7]); // H
        
        console.log(`Row ${index + 1}:`, {
            maNV: row[1],
            hoTen: row[2],
            giaoVanRaw: row[5],
            giaoVanParsed: thanhTienGiaoVan,
            lapDatRaw: row[6],
            lapDatParsed: thanhTienLapDat,
            tongRaw: row[7],
            tongParsed: tongThanhTien
        });
        
        return {
            stt: index + 1,
            maNhanVien: row[1] ? row[1].toString().trim() : '',
            hoTen: row[2] ? row[2].toString().trim() : '',
            thanhTienGiaoVan: thanhTienGiaoVan,
            thanhTienLapDat: thanhTienLapDat,
            tongThanhTien: tongThanhTien,
            tamUng: 0,
            thucLinh: tongThanhTien, // Ban đầu bằng tổng thành tiền
            stk: row[8] ? row[8].toString().trim() : '',
            nganHang: row[9] ? row[9].toString().trim() : '',
            chuTaiKhoan: row[10] ? row[10].toString().trim() : ''
        };
    })
    .filter(item => item.tongThanhTien > 0 || item.thanhTienGiaoVan > 0 || item.thanhTienLapDat > 0); // Lọc những dòng có giá trị

    ///XỬ LÝ BẢNG KHOÁN LẮP ĐẶT

    const table6Data = sheet3Data
    .filter(row => {
        // Kiểm tra xem có mã nhân viên và không phải hàng trống
        return row[1] && row[1].toString().trim() !== '';
    })
    .map((row, index) => {
        // Parse các giá trị số
        const thanhTienLapDat = parseNumberFromSheet(row[6]); // G
        
        console.log(`Row ${index + 1}:`, {
            maNV: row[1],
            hoTen: row[2],
            lapDatRaw: row[6],
            lapDatParsed: thanhTienLapDat,
        });
        
        return {
            stt: index + 1,
            maNhanVien: row[1] ? row[1].toString().trim() : '',
            hoTen: row[2] ? row[2].toString().trim() : '',
            thanhTienLapDat: thanhTienLapDat,
            ghiChu: ''
        };
    })
    .filter(item => item.thanhTienLapDat > 0); // Lọc những dòng có giá trị
// Format số với dấu phẩy phân cách hàng nghìn

        function formatNumber(num) {
            if (num === null || num === undefined) return '0';
            const number = parseFloat(num);
            if (isNaN(number)) return '0';
            return new Intl.NumberFormat('vi-VN').format(number);
        }



        // Nếu yêu cầu xuất Excel
        if (exportExcel === 'true') {
            const workbook = new exceljs.Workbook();
            
            // Sheet 1: DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN
            const sheet1 = workbook.addWorksheet('Danh sách đơn hàng trả khoán giao vận');
            
            // Tiêu đề sheet
            sheet1.mergeCells('A1:H1');
            sheet1.getCell('A1').value = 'DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN';
            sheet1.getCell('A1').font = { bold: true, size: 16 };
            sheet1.getCell('A1').alignment = { horizontal: 'center' };
            
            // Thông tin tháng/năm
            sheet1.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            // Tổng số đơn và tổng thành tiền
            sheet1.getCell('A3').value = `Tổng đơn giao vận: ${totalRecords}`;
            sheet1.getCell('A4').value = `Tổng thành tiền: ${formatNumber(totalAmount)}`;
            
            // Header bảng
            const headers1 = ['STT', 'Mã đơn hàng', 'Nhóm SP', 'Loại đơn hàng', 'Tải trọng/Kích thước', 'Nhân sự thực hiện', 'Đơn giá', 'Thành tiền'];
            sheet1.getRow(6).values = headers1;

            // Style cho header
            const headerRow1 = sheet1.getRow(6);
            headerRow1.font = { bold: true };
            headerRow1.alignment = { horizontal: 'center' };
            
            // Thêm dữ liệu
            table1Data.forEach(item => {
                sheet1.addRow([
                    item.stt,
                    item.maDonHang,
                    item.nhomSanPham,
                    item.loaiDonHang,
                    item.taiTrong,
                    item.nhanSu,
                    formatNumber(item.donGia),
                    formatNumber(item.thanhTien)
                ]);
            });
            
            // Định dạng cột
            sheet1.columns = [
                { width: 8 },  // STT
                { width: 20 }, // Mã đơn
                { width: 15 }, // Nhóm SP
                { width: 20 }, // Loại đơn
                { width: 20 }, // Tải trọng
                { width: 20 }, // Nhân sự
                { width: 15 }, // Đơn giá
                { width: 15 }  // Thành tiền
            ];
            
            // Thêm border cho toàn bộ bảng
            for (let i = 6; i <= sheet1.rowCount; i++) {
                for (let j = 1; j <= 8; j++) {
                    const cell = sheet1.getCell(i, j);
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (j === 7 || j === 8) { // Cột đơn giá và thành tiền
                        cell.numFmt = '#,##0';
                    }
                }
            }
            
            // Sheet 2: TỔNG HỢP KHOÁN GIAO VẬN THEO NHÂN SỰ/LOẠI ĐƠN HÀNG
            const sheet2 = workbook.addWorksheet('Tổng hợp khoán giao vận');
            
            sheet2.mergeCells('A1:D1');
            sheet2.getCell('A1').value = 'TỔNG HỢP KHOÁN GIAO VẬN THEO NHÂN SỰ THỰC HIỆN/LOẠI ĐƠN HÀNG';
            sheet2.getCell('A1').font = { bold: true, size: 16 };
            sheet2.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet2.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers2 = ['STT', 'Nhân sự thực hiện', 'Loại đơn hàng', 'Thành tiền'];

            sheet2.getRow(4).values = headers2;

            const headerRow2 = sheet2.getRow(4);
            headerRow2.font = { bold: true };
            headerRow2.alignment = { horizontal: 'center' };
            
            table2Data.forEach(item => {
                sheet2.addRow([
                    item.stt,
                    item.nhanSu,
                    item.loaiDonHang,
                    formatNumber(item.thanhTien)
                ]);
            });
            
            sheet2.columns = [
                { width: 8 },
                { width: 25 },
                { width: 20 },
                { width: 15 }
            ];
            
            for (let i = 4; i <= sheet2.rowCount; i++) {
                for (let j = 1; j <= 4; j++) {
                    const cell = sheet2.getCell(i, j);
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (j === 4) {
                        cell.numFmt = '#,##0';
                    }
                }
            }
            
            // Sheet 3: TỔNG HỢP CHI TRẢ KHOÁN GIAO VẬN
            const sheet3 = workbook.addWorksheet('Tổng hợp chi trả khoán GV theo loại ĐH');
            
            sheet3.mergeCells('A1:D1');
            sheet3.getCell('A1').value = 'TỔNG HỢP CHI TRẢ KHOÁN GIAO VẬN';
            sheet3.getCell('A1').font = { bold: true, size: 16 };
            sheet3.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet3.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers3 = ['STT', 'Tên nhân sự', 'Thành tiền', 'Ghi chú'];

            sheet3.getRow(4).values = headers3;
            
            const headerRow3 = sheet3.getRow(4);
            headerRow3.font = { bold: true };
            headerRow3.alignment = { horizontal: 'center' };
            
            table3Data.forEach(item => {
                sheet3.addRow([
                    item.stt,
                    item.nhanSu,
                    formatNumber(item.thanhTien),
                    item.ghiChu
                ]);
            });
            
            sheet3.columns = [
                { width: 8 },
                { width: 25 },
                { width: 15 },
                { width: 30 }
            ];
            
            for (let i = 4; i <= sheet3.rowCount; i++) {
                for (let j = 1; j <= 4; j++) {
                    const cell = sheet3.getCell(i, j);
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (j === 3) {
                        cell.numFmt = '#,##0';
                    }
                }
            }
            
            // Sheet 4: DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN LẮP ĐẶT
            const sheet4 = workbook.addWorksheet('Danh sách đơn khoán lắp đặt');
            
            sheet4.mergeCells('A1:E1');
            sheet4.getCell('A1').value = 'DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN LẮP ĐẶT';
            sheet4.getCell('A1').font = { bold: true, size: 16 };
            sheet4.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet4.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers4 = ['STT', 'Mã đơn hàng', 'Thành tiền', 'Thực chi', 'Ghi chú'];

            sheet4.getRow(4).values = headers4;

            const headerRow4 = sheet4.getRow(4);
            headerRow4.font = { bold: true };
            headerRow4.alignment = { horizontal: 'center' };
            
            table4Data.forEach(item => {
                sheet4.addRow([
                    item.stt,
                    item.maDonHang,
                    formatNumber(item.thanhTien),
                    formatNumber(item.thucChi),
                    item.ghiChu
                ]);
            });
            
            sheet4.columns = [
                { width: 8 },
                { width: 20 },
                { width: 15 },
                { width: 15 },
                { width: 30 }
            ];
            
            for (let i = 4; i <= sheet4.rowCount; i++) {
                for (let j = 1; j <= 5; j++) {
                    const cell = sheet4.getCell(i, j);
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (j === 3 || j === 4) {
                        cell.numFmt = '#,##0';
                    }
                }
            }

             // Sheet 5: TỔNG LƯƠNG KHOÁN LẮP ĐẶT
            const sheet5 = workbook.addWorksheet('Tổng hợp lương khoán lắp đặt theo nhân sự');
            
            sheet5.mergeCells('A1:E1');
            sheet5.getCell('A1').value = 'TỔNG LƯƠNG KHOÁN LẮP ĐẶT';
            sheet5.getCell('A1').font = { bold: true, size: 16 };
            sheet5.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet5.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers5 = [
                'STT', 
                'Mã nhân viên', 
                'Họ tên', 
                'Thành tiền khoán lắp đặt',
                'Ghi chú'
            ];
            sheet5.getRow(4).values = headers5;
            
            const headerRow5 = sheet5.getRow(4);
            headerRow5.font = { bold: true };
            headerRow5.alignment = { horizontal: 'center' };
            
            table6Data.forEach(item => {
                sheet5.addRow([
                    item.stt,
                    item.maNhanVien,
                    item.hoTen,
                    formatNumber(item.thanhTienLapDat),
                    item.ghiChu
                ]);
            });
            
            sheet5.columns = [
                { width: 8 },    // STT
                { width: 15 },   // Mã NV
                { width: 25 },   // Họ tên
                { width: 20 },   // Khoán lắp đặt
                { width: 20 },   // Ghi chú

            ];
            
            for (let i = 4; i <= sheet5.rowCount; i++) {
                for (let j = 1; j <= 5; j++) {
                    const cell = sheet5.getCell(i, j);
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (j >= 4 && j <= 8) { // Cột tiền từ 4-8
                        cell.numFmt = '#,##0';
                    }
                }
            }
            
            // Sheet 5: TỔNG LƯƠNG KHOÁN DỊCH VỤ
            const sheet6 = workbook.addWorksheet('Tổng hợp khoán dịch vụ theo nhân sự');
            
            sheet6.mergeCells('A1:K1');
            sheet6.getCell('A1').value = 'TỔNG LƯƠNG KHOÁN DỊCH VỤ';
            sheet6.getCell('A1').font = { bold: true, size: 16 };
            sheet6.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet6.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers6 = [
                'STT', 
                'Mã nhân viên', 
                'Họ tên', 
                'Thành tiền khoán giao vận', 
                'Thành tiền khoán lắp đặt',
                'Tổng thành tiền',
                'Tạm ứng',
                'Thực lĩnh',
                'STK ngân hàng',
                'Ngân hàng',
                'Chủ tài khoản'
            ];
            sheet6.getRow(4).values = headers6;
            
            const headerRow6 = sheet6.getRow(4);
            headerRow6.font = { bold: true };
            headerRow6.alignment = { horizontal: 'center' };
            
            table5Data.forEach(item => {
                sheet6.addRow([
                    item.stt,
                    item.maNhanVien,
                    item.hoTen,
                    formatNumber(item.thanhTienGiaoVan),
                    formatNumber(item.thanhTienLapDat),
                    formatNumber(item.tongThanhTien),
                    formatNumber(item.tamUng),
                    formatNumber(item.thucLinh),
                    item.stk,
                    item.nganHang,
                    item.chuTaiKhoan
                ]);
            });
            
            sheet6.columns = [
                { width: 8 },    // STT
                { width: 15 },   // Mã NV
                { width: 25 },   // Họ tên
                { width: 20 },   // Khoán giao vận
                { width: 20 },   // Khoán lắp đặt
                { width: 15 },   // Tổng thành tiền
                { width: 15 },   // Tạm ứng
                { width: 15 },   // Thực lĩnh
                { width: 20 },   // STK
                { width: 15 },   // Ngân hàng
                { width: 25 }    // Chủ tài khoản
            ];
            
            for (let i = 4; i <= sheet6.rowCount; i++) {
                for (let j = 1; j <= 11; j++) {
                    const cell = sheet6.getCell(i, j);
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (j >= 4 && j <= 8) { // Cột tiền từ 4-8
                        cell.numFmt = '#,##0';
                    }
                }
            }
            
            // Xuất file
            res.setHeader(
                'Content-Type',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            );
            res.setHeader(
                'Content-Disposition',
                `attachment; filename="Khoan_giao_van_${monthYear.replace('/', '_')}.xlsx"`
            );
            
            await workbook.xlsx.write(res);
            return res.end();
        }

       // Render template với dữ liệu
res.render("baocaoluongkhoan", {
    monthYear,
    data: {
        table1: paginatedTable1Data,
        table2: table2Data,
        table3: table3Data,
        table4: table4Data,
        table5: table5Data,
        table6: table6Data
    },
    currentPage,
    totalPages,
    table1Data: paginatedTable1Data,
    table2Data,
    table3Data,
    table4Data,
    table5Data,
    table6Data,
    totalRecords,
    totalAmount,
    formatNumber: (num) => {
        if (num === null || num === undefined) return '0';
        const number = parseFloat(num);
        if (isNaN(number)) return '0';
        return new Intl.NumberFormat('vi-VN').format(number);
    }
});

    } catch (error) {
        console.error('Lỗi khi lấy dữ liệu báo cáo:', error);
        res.status(500).render('error', { 
            message: 'Đã xảy ra lỗi khi tải dữ liệu báo cáo',
            error: process.env.NODE_ENV === 'development' ? error.message : null
        });
    }
});
/// XUẤT EXCEL CHO BẢNG LẮP ĐẶT
app.get("/baoluongkhoan/export-installation", async (req, res) => {
    try {
        const { monthYear } = req.query;
        
        if (!monthYear) {
            return res.status(400).send('Vui lòng chọn tháng/năm');
        }

        const [month, year] = monthYear.split('/').map(num => parseInt(num));

        // Lấy dữ liệu từ sheet TT_khoan_lap_dat
        const sheet2Range = 'TT_khoan_lap_dat!A2:Z';
        const sheet2Response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: sheet2Range,
        });
        
        const sheet2Data = sheet2Response.data.values || [];

        // Hàm parse date
        const parseDate = (dateString) => {
            if (!dateString) return null;
            
            if (typeof dateString === 'string' && dateString.includes('/')) {
                const parts = dateString.split('/');
                if (parts.length === 3) {
                    const day = parseInt(parts[0]);
                    const month = parseInt(parts[1]);
                    const year = parseInt(parts[2]);
                    return new Date(year, month - 1, day);
                }
            }
            
            if (typeof dateString === 'number') {
                const date = new Date((dateString - 25569) * 86400 * 1000);
                return isNaN(date.getTime()) ? null : date;
            }
            
            const date = new Date(dateString);
            return isNaN(date.getTime()) ? null : date;
        };

        // Lọc dữ liệu
        const filteredSheet2Data = sheet2Data.filter(row => {
            if (!row[12]) return false;
            
            const date = parseDate(row[12]);
            if (!date) return false;
            
            return date.getMonth() + 1 === month && date.getFullYear() === year;
        });

        // Xử lý dữ liệu
        const orderMap = new Map();
        
        filteredSheet2Data.forEach(row => {
            const maDonHang = row[3] || '';
            const thanhTien = parseFloat(row[9] || 0);
            
            if (maDonHang && !orderMap.has(maDonHang)) {
                orderMap.set(maDonHang, {
                    maDonHang,
                    thanhTien,
                    thucChi: thanhTien,
                    ghiChu: ''
                });
            }
        });
        
        const tableData = Array.from(orderMap.values()).map((item, index) => ({
            stt: index + 1,
            ...item
        }));

        // Tạo Excel
        const workbook = new exceljs.Workbook();
        const worksheet = workbook.addWorksheet('Danh sách đơn khoán lắp đặt');
        
        // Format số
        const formatNumber = (num) => {
            return new Intl.NumberFormat('vi-VN').format(num);
        };

        // Tiêu đề
        worksheet.mergeCells('A1:E1');
        worksheet.getCell('A1').value = 'DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN LẮP ĐẶT';
        worksheet.getCell('A1').font = { bold: true, size: 16 };
        worksheet.getCell('A1').alignment = { horizontal: 'center' };
        
        worksheet.getCell('A2').value = `Tháng/Năm: ${monthYear}`;

        // Header
        const headers = ['STT', 'Mã đơn hàng', 'Thành tiền', 'Thực chi', 'Ghi chú'];
        worksheet.addRow(headers);
        
        const headerRow = worksheet.getRow(4);
        headerRow.font = { bold: true };
        headerRow.alignment = { horizontal: 'center' };

        // Dữ liệu
        tableData.forEach(item => {
            worksheet.addRow([
                item.stt,
                item.maDonHang,
                formatNumber(item.thanhTien),
                formatNumber(item.thucChi),
                item.ghiChu
            ]);
        });

        // Định dạng cột
        worksheet.columns = [
            { width: 8 },
            { width: 20 },
            { width: 15 },
            { width: 15 },
            { width: 30 }
        ];

        // Thêm border
        for (let i = 4; i <= worksheet.rowCount; i++) {
            for (let j = 1; j <= 5; j++) {
                const cell = worksheet.getCell(i, j);
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                if (j === 3 || j === 4) {
                    cell.numFmt = '#,##0';
                }
            }
        }

        // Xuất file
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename="Danh_sach_don_khoan_lap_dat_${monthYear.replace('/', '_')}.xlsx"`
        );
        
        await workbook.xlsx.write(res);
        return res.end();

    } catch (error) {
        console.error('Lỗi xuất Excel:', error);
        res.status(500).send('Lỗi khi xuất file Excel');
    }
});

// --- Route phân bổ khoán lắp đặt theo từng nhân viên ---
// Thêm helper functions cho EJS khoán lắp đặt

app.locals.formatNumber = function(num, decimals = 0) {
  if (num === null || num === undefined || num === '') return "0";
  
  // Nếu là chuỗi có dấu phẩy thập phân, chuyển thành dấu chấm
  if (typeof num === 'string') {
    num = num.replace(',', '.');
  }
  
  num = Math.abs(parseFloat(num)); // luôn lấy giá trị dương
  if (isNaN(num)) return "0";
  
  // Làm tròn đến số chữ số thập phân được chỉ định
  let fixedNum = num.toFixed(decimals);
  
  // Tách phần nguyên và phần thập phân
  let parts = fixedNum.toString().split(".");
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  
  // Nếu decimals > 0 thì nối phần thập phân với dấu phẩy
  if (decimals > 0 && parts.length > 1) {
    return parts[0] + "," + parts[1];
  }
  
  return parts[0];
};

app.locals.formatDate = function(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return dateStr;
    return date.toLocaleDateString('vi-VN');
};


// Route báo cáo khoán lắp đặt theo nhân sự
app.get("/lapdat-:manv", async (req, res) => {
    try {
        const { manv } = req.params;
        const { thang, nam } = req.query;
        
        // Lấy dữ liệu từ sheet TT_khoan_lap_dat
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: "TT_khoan_lap_dat!A:N",
        });

        const data = response.data.values || [];
        if (data.length === 0) {
            return res.render("phanbolapdat", {
                manv: manv || '',
                thang: thang || '',
                nam: nam || '',
                hoTen: '',
                table1: [],
                table2: [],
                tongTien: 0,
                tongDonHang: 0,
                error: "Không có dữ liệu trong sheet"
            });
        }

        // Xác định index cột
        const colIndex = {
            hoTen: 1,        // Cột B
            maNV: 2,         // Cột C
            maDonHang: 3,    // Cột D
            maLanThucHien: 4, // Cột E
            vaiTro: 5,       // Cột F
            heSoDiem: 6,     // Cột G
            tongDiemHeSo: 8, // Cột I (bỏ qua cột H index 7)
            tienKhoanLD: 9,  // Cột J
            donGiaTB: 10,    // Cột K
            thanhTien: 11,   // Cột L
            ngayBaoCao: 12,  // Cột M
            xacNhanThanhToan: 13 // Cột N
        };

        // Bỏ qua dòng tiêu đề nếu có
        let dataRows = data;
        if (data.length > 0) {
            const firstRow = data[0];
            if (firstRow[colIndex.maNV] === "Mã nhân viên" || 
                firstRow[colIndex.hoTen] === "Họ và tên") {
                dataRows = data.slice(1);
            }
        }

        // Lọc dữ liệu theo mã nhân viên, tháng/năm và điều kiện xác nhận thanh toán
        let filteredData = dataRows.filter(row => {
            const maNV = row[colIndex.maNV] || '';
            const ngayStr = row[colIndex.ngayBaoCao] || '';
            const xacNhanTT = row[colIndex.xacNhanThanhToan] || '';
            
            // Kiểm tra mã nhân viên
            const matchMaNV = !manv || maNV.toString().trim() === manv.trim();
            
            // Kiểm tra điều kiện xác nhận thanh toán
            const matchXacNhan = xacNhanTT.toString().trim().toLowerCase() === "xác nhận thanh toán";
            
            // Kiểm tra ngày tháng
            let matchDate = true;
            if (thang && nam) {
                const ngay = parseGoogleSheetDate(ngayStr);
                if (ngay) {
                    const rowMonth = ngay.getMonth() + 1;
                    const rowYear = ngay.getFullYear();
                    matchDate = rowMonth === parseInt(thang) && rowYear === parseInt(nam);
                } else {
                    matchDate = false;
                }
            }
            
            return matchMaNV && matchDate && matchXacNhan;
        });

        // Lấy thông tin nhân viên (nếu có dữ liệu)
        let hoTen = '';
        if (filteredData.length > 0) {
            hoTen = filteredData[0][colIndex.hoTen] || '';
        }

        // Xử lý dữ liệu cho bảng 1: Tổng hợp các đơn hàng
        const donHangMap = new Map();
        
        filteredData.forEach(row => {
            const maDonHang = row[colIndex.maDonHang] || '';
            const tienKhoan = parseFloat(row[colIndex.tienKhoanLD] || 0);
            const tongHeSo = parseFloat(row[colIndex.tongDiemHeSo] || 0);
            
            if (maDonHang) {
                if (!donHangMap.has(maDonHang)) {
                    donHangMap.set(maDonHang, {
                        maDonHang: maDonHang,
                        giaTriKhoan: 0,
                        tongHeSoDiem: 0
                    });
                }
                
                const donHang = donHangMap.get(maDonHang);
                // Lấy giá trị lớn nhất hoặc cộng dồn tùy logic
                if (tienKhoan > donHang.giaTriKhoan) {
                    donHang.giaTriKhoan = tienKhoan;
                }
                if (tongHeSo > donHang.tongHeSoDiem) {
                    donHang.tongHeSoDiem = tongHeSo;
                }
            }
        });

        const table1 = Array.from(donHangMap.values()).map((item, index) => ({
            stt: index + 1,
            maDonHang: item.maDonHang,
            giaTriKhoan: item.giaTriKhoan,
            tongHeSoDiem: item.tongHeSoDiem
        }));

        // Xử lý dữ liệu cho bảng 2: Tổng hợp tiền khoán được hưởng/đơn hàng
        const table2 = filteredData.map((row, index) => ({
            stt: index + 1,
            maLanThucHien: row[colIndex.maLanThucHien] || '',
            hoTen: row[colIndex.hoTen] || '',
            vaiTro: row[colIndex.vaiTro] || '',
            heSoDiem: row[colIndex.heSoDiem] || '0', // Giữ nguyên chuỗi để xử lý dấu phẩy
            donGiaTB: parseFloat(row[colIndex.donGiaTB] || 0),
            thanhTien: parseFloat(row[colIndex.thanhTien] || 0),
            maDonHang: row[colIndex.maDonHang] || ''
        }));

        // Tính tổng tiền
        const tongTien = table2.reduce((sum, item) => sum + item.thanhTien, 0);
        const tongDonHang = table1.length;

        res.render("phanbolapdat", {
            manv: manv || '',
            thang: thang || '',
            nam: nam || '',
            hoTen: hoTen,
            table1: table1,
            table2: table2,
            tongTien: tongTien,
            tongDonHang: tongDonHang,
            error: null
        });

    } catch (error) {
        console.error("❌ Lỗi khi lấy báo cáo lắp đặt:", error);
        res.render("phanbolapdat", {
            manv: req.params.manv || '',
            thang: req.query.thang || '',
            nam: req.query.nam || '',
            hoTen: '',
            table1: [],
            table2: [],
            tongTien: 0,
            tongDonHang: 0,
            error: "Đã xảy ra lỗi khi lấy dữ liệu: " + error.message
        });
    }
});

// Route xuất Excel báo cáo lắp đặt (đã sửa với điều kiện xác nhận thanh toán)
app.get("/export/lapdat", async (req, res) => {
    try {
        const { manv, thang, nam } = req.query;
        
        // Lấy dữ liệu từ sheet TT_khoan_lap_dat
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: "TT_khoan_lap_dat!A:N",
        });

        const data = response.data.values || [];
        if (data.length === 0) {
            throw new Error("Không có dữ liệu trong sheet");
        }

        // Xác định index cột
        const colIndex = {
            hoTen: 1,        // Cột B
            maNV: 2,         // Cột C
            maDonHang: 3,    // Cột D
            maLanThucHien: 4, // Cột E
            vaiTro: 5,       // Cột F
            heSoDiem: 6,     // Cột G
            tongDiemHeSo: 8, // Cột I
            tienKhoanLD: 9,  // Cột J
            donGiaTB: 10,    // Cột K
            thanhTien: 11,   // Cột L
            ngayBaoCao: 12,  // Cột M
            xacNhanThanhToan: 13 // Cột N
        };

        // Bỏ qua dòng tiêu đề nếu có
        let dataRows = data;
        if (data.length > 0) {
            const firstRow = data[0];
            if (firstRow[colIndex.maNV] === "Mã nhân viên" || 
                firstRow[colIndex.hoTen] === "Họ và tên") {
                dataRows = data.slice(1);
            }
        }

        // Lọc dữ liệu (thêm điều kiện xác nhận thanh toán)
        let filteredData = dataRows.filter(row => {
            const maNV = row[colIndex.maNV] || '';
            const ngayStr = row[colIndex.ngayBaoCao] || '';
            const xacNhanTT = row[colIndex.xacNhanThanhToan] || '';
            
            const matchMaNV = !manv || maNV.toString().trim() === manv.trim();
            
            // Điều kiện xác nhận thanh toán
            const matchXacNhan = xacNhanTT.toString().trim().toLowerCase() === "xác nhận thanh toán";
            
            let matchDate = true;
            if (thang && nam) {
                const ngay = parseGoogleSheetDate(ngayStr);
                if (ngay) {
                    const rowMonth = ngay.getMonth() + 1;
                    const rowYear = ngay.getFullYear();
                    matchDate = rowMonth === parseInt(thang) && rowYear === parseInt(nam);
                } else {
                    matchDate = false;
                }
            }
            
            return matchMaNV && matchDate && matchXacNhan;
        });

        // Lấy tên nhân viên
        let hoTen = '';
        if (filteredData.length > 0) {
            hoTen = filteredData[0][colIndex.hoTen] || '';
        }

        // Tạo workbook Excel
        const workbook = new exceljs.Workbook();
        workbook.creator = 'Hệ thống báo cáo lắp đặt';
        workbook.created = new Date();

        // Sheet 1: Danh sách đơn hàng
        const sheet1 = workbook.addWorksheet('danh_sach_don_hang');
        
        // Tiêu đề sheet 1
        sheet1.mergeCells('A1:D1');
        const title1 = sheet1.getCell('A1');
        title1.value = 'BẢNG TỔNG HỢP CÁC ĐƠN HÀNG CÓ SỰ THAM GIA LẮP ĐẶT';
        title1.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title1.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title1.alignment = { horizontal: 'center', vertical: 'middle' };

        // Thông tin báo cáo
        sheet1.getCell('A2').value = 'Nhân viên:';
        sheet1.getCell('B2').value = hoTen;
        sheet1.getCell('A3').value = 'Mã NV:';
        sheet1.getCell('B3').value = manv;
        sheet1.getCell('A4').value = 'Tháng/Năm:';
        sheet1.getCell('B4').value = `${thang}/${nam}`;

        // Header sheet 1
        const header1 = ['STT', 'Mã đơn hàng', 'Giá trị khoán', 'Tổng hệ số điểm khoán đơn hàng'];
        const headerRow1 = sheet1.getRow(6);
        headerRow1.values = header1;
        headerRow1.eachCell((cell, colNumber) => {
            cell.font = { bold: true, color: { argb: 'FFFFFF' } };
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '5B9BD5' }
            };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
            cell.alignment = { horizontal: 'center', vertical: 'middle' };
        });

        // Tạo bảng 1 từ dữ liệu
        const donHangMap = new Map();
        filteredData.forEach(row => {
            const maDonHang = row[colIndex.maDonHang] || '';
            const tienKhoan = parseFloat(row[colIndex.tienKhoanLD] || 0);
            const tongHeSo = parseFloat(row[colIndex.tongDiemHeSo] || 0);
            
            if (maDonHang) {
                if (!donHangMap.has(maDonHang)) {
                    donHangMap.set(maDonHang, {
                        maDonHang: maDonHang,
                        giaTriKhoan: 0,
                        tongHeSoDiem: 0
                    });
                }
                
                const donHang = donHangMap.get(maDonHang);
                if (tienKhoan > donHang.giaTriKhoan) {
                    donHang.giaTriKhoan = tienKhoan;
                }
                if (tongHeSo > donHang.tongHeSoDiem) {
                    donHang.tongHeSoDiem = tongHeSo;
                }
            }
        });

        // Dữ liệu sheet 1
        let rowIndex = 7;
        let stt = 1;
        donHangMap.forEach((item) => {
            const dataRow = sheet1.getRow(rowIndex);
            dataRow.values = [
                stt,
                item.maDonHang,
                item.giaTriKhoan,
                item.tongHeSoDiem
            ];
            
            // Style cho dòng
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber === 3) { // Cột giá trị khoán
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                } else if (colNumber === 4) { // Cột tổng hệ số điểm
                    cell.numFmt = '#,##0.0'; // 1 chữ số thập phân
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu xen kẽ
                if (rowIndex % 2 === 0) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'F2F2F2' }
                    };
                }
            });
            
            rowIndex++;
            stt++;
        });

        // Tổng sheet 1
        const totalRow1 = sheet1.getRow(rowIndex);
        totalRow1.getCell(1).value = 'Tổng số đơn hàng:';
        totalRow1.getCell(2).value = donHangMap.size;
        totalRow1.getCell(1).font = { bold: true };
        totalRow1.getCell(2).font = { bold: true };

        // Auto fit columns
        sheet1.columns.forEach((column, index) => {
            if (index === 0) column.width = 8; // STT
            else if (index === 1) column.width = 20; // Mã đơn hàng
            else column.width = 25;
        });

        // Sheet 2: Phân bổ khoán
        const sheet2 = workbook.addWorksheet('Phan_bo_khoan');
        
        // Tiêu đề sheet 2
        sheet2.mergeCells('A1:H1');
        const title2 = sheet2.getCell('A1');
        title2.value = 'TỔNG HỢP TIỀN KHOÁN ĐƯỢC HƯỞNG/ĐƠN HÀNG';
        title2.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title2.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title2.alignment = { horizontal: 'center', vertical: 'middle' };

        // Thông tin báo cáo sheet 2
        sheet2.getCell('A2').value = 'Nhân viên:';
        sheet2.getCell('B2').value = hoTen;
        sheet2.getCell('A3').value = 'Mã NV:';
        sheet2.getCell('B3').value = manv;
        sheet2.getCell('A4').value = 'Tháng/Năm:';
        sheet2.getCell('B4').value = `${thang}/${nam}`;

        // Header sheet 2
        const header2 = ['STT', 'Mã đơn hàng', 'Mã lần thực hiện', 'Họ tên nhân sự', 'Vai trò', 'Hệ số điểm', 'Đơn giá trung bình', 'Thành tiền trên lần thực hiện'];
        const headerRow2 = sheet2.getRow(6);
        headerRow2.values = header2;
        headerRow2.eachCell((cell, colNumber) => {
            cell.font = { bold: true, color: { argb: 'FFFFFF' } };
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '5B9BD5' }
            };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
            cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
        });

        // Dữ liệu sheet 2
        rowIndex = 7;
        let tongThanhTien = 0;
        filteredData.forEach((row, index) => {
            const dataRow = sheet2.getRow(rowIndex);
            const thanhTien = parseFloat(row[colIndex.thanhTien] || 0);
            
            // Xử lý hệ số điểm (giữ nguyên định dạng dấu phẩy)
            let heSoDiem = row[colIndex.heSoDiem] || '0';
            if (typeof heSoDiem === 'string' && heSoDiem.includes('.')) {
                heSoDiem = heSoDiem.replace('.', ',');
            }
            
            dataRow.values = [
                index + 1,
                row[colIndex.maDonHang] || '',
                row[colIndex.maLanThucHien] || '',
                row[colIndex.hoTen] || '',
                row[colIndex.vaiTro] || '',
                heSoDiem,
                parseFloat(row[colIndex.donGiaTB] || 0),
                thanhTien
            ];
            
            tongThanhTien += thanhTien;
            
            // Style cho dòng
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber === 6) { // Cột đơn giá trung bình
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                } else if (colNumber === 7) { // Cột hệ số điểm - giữ nguyên chuỗi
                    cell.alignment = { horizontal: 'right' };
                } else if (colNumber === 8) { // Cột thành tiền
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu xen kẽ
                if (rowIndex % 2 === 0) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'F2F2F2' }
                    };
                }
            });
            
            rowIndex++;
        });

        // Tổng sheet 2
        const totalRow2 = sheet2.getRow(rowIndex);
        totalRow2.getCell(7).value = 'Tổng cộng:';
        totalRow2.getCell(8).value = tongThanhTien;
        totalRow2.eachCell((cell, colNumber) => {
            if (colNumber >= 7) {
                cell.font = { bold: true };
                cell.fill = {
                    type: 'pattern',
                    pattern: 'solid',
                    fgColor: { argb: 'C6EFCE' }
                };
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                if (colNumber === 8) {
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
            }
        });

        // Auto fit columns sheet 2
        sheet2.columns.forEach((column, index) => {
            if (index === 0) column.width = 8; // STT
            else if (index === 1) column.width = 15; // Mã đơn hàng
            else if (index === 2) column.width = 20; // Mã lần thực hiện
            else if (index === 3) column.width = 25; // Họ tên
            else if (index === 4) column.width = 15; // Vai trò
            else if (index === 5) column.width = 15; // Hệ số điểm
            else if (index === 6) column.width = 18; // Đơn giá trung bình
            else if (index === 7) column.width = 18; // Thành tiền
        });

        // Thiết lập response header
        const fileName = `BC_khoan_LD_${hoTen || manv || 'unknown'}_${thang || ''}_${nam || ''}.xlsx`;
        
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${encodeURIComponent(fileName)}`
        );

        await workbook.xlsx.write(res);
        res.end();

    } catch (error) {
        console.error("❌ Lỗi khi xuất Excel lắp đặt:", error);
        res.status(500).send("Lỗi khi xuất file Excel: " + error.message);
    }
});


////BÁO CÁO KINH DOANH
// Thêm route mới sau các route khác trong app.js
app.get("/baocaokinhdoanh", async (req, res) => {
    try {
        // Lấy dữ liệu từ các sheet
        const [donHangRes, donHangPVCRes, donHangNKRes] = await Promise.all([
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang!A:BR",
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang_PVC_ct!A:AV",
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang_nk_ct!A:Z",
            })
        ]);

        const donHangData = donHangRes.data.values || [];
        const donHangPVCData = donHangPVCRes.data.values || [];
        const donHangNKData = donHangNKRes.data.values || [];

        // Xử lý các tham số filter
        const currentYear = new Date().getFullYear();
        const {
            filterType = 'thang',
            filterMonth = new Date().getMonth() + 1,
            filterYear = currentYear,
            filterDay,
            page = 1,
            productYear = currentYear,
            employeeMonth = new Date().getMonth() + 1,
            employeeYear = currentYear,
            cancelMonth = new Date().getMonth() + 1,
            cancelYear = currentYear
        } = req.query;

        // Bảng 1: Tổng hợp doanh số theo năm/mảng sản phẩm
        const table1 = await generateTable1(donHangData);
        
        // Bảng 2: Tổng hợp doanh số theo nhân viên kinh doanh
        const table2 = await generateTable2(donHangData, currentYear);
        
        // Bảng 3: Báo cáo doanh số theo đơn chi tiết
        const table3Data = await generateTable3(donHangData, {
            filterType,
            filterMonth: parseInt(filterMonth),
            filterYear: parseInt(filterYear),
            filterDay: filterDay ? parseInt(filterDay) : null,
            page: parseInt(page),
            limit: 15
        });
        
        // Bảng 4: Báo cáo doanh số theo dòng sản phẩm
        const table4 = await generateTable4(donHangPVCData, donHangNKData, parseInt(productYear));
        
        // Bảng 5: Báo cáo doanh số mảng sản phẩm/nhân viên kinh doanh
        const table5 = await generateTable5(donHangPVCData, donHangNKData, 
            parseInt(employeeMonth), parseInt(employeeYear));
        
        // Bảng 6: Báo cáo danh sách hủy đơn hàng
        const table6 = await generateTable6(donHangData, 
            parseInt(cancelMonth), parseInt(cancelYear));

        res.render("baocaokinhdoanh", {
            table1,
            table2,
            table3: table3Data.data,
            table3TotalPages: table3Data.totalPages,
            table3CurrentPage: parseInt(page),
            table4,
            table5,
            table6,
            filterType,
            filterMonth,
            filterYear,
            filterDay,
            productYear,
            employeeMonth,
            employeeYear,
            cancelMonth,
            cancelYear,
            currentYear
        });
    } catch (error) {
        console.error("❌ Lỗi khi lấy báo cáo kinh doanh:", error);
        res.status(500).send("Lỗi server khi xử lý báo cáo");
    }
});

// Route xuất Excel
app.get("/export/baocaokinhdoanh", async (req, res) => {
    try {
        const workbook = await generateExcelReport();
        
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=bao-cao-kinh-doanh-${new Date().toISOString().split('T')[0]}.xlsx`
        );

        await workbook.xlsx.write(res);
        res.end();
    } catch (error) {
        console.error("❌ Lỗi khi xuất Excel:", error);
        res.status(500).send("Lỗi khi xuất file Excel");
    }
});

// Hàm hỗ trợ chuyển đổi ngày
function parseGoogleSheetDate(dateStr) {
    if (!dateStr) return null;
    // Chuyển đổi từ dd/mm/yyyy hoặc dd/mm/yyyy hh:mm:ss
    const parts = dateStr.split(/[/ :]/);
    if (parts.length >= 3) {
        const day = parseInt(parts[0]);
        const month = parseInt(parts[1]) - 1;
        const year = parseInt(parts[2]);
        return new Date(year, month, day);
    }
    return null;
}


// Hàm generate Table 1
async function generateTable1(donHangData) {
    const currentYear = new Date().getFullYear();
    const years = [currentYear - 2, currentYear - 1, currentYear];
    
    const months = [
        { label: "Tháng 1", month: 1, isMonth: true },
        { label: "Tháng 2", month: 2, isMonth: true },
        { label: "Tháng 3", month: 3, isMonth: true },
        { label: "Quý 1", quarter: 1, isQuarter: true },
        { label: "Tháng 4", month: 4, isMonth: true },
        { label: "Tháng 5", month: 5, isMonth: true },
        { label: "Tháng 6", month: 6, isMonth: true },
        { label: "Quý 2", quarter: 2, isQuarter: true },
        { label: "Tháng 7", month: 7, isMonth: true },
        { label: "Tháng 8", month: 8, isMonth: true },
        { label: "Tháng 9", month: 9, isMonth: true },
        { label: "Quý 3", quarter: 3, isQuarter: true },
        { label: "Tháng 10", month: 10, isMonth: true },
        { label: "Tháng 11", month: 11, isMonth: true },
        { label: "Tháng 12", month: 12, isMonth: true },
        { label: "Quý 4", quarter: 4, isQuarter: true },
        { label: "Tổng năm", isTotal: true }
    ];

    // Khởi tạo kết quả
    const result = months.map(month => ({
        label: month.label,
        isMonth: month.isMonth || false,
        isQuarter: month.isQuarter || false,
        isTotal: month.isTotal || false,
        month: month.month,
        quarter: month.quarter,
        years: years.map(year => ({
            year,
            nhua: 0,
            nhom: 0,
            tong: 0
        }))
    }));

    // Xử lý dữ liệu
    for (let i = 1; i < donHangData.length; i++) {
        const row = donHangData[i];
        const ngayStr = row[41]; // Cột AP (index 42)
        const doanhSo = parseFloat(row[69] || 0); // Cột BR (index 69)
        const nhomSP = row[26]; // Cột AA (index 26)
        const tinhTrang = row[38]; // Cột AM (index 38)

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay) continue;

        const year = ngay.getFullYear();
        const month = ngay.getMonth() + 1;
        const quarter = Math.ceil(month / 3);

        const yearIndex = years.indexOf(year);
        if (yearIndex === -1) continue;

        // Phân loại nhóm sản phẩm
        const isNhom = (nhomSP === "NK" || nhomSP === "PKNK");
        const isNhua = !isNhom;

        // Cập nhật theo tháng
        const monthIndex = result.findIndex(r => r.month === month && r.isMonth);
        if (monthIndex !== -1) {
            if (isNhua) result[monthIndex].years[yearIndex].nhua += doanhSo;
            if (isNhom) result[monthIndex].years[yearIndex].nhom += doanhSo;
            result[monthIndex].years[yearIndex].tong += doanhSo;
        }

        // Cập nhật theo quý
        const quarterIndex = result.findIndex(r => r.quarter === quarter && r.isQuarter);
        if (quarterIndex !== -1) {
            if (isNhua) result[quarterIndex].years[yearIndex].nhua += doanhSo;
            if (isNhom) result[quarterIndex].years[yearIndex].nhom += doanhSo;
            result[quarterIndex].years[yearIndex].tong += doanhSo;
        }

        // Cập nhật tổng năm
        const totalIndex = result.findIndex(r => r.isTotal);
        if (totalIndex !== -1) {
            if (isNhua) result[totalIndex].years[yearIndex].nhua += doanhSo;
            if (isNhom) result[totalIndex].years[yearIndex].nhom += doanhSo;
            result[totalIndex].years[yearIndex].tong += doanhSo;
        }
    }

    return result;
}

// Hàm generate Table 2
async function generateTable2(donHangData, currentYear) {
    // Lấy danh sách nhân viên
    const employees = new Set();
    for (let i = 1; i < donHangData.length; i++) {
        const row = donHangData[i];
        const nguoiTao = row[2]; // Cột C
        const ngayStr = row[41]; // AP
        const ngay = parseGoogleSheetDate(ngayStr);
        if (ngay && ngay.getFullYear() === currentYear && nguoiTao) {
            employees.add(nguoiTao);
        }
    }
    const employeeList = Array.from(employees).sort();

    const months = [
        { label: "Tháng 1", month: 1, isMonth: true },
        { label: "Tháng 2", month: 2, isMonth: true },
        { label: "Tháng 3", month: 3, isMonth: true },
        { label: "Quý 1", quarter: 1, isQuarter: true },
        { label: "Tháng 4", month: 4, isMonth: true },
        { label: "Tháng 5", month: 5, isMonth: true },
        { label: "Tháng 6", month: 6, isMonth: true },
        { label: "Quý 2", quarter: 2, isQuarter: true },
        { label: "Tháng 7", month: 7, isMonth: true },
        { label: "Tháng 8", month: 8, isMonth: true },
        { label: "Tháng 9", month: 9, isMonth: true },
        { label: "Quý 3", quarter: 3, isQuarter: true },
        { label: "Tháng 10", month: 10, isMonth: true },
        { label: "Tháng 11", month: 11, isMonth: true },
        { label: "Tháng 12", month: 12, isMonth: true },
        { label: "Quý 4", quarter: 4, isQuarter: true },
        { label: "Tổng năm", isTotal: true }
    ];

    // Khởi tạo kết quả
    const result = months.map(month => ({
        label: month.label,
        isMonth: month.isMonth || false,
        isQuarter: month.isQuarter || false,
        isTotal: month.isTotal || false,
        month: month.month,
        quarter: month.quarter,
        employees: employeeList.map(emp => ({
            name: emp,
            doanhSo: 0
        })),
        tongQuy: 0
    }));

    // Xử lý dữ liệu
    for (let i = 1; i < donHangData.length; i++) {
        const row = donHangData[i];
        const nguoiTao = row[2];
        const ngayStr = row[41];
        const doanhSo = parseFloat(row[69] || 0);
        const tinhTrang = row[38];

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;
        if (!nguoiTao) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay || ngay.getFullYear() !== currentYear) continue;

        const month = ngay.getMonth() + 1;
        const quarter = Math.ceil(month / 3);
        const empIndex = employeeList.indexOf(nguoiTao);

        if (empIndex === -1) continue;

        // Cập nhật theo tháng
        const monthIndex = result.findIndex(r => r.month === month && r.isMonth);
        if (monthIndex !== -1) {
            result[monthIndex].employees[empIndex].doanhSo += doanhSo;
            result[monthIndex].tongQuy += doanhSo;
        }

        // Cập nhật theo quý
        const quarterIndex = result.findIndex(r => r.quarter === quarter && r.isQuarter);
        if (quarterIndex !== -1) {
            result[quarterIndex].employees[empIndex].doanhSo += doanhSo;
            result[quarterIndex].tongQuy += doanhSo;
        }

        // Cập nhật tổng năm
        const totalIndex = result.findIndex(r => r.isTotal);
        if (totalIndex !== -1) {
            result[totalIndex].employees[empIndex].doanhSo += doanhSo;
            result[totalIndex].tongQuy += doanhSo;
        }
    }

    return {
        employeeList,
        data: result
    };
}

// Hàm generate Table 3
async function generateTable3(donHangData, options) {
    const { filterType, filterMonth, filterYear, filterDay, page, limit } = options;
    const startIndex = (page - 1) * limit;
    
    let filteredData = [];

    for (let i = 1; i < donHangData.length; i++) {
        const row = donHangData[i];
        const ngayStr = row[41];
        const tinhTrang = row[38];
        const doanhSo = parseFloat(row[69] || 0);

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay) continue;

        // Lọc theo điều kiện
        let match = false;
        switch (filterType) {
            case 'ngay':
                if (ngay.getDate() === filterDay && 
                    ngay.getMonth() + 1 === filterMonth && 
                    ngay.getFullYear() === filterYear) {
                    match = true;
                }
                break;
            case 'thang':
                if (ngay.getMonth() + 1 === filterMonth && 
                    ngay.getFullYear() === filterYear) {
                    match = true;
                }
                break;
            case 'nam':
                if (ngay.getFullYear() === filterYear) {
                    match = true;
                }
                break;
        }

        if (match) {
            filteredData.push({
                stt: 0,
                ngayDuyet: `${ngay.getDate().toString().padStart(2, '0')}/${(ngay.getMonth() + 1).toString().padStart(2, '0')}/${ngay.getFullYear()}`,
                maDonHang: row[6] || '', // G
                loaiKhach: row[8] || '', // I
                khachHangID: row[7] || '', // H
                tenKhachHang: row[9] || '', // J
                diaChi: row[11] || '', // L
                tinh: row[12] || '', // M
                nguoiLienHe: row[17] || '', // R
                soDienThoai: row[18] || '', // S
                nhomSanPham: row[26] || '', // AA
                loaiDonHang: row[28] || '', // AC
                nhomSanXuat: row[27] || '', // AB
                doanhSo: doanhSo,
                kinhDoanh: row[2] || '' // C
            });
        }
    }

    // Sắp xếp theo ngày (mới nhất trước)
    filteredData.sort((a, b) => {
        const dateA = parseGoogleSheetDate(a.ngayDuyet);
        const dateB = parseGoogleSheetDate(b.ngayDuyet);
        return dateB - dateA;
    });

    // Phân trang
    const total = filteredData.length;
    const totalPages = Math.ceil(total / limit);
    const paginatedData = filteredData.slice(startIndex, startIndex + limit);

    // Cập nhật STT
    paginatedData.forEach((item, index) => {
        item.stt = startIndex + index + 1;
    });

    return {
        data: paginatedData,
        total,
        totalPages,
        currentPage: page
    };
}

// Hàm generate Table 4
async function generateTable4(pvcData, nkData, year) {
    const productGroups = [
        "PVC tiêu chuẩn",
        "PVC khổ lớn", 
        "PVC ESD",
        "Vật tư phụ PVC",
        "HDOOR",
        "Quạt - Đèn",
        "BẠT PVC",
        "Silicon",
        "VREMTD - VREMTO",
        "Vật tư phụ",
        "Khác",
        "Nhân công",
        "Vận chuyển",
        "Mảng nhôm kính"
    ];

    const pvcMapping = {
        "PVCTCGC": "PVC tiêu chuẩn",
        "PVCTCTM": "PVC tiêu chuẩn",
        "PVCKLGC": "PVC khổ lớn",
        "PVCKLTM": "PVC khổ lớn",
        "PVCCTĐ (ESD)": "PVC ESD",
        "Vật tư phụ PVC": "Vật tư phụ PVC",
        "HDOOR": "HDOOR",
        "PKHDOOR": "HDOOR",
        "Quạt": "Quạt - Đèn",
        "Đèn": "Quạt - Đèn",
        "PKDEN": "Quạt - Đèn",
        "PKQUAT": "Quạt - Đèn",
        "BPVCKL": "BẠT PVC",
        "SLCGC": "Silicon",
        "VREMTD": "VREMTD - VREMTO",
        "VREMTO": "VREMTD - VREMTO",
        "Vật tư phụ": "Vật tư phụ",
        "KHAC": "Khác",
        "Nhân công": "Nhân công",
        "Vận chuyển": "Vận chuyển"
    };

    const months = [
        { label: "Tháng 1", month: 1, isMonth: true },
        { label: "Tháng 2", month: 2, isMonth: true },
        { label: "Tháng 3", month: 3, isMonth: true },
        { label: "Quý 1", quarter: 1, isQuarter: true },
        { label: "Tháng 4", month: 4, isMonth: true },
        { label: "Tháng 5", month: 5, isMonth: true },
        { label: "Tháng 6", month: 6, isMonth: true },
        { label: "Quý 2", quarter: 2, isQuarter: true },
        { label: "Tháng 7", month: 7, isMonth: true },
        { label: "Tháng 8", month: 8, isMonth: true },
        { label: "Tháng 9", month: 9, isMonth: true },
        { label: "Quý 3", quarter: 3, isQuarter: true },
        { label: "Tháng 10", month: 10, isMonth: true },
        { label: "Tháng 11", month: 11, isMonth: true },
        { label: "Tháng 12", month: 12, isMonth: true },
        { label: "Quý 4", quarter: 4, isQuarter: true },
        { label: "Tổng năm", isTotal: true }
    ];

    // Khởi tạo kết quả
    const result = months.map(month => ({
        label: month.label,
        isMonth: month.isMonth || false,
        isQuarter: month.isQuarter || false,
        isTotal: month.isTotal || false,
        month: month.month,
        quarter: month.quarter,
        products: productGroups.map(group => ({
            name: group,
            doanhSo: 0
        })),
        tongThang: 0
    }));

    // Xử lý dữ liệu PVC
    for (let i = 1; i < pvcData.length; i++) {
        const row = pvcData[i];
        const ngayStr = row[32]; // AG (index 32)
        const tinhTrang = row[33]; // AH (index 33)
        const doanhSo = parseFloat(row[47] || 0); // AV (index 47)
        const nhomSP = row[5]; // F (index 5)

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay || ngay.getFullYear() !== year) continue;

        const month = ngay.getMonth() + 1;
        const quarter = Math.ceil(month / 3);
        const productName = pvcMapping[nhomSP] || "Khác";
        const productIndex = productGroups.indexOf(productName);

        if (productIndex === -1) continue;

        // Cập nhật theo tháng
        const monthIndex = result.findIndex(r => r.month === month && r.isMonth);
        if (monthIndex !== -1) {
            result[monthIndex].products[productIndex].doanhSo += doanhSo;
            result[monthIndex].tongThang += doanhSo;
        }

        // Cập nhật theo quý
        const quarterIndex = result.findIndex(r => r.quarter === quarter && r.isQuarter);
        if (quarterIndex !== -1) {
            result[quarterIndex].products[productIndex].doanhSo += doanhSo;
            result[quarterIndex].tongThang += doanhSo;
        }

        // Cập nhật tổng năm
        const totalIndex = result.findIndex(r => r.isTotal);
        if (totalIndex !== -1) {
            result[totalIndex].products[productIndex].doanhSo += doanhSo;
            result[totalIndex].tongThang += doanhSo;
        }
    }

    // Xử lý dữ liệu Nhôm kính
    for (let i = 1; i < nkData.length; i++) {
        const row = nkData[i];
        const ngayStr = row[24]; // Y (index 24)
        const tinhTrang = row[25]; // Z (index 25)
        const doanhSo = parseFloat(row[19] || 0); // T (index 19)

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay || ngay.getFullYear() !== year) continue;

        const month = ngay.getMonth() + 1;
        const quarter = Math.ceil(month / 3);
        const productIndex = productGroups.indexOf("Mảng nhôm kính");

        // Cập nhật theo tháng
        const monthIndex = result.findIndex(r => r.month === month && r.isMonth);
        if (monthIndex !== -1) {
            result[monthIndex].products[productIndex].doanhSo += doanhSo;
            result[monthIndex].tongThang += doanhSo;
        }

        // Cập nhật theo quý
        const quarterIndex = result.findIndex(r => r.quarter === quarter && r.isQuarter);
        if (quarterIndex !== -1) {
            result[quarterIndex].products[productIndex].doanhSo += doanhSo;
            result[quarterIndex].tongThang += doanhSo;
        }

        // Cập nhật tổng năm
        const totalIndex = result.findIndex(r => r.isTotal);
        if (totalIndex !== -1) {
            result[totalIndex].products[productIndex].doanhSo += doanhSo;
            result[totalIndex].tongThang += doanhSo;
        }
    }

    return {
        productGroups,
        data: result,
        year
    };
}

// Hàm generate Table 5
// Hàm generate Table 5: Báo cáo doanh số mảng sản phẩm/nhân viên kinh doanh
async function generateTable5(pvcData, nkData, month, year) {
    const productGroups = [
        "PVC tiêu chuẩn",
        "PVC khổ lớn", 
        "PVC ESD",
        "Vật tư phụ PVC",
        "HDOOR",
        "Quạt - Đèn",
        "BẠT PVC",
        "Silicon",
        "VREMTD - VREMTO",
        "Vật tư phụ",
        "Khác",
        "Nhân công",
        "Vận chuyển",
        "Mảng nhôm kính"
    ];

    const pvcMapping = {
        "PVCTCGC": "PVC tiêu chuẩn",
        "PVCTCTM": "PVC tiêu chuẩn",
        "PVCKLGC": "PVC khổ lớn",
        "PVCKLTM": "PVC khổ lớn",
        "PVCCTĐ (ESD)": "PVC ESD",
        "Vật tư phụ PVC": "Vật tư phụ PVC",
        "HDOOR": "HDOOR",
        "PKHDOOR": "HDOOR",
        "Quạt": "Quạt - Đèn",
        "Đèn": "Quạt - Đèn",
        "PKDEN": "Quạt - Đèn",
        "PKQUAT": "Quạt - Đèn",
        "BPVCKL": "BẠT PVC",
        "SLCGC": "Silicon",
        "VREMTD": "VREMTD - VREMTO",
        "VREMTO": "VREMTD - VREMTO",
        "Vật tư phụ": "Vật tư phụ",
        "KHAC": "Khác",
        "Nhân công": "Nhân công",
        "Vận chuyển": "Vận chuyển"
    };

    // Lấy danh sách nhân viên từ cả 2 sheet
    const employeesSet = new Set();

    // Từ PVC
    for (let i = 1; i < pvcData.length; i++) {
        const row = pvcData[i];
        const nguoiTao = row[31]; // AF (index 31)
        const ngayStr = row[32]; // AG
        const ngay = parseGoogleSheetDate(ngayStr);
        if (ngay && ngay.getFullYear() === year && nguoiTao) {
            if (month === 0 || ngay.getMonth() + 1 === month) {
                employeesSet.add(nguoiTao);
            }
        }
    }

    // Từ NK
    for (let i = 1; i < nkData.length; i++) {
        const row = nkData[i];
        const nguoiTao = row[22]; // W (index 22)
        const ngayStr = row[24]; // Y
        const ngay = parseGoogleSheetDate(ngayStr);
        if (ngay && ngay.getFullYear() === year && nguoiTao) {
            if (month === 0 || ngay.getMonth() + 1 === month) {
                employeesSet.add(nguoiTao);
            }
        }
    }

    const employees = Array.from(employeesSet).sort();

    // Xác định periods (tháng)
    let periods = [];
    if (month === 0) {
        // Lấy cả năm
        for (let m = 1; m <= 12; m++) {
            periods.push({ 
                label: `Tháng ${m}`, 
                month: m, 
                year: year,
                data: []
            });
        }
    } else {
        // Lấy theo tháng cụ thể
        periods.push({ 
            label: `Tháng ${month}/${year}`, 
            month: month, 
            year: year,
            data: []
        });
    }

    // Khởi tạo cấu trúc dữ liệu
    periods.forEach(period => {
        period.data = employees.map(emp => ({
            employee: emp,
            products: productGroups.reduce((acc, group) => {
                acc[group] = 0;
                return acc;
            }, {}),
            tongNhua: 0,
            tongNhom: 0,
            tong: 0
        }));
    });

    // Xử lý dữ liệu PVC
    for (let i = 1; i < pvcData.length; i++) {
        const row = pvcData[i];
        const ngayStr = row[32]; // AG
        const tinhTrang = row[33]; // AH
        const doanhSo = parseFloat(row[47] || 0); // AV
        const nhomSP = row[5]; // F
        const nguoiTao = row[31]; // AF

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;
        if (!nguoiTao) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay || ngay.getFullYear() !== year) continue;

        const currentMonth = ngay.getMonth() + 1;
        
        // Tìm period phù hợp
        const period = periods.find(p => 
            (month === 0 && p.month === currentMonth) || 
            (month !== 0 && p.month === month)
        );
        
        if (!period) continue;

        // Tìm employee trong period
        const employeeData = period.data.find(d => d.employee === nguoiTao);
        if (!employeeData) continue;

        // Ánh xạ nhóm sản phẩm
        const productName = pvcMapping[nhomSP] || "Khác";
        
        // Cập nhật doanh số cho nhóm sản phẩm
        if (employeeData.products.hasOwnProperty(productName)) {
            employeeData.products[productName] += doanhSo;
        }
        
        // Cập nhật tổng nhựa (bỏ đi vận chuyển)
        if (productName !== "Vận chuyển") {
            employeeData.tongNhua += doanhSo;
        }
        
        employeeData.tong += doanhSo;
    }

    // Xử lý dữ liệu Nhôm kính
    for (let i = 1; i < nkData.length; i++) {
        const row = nkData[i];
        const ngayStr = row[24]; // Y
        const tinhTrang = row[25]; // Z
        const doanhSo = parseFloat(row[19] || 0); // T
        const nguoiTao = row[22]; // W

        if (tinhTrang !== "Đơn hàng") continue;
        if (isNaN(doanhSo) || doanhSo <= 0) continue;
        if (!nguoiTao) continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        if (!ngay || ngay.getFullYear() !== year) continue;

        const currentMonth = ngay.getMonth() + 1;
        
        // Tìm period phù hợp
        const period = periods.find(p => 
            (month === 0 && p.month === currentMonth) || 
            (month !== 0 && p.month === month)
        );
        
        if (!period) continue;

        // Tìm employee trong period
        const employeeData = period.data.find(d => d.employee === nguoiTao);
        if (!employeeData) continue;

        // Cập nhật doanh số cho Mảng nhôm kính
        employeeData.products["Mảng nhôm kính"] += doanhSo;
        employeeData.tongNhom += doanhSo;
        employeeData.tong += doanhSo;
    }

    return {
        productGroups,
        employees,
        periods,
        month,
        year
    };
}

// Hàm generate Table 6
async function generateTable6(donHangData, month, year) {
    const filteredData = [];

    for (let i = 1; i < donHangData.length; i++) {
        const row = donHangData[i];
        const ngayStr = row[41]; // AP
        const tinhTrang = row[35]; // AJ
        const ngayHuyStr = row[47]; // AV

        if (tinhTrang !== "Hủy đơn") continue;

        const ngay = parseGoogleSheetDate(ngayStr);
        const ngayHuy = parseGoogleSheetDate(ngayHuyStr);
        
        if (!ngay) continue;
        
        // Lọc theo tháng/năm
        if (month && year) {
            if (ngay.getMonth() + 1 !== month || ngay.getFullYear() !== year) continue;
        } else if (year) {
            if (ngay.getFullYear() !== year) continue;
        }

        filteredData.push({
            stt: filteredData.length + 1,
            khachHangID: row[7] || '', // H
            tenDayDu: row[9] || '', // J
            nguoiLienHe: row[17] || '', // R
            dienThoai: row[18] || '', // S
            diaChiThucHien: row[20] || '', // U
            maDonHang: row[6] || '', // G
            hetHang: '',
            giaTriHuy: parseFloat(row[69] || 0), // BR
            lyDoHuy: row[57] || '', // BF
            ngayThangHuy: ngayHuy ? 
                `${ngayHuy.getDate().toString().padStart(2, '0')}/${(ngayHuy.getMonth() + 1).toString().padStart(2, '0')}/${ngayHuy.getFullYear()}` : '',
            khoiLuongSPHuy: row[31] || '', // AF
            ghiChu: ''
        });
    }

    return filteredData;
}

// Hàm tạo Excel report cho báo cáo kinh doanh
async function generateExcelReport() {
    try {
        // Lấy dữ liệu từ Google Sheets
        const [donHangRes, donHangPVCRes, donHangNKRes] = await Promise.all([
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang!A:BR",
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang_PVC_ct!A:AV",
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang_nk_ct!A:Z",
            })
        ]);

        const donHangData = donHangRes.data.values || [];
        const donHangPVCData = donHangPVCRes.data.values || [];
        const donHangNKData = donHangNKRes.data.values || [];

        const currentYear = new Date().getFullYear();
        const currentMonth = new Date().getMonth() + 1;

        // Tạo các bảng dữ liệu
        const table1 = await generateTable1(donHangData);
        const table2 = await generateTable2(donHangData, currentYear);
        const table3Data = await generateTable3(donHangData, {
            filterType: 'thang',
            filterMonth: currentMonth,
            filterYear: currentYear,
            filterDay: null,
            page: 1,
            limit: 10000
        });
        const table4 = await generateTable4(donHangPVCData, donHangNKData, currentYear);
        const table5 = await generateTable5(donHangPVCData, donHangNKData, 0, currentYear);
        const table6 = await generateTable6(donHangData, currentMonth, currentYear);

        // Tạo workbook
        const workbook = new exceljs.Workbook();
        workbook.creator = 'Hệ thống báo cáo kinh doanh';
        workbook.created = new Date();

        // ============ SHEET TỔNG HỢP (tạo đầu tiên) ============
        const summarySheet = workbook.addWorksheet('Tổng hợp');
        
        summarySheet.mergeCells('A1:E1');
        const summaryTitle = summarySheet.getCell('A1');
        summaryTitle.value = 'TỔNG HỢP BÁO CÁO KINH DOANH';
        summaryTitle.font = { bold: true, size: 16, color: { argb: 'FFFFFF' } };
        summaryTitle.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        summaryTitle.alignment = { horizontal: 'center', vertical: 'middle' };

        // Thông tin xuất báo cáo
        summarySheet.getCell('A3').value = 'Thông tin báo cáo:';
        summarySheet.getCell('A3').font = { bold: true };
        
        summarySheet.getCell('A4').value = 'Ngày xuất báo cáo:';
        summarySheet.getCell('B4').value = new Date().toLocaleDateString('vi-VN');
        
        summarySheet.getCell('A5').value = 'Năm báo cáo:';
        summarySheet.getCell('B5').value = currentYear;
        
        summarySheet.getCell('A7').value = 'Danh sách các báo cáo:';
        summarySheet.getCell('A7').font = { bold: true };
        
        // Danh sách các sheet sẽ tạo (chuẩn bị trước)
        const sheetNames = [
            'Tổng hợp doanh số năm-mảng SP',
            'Doanh số theo nhân viên',
            'Báo cáo đơn chi tiết',
            'Doanh số theo dòng SP',
            'Doanh số SP theo nhân viên',
            'Danh sách hủy đơn hàng'
        ];
        
        let summaryRow = 8;
        sheetNames.forEach((name, index) => {
            summarySheet.getCell(`A${summaryRow}`).value = `${index + 1}. ${name}`;
            summaryRow++;
        });

        // Auto fit summary
        summarySheet.columns.forEach(column => {
            column.width = 25;
        });

        // ============ SHEET 1: TỔNG HỢP DOANH SỐ THEO NĂM/MẢNG SẢN PHẨM ============
        const sheet1 = workbook.addWorksheet('Tổng hợp doanh số năm-mảng SP');
        
        // Tiêu đề
        sheet1.mergeCells('A1:J1');
        const title1 = sheet1.getCell('A1');
        title1.value = 'BẢNG TỔNG HỢP DOANH SỐ THEO NĂM/MẢNG SẢN PHẨM';
        title1.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title1.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title1.alignment = { horizontal: 'center', vertical: 'middle' };

        // Header
        sheet1.mergeCells('A2:A3');
        sheet1.getCell('A2').value = 'Quý/Tháng';
        sheet1.getCell('A2').alignment = { horizontal: 'center', vertical: 'middle' };
        
        // Tạo header cho các năm
        const years = [currentYear - 2, currentYear - 1, currentYear];
        let colIndex = 1;
        
        years.forEach((year, yearIdx) => {
            const startCol = colIndex;
            const endCol = colIndex + 2;
            const startCell = sheet1.getCell(2, startCol + 1);
            const endCell = sheet1.getCell(2, endCol + 1);
            
            sheet1.mergeCells(2, startCol + 1, 2, endCol + 1);
            startCell.value = `Năm ${year}`;
            startCell.alignment = { horizontal: 'center', vertical: 'middle' };
            
            // Sub headers
            sheet1.getCell(3, startCol + 1).value = 'Mảng Nhựa';
            sheet1.getCell(3, startCol + 2).value = 'Mảng Nhôm';
            sheet1.getCell(3, startCol + 3).value = 'Tổng';
            
            colIndex += 3;
        });

        // Style cho header
        for (let row = 2; row <= 3; row++) {
            for (let col = 1; col <= 10; col++) {
                const cell = sheet1.getCell(row, col);
                cell.font = { bold: true, color: { argb: 'FFFFFF' } };
                cell.fill = {
                    type: 'pattern',
                    pattern: 'solid',
                    fgColor: { argb: '5B9BD5' }
                };
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                cell.alignment = { horizontal: 'center', vertical: 'middle' };
            }
        }

        // Dữ liệu
        let rowIndex = 4;
        table1.forEach((row, idx) => {
            const dataRow = sheet1.getRow(rowIndex);
            
            dataRow.getCell(1).value = row.label;
            
            // Dữ liệu cho từng năm
            let cellIndex = 2;
            row.years.forEach(yearData => {
                dataRow.getCell(cellIndex).value = yearData.nhua;
                dataRow.getCell(cellIndex + 1).value = yearData.nhom;
                dataRow.getCell(cellIndex + 2).value = yearData.tong;
                cellIndex += 3;
            });

            // Style cho dòng
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber > 1) {
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu cho dòng quý và tổng
                if (row.isQuarter) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'DDEBF7' }
                    };
                    cell.font = { bold: true };
                } else if (row.isTotal) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'C6EFCE' }
                    };
                    cell.font = { bold: true };
                }
            });
            
            rowIndex++;
        });

        // Auto fit columns
        sheet1.columns.forEach(column => {
            column.width = 15;
        });
        sheet1.getColumn(1).width = 20;

        // ============ SHEET 2: DOANH SỐ THEO NHÂN VIÊN ============
        const sheet2 = workbook.addWorksheet('Doanh số theo nhân viên');
        
        // Tiêu đề
        const lastCol2 = String.fromCharCode(65 + table2.employeeList.length);
        sheet2.mergeCells(`A1:${lastCol2}1`);
        const title2 = sheet2.getCell('A1');
        title2.value = `BẢNG TỔNG HỢP DOANH SỐ THEO NHÂN VIÊN KINH DOANH NĂM ${currentYear}`;
        title2.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title2.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title2.alignment = { horizontal: 'center', vertical: 'middle' };

        // Header
        const headerRow2 = sheet2.getRow(2);
        headerRow2.values = ['Quý/Tháng', ...table2.employeeList, 'Tổng Quý/Tháng'];
        headerRow2.eachCell((cell, colNumber) => {
            cell.font = { bold: true, color: { argb: 'FFFFFF' } };
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '5B9BD5' }
            };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
            cell.alignment = { horizontal: 'center', vertical: 'middle' };
        });

        // Dữ liệu
        rowIndex = 3;
        table2.data.forEach(row => {
            const dataRow = sheet2.getRow(rowIndex);
            const rowData = [row.label];
            
            row.employees.forEach(emp => {
                rowData.push(emp.doanhSo);
            });
            rowData.push(row.tongQuy);
            
            dataRow.values = rowData;
            
            // Style
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber > 1) {
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu cho dòng quý và tổng
                if (row.isQuarter) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'DDEBF7' }
                    };
                    cell.font = { bold: true };
                } else if (row.isTotal) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'C6EFCE' }
                    };
                    cell.font = { bold: true };
                }
            });
            
            rowIndex++;
        });

        // Auto fit
        sheet2.columns.forEach(column => {
            column.width = 15;
        });
        sheet2.getColumn(1).width = 20;

        // ============ SHEET 3: BÁO CÁO ĐƠN CHI TIẾT ============
        const sheet3 = workbook.addWorksheet('Báo cáo đơn chi tiết');
        
        // Tiêu đề
        sheet3.mergeCells('A1:O1');
        const title3 = sheet3.getCell('A1');
        title3.value = 'BÁO CÁO DOANH SỐ THEO ĐƠN CHI TIẾT';
        title3.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title3.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title3.alignment = { horizontal: 'center', vertical: 'middle' };

        // Header
        const header3 = [
            'STT', 'Ngày duyệt đơn', 'Mã đơn hàng', 'Loại khách', 'Khách hàng ID',
            'Tên công ty/Khách hàng', 'Địa chỉ trụ sở', 'Tỉnh', 'Tên Người liên hệ',
            'Số điện thoại', 'Nhóm sản phẩm', 'Loại đơn hàng', 'Nhóm sản xuất',
            'Doanh số thực lĩnh', 'Kinh doanh'
        ];
        
        const headerRow3 = sheet3.getRow(2);
        headerRow3.values = header3;
        headerRow3.eachCell((cell, colNumber) => {
            cell.font = { bold: true, color: { argb: 'FFFFFF' } };
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '5B9BD5' }
            };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
            cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
        });

        // Dữ liệu
        rowIndex = 3;
        table3Data.data.forEach((item, idx) => {
            const dataRow = sheet3.getRow(rowIndex);
            const rowData = [
                idx + 1,
                item.ngayDuyet,
                item.maDonHang,
                item.loaiKhach,
                item.khachHangID,
                item.tenKhachHang,
                item.diaChi,
                item.tinh,
                item.nguoiLienHe,
                item.soDienThoai,
                item.nhomSanPham,
                item.loaiDonHang,
                item.nhomSanXuat,
                item.doanhSo,
                item.kinhDoanh
            ];
            
            dataRow.values = rowData;
            
            // Style
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber === 14) { // Doanh số
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu xen kẽ
                if (rowIndex % 2 === 0) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'F2F2F2' }
                    };
                }
            });
            
            rowIndex++;
        });

        // Auto fit
        sheet3.columns.forEach((column, index) => {
            if (index === 0) column.width = 8; // STT
            else if (index === 5) column.width = 25; // Tên công ty
            else if (index === 6) column.width = 30; // Địa chỉ
            else column.width = 15;
        });

        // ============ SHEET 4: DOANH SỐ THEO DÒNG SP ============
        const sheet4 = workbook.addWorksheet('Doanh số theo dòng SP');
        
        // Tiêu đề
        const lastCol4 = String.fromCharCode(65 + table4.productGroups.length);
        sheet4.mergeCells(`A1:${lastCol4}1`);
        const title4 = sheet4.getCell('A1');
        title4.value = `BÁO CÁO DOANH SỐ THEO DÒNG SẢN PHẨM NĂM ${currentYear}`;
        title4.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title4.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title4.alignment = { horizontal: 'center', vertical: 'middle' };

        // Header
        const header4 = ['Quý/Tháng', ...table4.productGroups, 'Tổng tháng/quý'];
        const headerRow4 = sheet4.getRow(2);
        headerRow4.values = header4;
        headerRow4.eachCell((cell, colNumber) => {
            cell.font = { bold: true, color: { argb: 'FFFFFF' } };
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '5B9BD5' }
            };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
            cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
        });

        // Dữ liệu
        rowIndex = 3;
        table4.data.forEach(row => {
            const dataRow = sheet4.getRow(rowIndex);
            const rowData = [row.label];
            
            row.products.forEach(product => {
                rowData.push(product.doanhSo);
            });
            rowData.push(row.tongThang);
            
            dataRow.values = rowData;
            
            // Style
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber > 1) {
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu cho dòng quý và tổng
                if (row.isQuarter) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'DDEBF7' }
                    };
                    cell.font = { bold: true };
                } else if (row.isTotal) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'C6EFCE' }
                    };
                    cell.font = { bold: true };
                }
            });
            
            rowIndex++;
        });

        // Auto fit
        sheet4.columns.forEach(column => {
            column.width = 15;
        });
        sheet4.getColumn(1).width = 20;

        // ============ SHEET 5: DOANH SỐ SP THEO NHÂN VIÊN ============
        const sheet5 = workbook.addWorksheet('Doanh số SP theo nhân viên');
        
        // Tiêu đề
        const lastCol5 = String.fromCharCode(65 + table5.productGroups.length + 3);
        sheet5.mergeCells(`A1:${lastCol5}1`);
        const title5 = sheet5.getCell('A1');
        title5.value = `BÁO CÁO DOANH SỐ MẢNG SẢN PHẨM/NHÂN VIÊN KINH DOANH NĂM ${currentYear}`;
        title5.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title5.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title5.alignment = { horizontal: 'center', vertical: 'middle' };

        // Dữ liệu - Tạo theo từng tháng
        let currentRow = 2;
        
        table5.periods.forEach(period => {
            // Tiêu đề tháng
            sheet5.mergeCells(currentRow, 1, currentRow, table5.productGroups.length + 3);
            const periodTitle = sheet5.getCell(currentRow, 1);
            periodTitle.value = period.label;
            periodTitle.font = { bold: true, size: 12 };
            periodTitle.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: 'BDD7EE' }
            };
            periodTitle.alignment = { horizontal: 'center', vertical: 'middle' };
            currentRow++;

            // Header cho tháng
            const monthHeader = sheet5.getRow(currentRow);
            const monthHeaderValues = ['Nhân viên', ...table5.productGroups, 'Tổng Nhựa', 'Tổng Nhôm', 'Tổng'];
            monthHeader.values = monthHeaderValues;
            
            monthHeader.eachCell((cell, colNumber) => {
                cell.font = { bold: true, color: { argb: 'FFFFFF' } };
                cell.fill = {
                    type: 'pattern',
                    pattern: 'solid',
                    fgColor: { argb: '5B9BD5' }
                };
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
            });
            currentRow++;

            // Dữ liệu nhân viên
            period.data.forEach(empData => {
                const empRow = sheet5.getRow(currentRow);
                const rowData = [empData.employee];
                
                // Dữ liệu sản phẩm
                table5.productGroups.forEach(group => {
                    rowData.push(empData.products[group] || 0);
                });
                
                // Tổng
                rowData.push(empData.tongNhua || 0, empData.tongNhom || 0, empData.tong || 0);
                
                empRow.values = rowData;
                
                // Style
                empRow.eachCell((cell, colNumber) => {
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    
                    if (colNumber > 1) {
                        cell.numFmt = '#,##0';
                        cell.alignment = { horizontal: 'right' };
                    }
                    
                    // Màu xen kẽ
                    if (currentRow % 2 === 0) {
                        cell.fill = {
                            type: 'pattern',
                            pattern: 'solid',
                            fgColor: { argb: 'F2F2F2' }
                        };
                    }
                });
                
                currentRow++;
            });
            
            // Thêm dòng trống giữa các tháng
            currentRow++;
        });

        // Auto fit
        sheet5.columns.forEach(column => {
            column.width = 12;
        });
        sheet5.getColumn(1).width = 20;

        // ============ SHEET 6: DANH SÁCH HỦY ĐƠN HÀNG ============
        const sheet6 = workbook.addWorksheet('Danh sách hủy đơn hàng');
        
        // Tiêu đề
        sheet6.mergeCells('A1:M1');
        const title6 = sheet6.getCell('A1');
        title6.value = 'BÁO CÁO DANH SÁCH HỦY ĐƠN HÀNG';
        title6.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
        title6.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: '4472C4' }
        };
        title6.alignment = { horizontal: 'center', vertical: 'middle' };

        // Header
        const header6 = [
            'STT', 'Khách hàng ID', 'Tên đầy đủ', 'Tên người liên hệ',
            'Điện thoại liên hệ', 'Địa chỉ thực hiện', 'Mã đơn hàng',
            'Hết hàng', 'Giá trị hủy', 'Lý do hủy ghi nhận',
            'Ngày tháng hủy', 'Khối lượng SP hủy', 'Ghi chú'
        ];
        
        const headerRow6 = sheet6.getRow(2);
        headerRow6.values = header6;
        headerRow6.eachCell((cell, colNumber) => {
            cell.font = { bold: true, color: { argb: 'FFFFFF' } };
            cell.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '5B9BD5' }
            };
            cell.border = {
                top: { style: 'thin' },
                left: { style: 'thin' },
                bottom: { style: 'thin' },
                right: { style: 'thin' }
            };
            cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
        });

        // Dữ liệu
        rowIndex = 3;
        table6.forEach((item, idx) => {
            const dataRow = sheet6.getRow(rowIndex);
            const rowData = [
                idx + 1,
                item.khachHangID,
                item.tenDayDu,
                item.nguoiLienHe,
                item.dienThoai,
                item.diaChiThucHien,
                item.maDonHang,
                item.hetHang,
                item.giaTriHuy,
                item.lyDoHuy,
                item.ngayThangHuy,
                item.khoiLuongSPHuy,
                item.ghiChu
            ];
            
            dataRow.values = rowData;
            
            // Style
            dataRow.eachCell((cell, colNumber) => {
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                
                if (colNumber === 9) { // Giá trị hủy
                    cell.numFmt = '#,##0';
                    cell.alignment = { horizontal: 'right' };
                }
                
                // Màu xen kẽ
                if (rowIndex % 2 === 0) {
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'F2F2F2' }
                    };
                }
            });
            
            rowIndex++;
        });

        // Auto fit
        sheet6.columns.forEach((column, index) => {
            if (index === 0) column.width = 8; // STT
            else if (index === 2) column.width = 25; // Tên đầy đủ
            else if (index === 5) column.width = 30; // Địa chỉ
            else if (index === 9) column.width = 20; // Lý do
            else if (index === 12) column.width = 25; // Ghi chú
            else column.width = 15;
        });

        return workbook;
    } catch (error) {
        console.error("❌ Lỗi khi tạo Excel report:", error);
        throw error;
    }
}


////BÁO CÁO - KPI PHÒNG KINH DOANH
// ============================================
// HÀM TIỆN ÍCH
// ============================================

/**
 * Hàm đọc dữ liệu từ Google Sheets
 */
async function readSheet(spreadsheetId, range) {
    try {
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId,
            range,
        });
        return response.data.values || [];
    } catch (error) {
        console.error(`Lỗi đọc sheet ${range}:`, error);
        return [];
    }
}

/**
 * Parse date từ nhiều định dạng - FIXED cho các định dạng mới
 */
function parseDate(dateStr) {
    if (!dateStr || dateStr === '' || dateStr === undefined || dateStr === null) {
        return null;
    }
    
    // Nếu là số serial Excel
    if (typeof dateStr === 'number') {
        try {
            const date = new Date((dateStr - 25569) * 86400 * 1000);
            return isNaN(date.getTime()) ? null : date;
        } catch (error) {
            return null;
        }
    }
    
    // Nếu là chuỗi
    if (typeof dateStr === 'string') {
        dateStr = dateStr.trim();
        if (dateStr === '') return null;
        
        // 1. Định dạng YYYY-MM (tháng/năm)
        const monthYearMatch = /^(\d{4})-(\d{1,2})$/.exec(dateStr);
        if (monthYearMatch) {
            try {
                const year = parseInt(monthYearMatch[1], 10);
                const month = parseInt(monthYearMatch[2], 10) - 1;
                return new Date(year, month, 1); // Ngày đầu tháng
            } catch (error) {
                return null;
            }
        }
        
        // 2. Định dạng Q-YYYY (quý/năm)
        const quarterYearMatch = /^(\d{1})-(\d{4})$/.exec(dateStr);
        if (quarterYearMatch) {
            try {
                const quarter = parseInt(quarterYearMatch[1], 10);
                const year = parseInt(quarterYearMatch[2], 10);
                const month = (quarter - 1) * 3; // Quý 1: tháng 0-2, Quý 2: tháng 3-5, v.v.
                return new Date(year, month, 1); // Ngày đầu quý
            } catch (error) {
                return null;
            }
        }
        
        // 3. Định dạng YYYY (chỉ năm)
        const yearOnlyMatch = /^(\d{4})$/.exec(dateStr);
        if (yearOnlyMatch) {
            try {
                const year = parseInt(yearOnlyMatch[1], 10);
                return new Date(year, 0, 1); // Ngày đầu năm
            } catch (error) {
                return null;
            }
        }
        
        // 4. Định dạng dd/mm/yyyy hoặc dd/mm/yyyy hh:mm:ss
        // Pattern 1: dd/mm/yyyy
        const pattern1 = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/;
        // Pattern 2: dd/mm/yyyy hh:mm:ss hoặc dd/mm/yyyy hh:mm
        const pattern2 = /^(\d{1,2})\/(\d{1,2})\/(\d{4}) (\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/;
        
        let match;
        
        // Thử pattern 2 (có thời gian)
        match = dateStr.match(pattern2);
        if (match) {
            try {
                const day = parseInt(match[1], 10);
                const month = parseInt(match[2], 10) - 1;
                const year = parseInt(match[3], 10);
                const hour = parseInt(match[4], 10) || 0;
                const minute = parseInt(match[5], 10) || 0;
                const second = parseInt(match[6], 10) || 0;
                
                return new Date(year, month, day, hour, minute, second);
            } catch (error) {
                return null;
            }
        }
        
        // Thử pattern 1 (chỉ ngày)
        match = dateStr.match(pattern1);
        if (match) {
            try {
                const day = parseInt(match[1], 10);
                const month = parseInt(match[2], 10) - 1;
                const year = parseInt(match[3], 10);
                
                return new Date(year, month, day);
            } catch (error) {
                return null;
            }
        }
        
        // 5. Định dạng ISO (yyyy-mm-dd)
        const isoMatch = dateStr.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
        if (isoMatch) {
            try {
                const year = parseInt(isoMatch[1], 10);
                const month = parseInt(isoMatch[2], 10) - 1;
                const day = parseInt(isoMatch[3], 10);
                return new Date(year, month, day);
            } catch (error) {
                return null;
            }
        }
        
        // 6. Thử parse với Date constructor (fallback)
        try {
            const date = new Date(dateStr);
            if (!isNaN(date.getTime())) {
                return date;
            }
        } catch (error) {
            return null;
        }
    }
    
    console.log(`Không thể parse date: ${dateStr}`);
    return null;
}


/**
 * Lọc dữ liệu theo thời gian
 */
function filterByDate(data, dateField, filterType, startDate, endDate) {
    if (!data || data.length === 0 || dateField === -1) {
        console.log(`⚠️ Không có dữ liệu hoặc cột ngày không tồn tại: dateField=${dateField}`);
        return data || [];
    }
    
    // Nếu không có điều kiện lọc, trả về toàn bộ dữ liệu
    if (!filterType || filterType === 'none') {
        return data;
    }
    
    // Nếu không có startDate, trả về toàn bộ dữ liệu
    if (!startDate) {
        return data;
    }
    
    return data.filter(row => {
        // Kiểm tra nếu row tồn tại và có giá trị tại cột dateField
        if (!row || row[dateField] === undefined || row[dateField] === null || row[dateField] === '') {
            return false; // Bỏ qua dòng không có ngày
        }
        
        const rowDate = parseDate(row[dateField]);
        if (!rowDate) {
            return false; // Bỏ qua dòng không parse được ngày
        }
        
        // Reset time part for day comparison
        const rowDateOnly = new Date(rowDate.getFullYear(), rowDate.getMonth(), rowDate.getDate());
        
        switch(filterType) {
            case 'day':
                const filterDay = parseDate(startDate);
                if (!filterDay) return false;
                const filterDayOnly = new Date(filterDay.getFullYear(), filterDay.getMonth(), filterDay.getDate());
                return rowDateOnly.getTime() === filterDayOnly.getTime();
                
            case 'week':
                const filterWeek = parseDate(startDate);
                if (!filterWeek) return false;
                const rowWeek = getWeekNumber(rowDate);
                const filterWeekNum = getWeekNumber(filterWeek);
                return rowWeek === filterWeekNum && rowDate.getFullYear() === filterWeek.getFullYear();
                
            case 'month':
                const filterMonth = parseDate(startDate);
                if (!filterMonth) return false;
                return rowDate.getMonth() === filterMonth.getMonth() && 
                       rowDate.getFullYear() === filterMonth.getFullYear();
                
            case 'quarter':
                const filterQuarterDate = parseDate(startDate);
                if (!filterQuarterDate) return false;
                const rowQuarter = getQuarter(rowDate);
                const filterQuarter = getQuarter(filterQuarterDate);
                return rowQuarter === filterQuarter && 
                       rowDate.getFullYear() === filterQuarterDate.getFullYear();
                
            case 'year':
                const filterYear = parseDate(startDate);
                if (!filterYear) return false;
                return rowDate.getFullYear() === filterYear.getFullYear();
                
            case 'range':
                if (!endDate) return true;
                const start = parseDate(startDate);
                const end = parseDate(endDate);
                if (!start || !end) return false;
                const startOnly = new Date(start.getFullYear(), start.getMonth(), start.getDate());
                const endOnly = new Date(end.getFullYear(), end.getMonth(), end.getDate());
                return rowDateOnly >= startOnly && rowDateOnly <= endOnly;
                
            default:
                return true;
        }
    });
}

// Hàm lấy số tuần trong năm
function getWeekNumber(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 4 - (d.getDay() || 7));
    const yearStart = new Date(d.getFullYear(), 0, 1);
    const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
    return weekNo;
}

// Hàm lấy quý
function getQuarter(date) {
    return Math.ceil((date.getMonth() + 1) / 3);
}

// ============================================
// THÊM HÀM ĐỂ CHUYỂN ĐỔI ĐỊNH DẠNG HIỂN THỊ
// ============================================

app.locals.formatMonthYear = function(dateStr) {
    if (!dateStr) return '';
    const date = parseDate(dateStr);
    if (!date) return dateStr;
    return date.toLocaleDateString('vi-VN', { month: '2-digit', year: 'numeric' });
};

app.locals.formatQuarterYear = function(dateStr) {
    if (!dateStr) return '';
    // Định dạng: Q-YYYY
    const match = /^(\d{1})-(\d{4})$/.exec(dateStr);
    if (match) {
        return `Quý ${match[1]}/${match[2]}`;
    }
    return dateStr;
};

// Thêm helper functions vào app.locals
app.locals.getWeekNumber = getWeekNumber;
app.locals.getQuarter = getQuarter;

// ============================================
// HÀM XỬ LÝ BÁO CÁO KPI
// ============================================

//  HÀM LẤY DANH SÁCH NHÂN VIÊN
// ============================================

async function getNhanVienList() {
    try {
        const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
        if (!data || data.length < 2) return [];
        
        const headers = data[0];
        const rows = data.slice(1);
        const colIndex = headers.indexOf('C'); // Cột tên nhân viên
        
        // Lấy danh sách nhân viên duy nhất
        const nhanVienSet = new Set();
        rows.forEach(row => {
            if (row[colIndex] && row[colIndex].trim()) {
                nhanVienSet.add(row[colIndex].trim());
            }
        });
        
        return Array.from(nhanVienSet).sort();
    } catch (error) {
        console.error('Lỗi khi lấy danh sách nhân viên:', error);
        return [];
    }
}

/**
 * 4.1.1 Báo cáo báo giá & đơn hàng theo nhân viên
 */
async function getBaoCaoBaoGiaDonHang(
  filterType = 'month',
  startDate = null,
  endDate = null,
  nhanVien = 'all'
    ) {
  try {
    console.log(`\n=== [DEBUG] getBaoCaoBaoGiaDonHang() called ===`);

    const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!data || data.length < 2) {
      console.log('❌ Không có dữ liệu từ sheet Don_hang');
      return {};
    }

    console.log(`✅ Tổng số dòng dữ liệu: ${data.length}`);

    const headers = data[0];
    const rows = data.slice(1);

    // Map index cột (bắt đầu từ 0)
    const colIndex = {
      ngayTao: 1,            // B
      tenNV: 2,              // C
      maNV: 3,               // D
      maDon: 6,              // G
      tinhTrang: 35,         // AJ
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      nhanVienPheDuyet: 40,  // AO
      ngayPheDuyet: 41,      // AP
      thoiGianChot: 48,      // AW
      thanhTien: 56,         // BE
      hoaHongQC: 65,         // BN
      doanhSoKPI: 69,        // BR
      hoaHongKD: 70          // BS
    };

    // Lọc theo ngày
    let filteredData = filterByDate(
      rows,
      colIndex.ngayTao,
      filterType,
      startDate,
      endDate
    );

    // Lọc theo nhân viên
    if (nhanVien !== 'all') {
      filteredData = filteredData.filter(row => {
        const tenNV = row[colIndex.tenNV] || '';
        const maNV = row[colIndex.maNV] || '';
        return tenNV === nhanVien || maNV === nhanVien;
      });

      console.log(
        `Sau khi lọc theo nhân viên "${nhanVien}": ${filteredData.length} dòng`
      );
    }

    // Nhóm theo nhân viên
    const result = {};

    filteredData.forEach(row => {
      const nv = row[colIndex.tenNV] || 'Chưa xác định';

      if (!result[nv]) {
        result[nv] = {
          tongBaoGia: 0,
          tongDonHang: 0,
          tongDoanhSo: 0,
          tyLeChuyenDoi: 0
        };
      }

      const trangThai = row[colIndex.trangThaiTao];
      const tinhTrang = row[colIndex.tinhTrang];

      if (trangThai === 'Báo giá') {
        result[nv].tongBaoGia++;
      } else if (trangThai === 'Đơn hàng') {
        result[nv].tongDonHang++;

        if (tinhTrang === 'Kế hoạch sản xuất') {
          const doanhSo = Number(row[colIndex.doanhSoKPI]) || 0;
          result[nv].tongDoanhSo += doanhSo;
        }
      }
    });

    // Tính tỷ lệ chuyển đổi
    Object.keys(result).forEach(nv => {
      if (result[nv].tongBaoGia > 0) {
        result[nv].tyLeChuyenDoi =
          (result[nv].tongDonHang / result[nv].tongBaoGia) * 100;
      }
    });

    return result;

  } catch (error) {
    console.error('❌ Lỗi getBaoCaoBaoGiaDonHang:', error);
    throw error; // để API layer xử lý tiếp
  }
}


/**
 * 4.1.2 Doanh số theo nhân viên
 */
async function getDoanhSoTheoNhanVien(filterType = 'month', startDate = null, endDate = null, nhanVien = 'all') {
    const donHangData = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    const kpiData = await readSheet(SPREADSHEET_ID, 'KPI_CHI_TIEU!A:R');
    
    if (!donHangData || donHangData.length < 2) return [];
    
    // Map chỉ số cột Don_hang BẰNG SỐ
    const donHangColIndex = {
        ngayTao: 1,        // B
        tenNV: 2,          // C
        doanhSoKPI: 69,    // BR
        trangThaiTao: 38,  // AM
        pheDuyet: 39       // AN
    };
    
    // Map chỉ số cột KPI_CHI_TIEU
    const kpiColIndex = {
        tenKPI: 2,         // C
        tenNhanSu: 6,      // G
        moTaKPI: 7,        // H
        donVi: 9,          // J
        mucTieu: 10,       // K
        ngayBatDau: 14,    // O
        ngayKetThuc: 15,   // P
        tinhTrang: 17      // R
    };
    
    // Lọc và tính doanh số thực tế
    let filteredDonHang = filterByDate(donHangData.slice(1), donHangColIndex.ngayTao, filterType, startDate, endDate);
    
    if (nhanVien !== 'all') {
        filteredDonHang = filteredDonHang.filter(row => row[donHangColIndex.tenNV] === nhanVien);
    }
    
    // Chỉ tính đơn hàng đã phê duyệt
    filteredDonHang = filteredDonHang.filter(row => 
        row[donHangColIndex.trangThaiTao] === 'Đơn hàng' && 
        row[donHangColIndex.pheDuyet] === 'Phê duyệt'
    );
    
    // Nhóm doanh số theo nhân viên
    const doanhSoThucTe = {};
    filteredDonHang.forEach(row => {
        const nv = row[donHangColIndex.tenNV] || 'Chưa xác định';
        const doanhSo = parseFloat(row[donHangColIndex.doanhSoKPI] || 0);
        
        if (!doanhSoThucTe[nv]) {
            doanhSoThucTe[nv] = 0;
        }
        doanhSoThucTe[nv] += doanhSo;
    });
    
    // Lấy KPI doanh số
    const kpiDoanhSo = kpiData.slice(1).filter(row => 
        row[kpiColIndex.moTaKPI] === 'Doanh số bán hàng' &&
        row[kpiColIndex.tinhTrang] === 'Áp dụng'
    );
    
    // Kết hợp dữ liệu
    const result = [];
    Object.keys(doanhSoThucTe).forEach(nv => {
        const kpiNV = kpiDoanhSo.find(kpi => kpi[kpiColIndex.tenNhanSu] === nv);
        
        result.push({
            tenNhanVien: nv,
            doanhSoThucTe: doanhSoThucTe[nv],
            kpiMucTieu: kpiNV ? parseFloat(kpiNV[kpiColIndex.mucTieu] || 0) : 0,
            tyLeHoanThanh: kpiNV && parseFloat(kpiNV[kpiColIndex.mucTieu]) > 0 ? 
                (doanhSoThucTe[nv] / parseFloat(kpiNV[kpiColIndex.mucTieu])) * 100 : 0,
            danhGia: kpiNV ? (doanhSoThucTe[nv] >= parseFloat(kpiNV[kpiColIndex.mucTieu]) ? 'Đạt' : 'Chưa đạt') : 'Không có KPI'
        });
    });
    
    return result;
}

/**
 * 4.1.3 Đơn hàng hủy
 */
async function getDonHangHuy(page = 1, pageSize = 10, filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!data || data.length < 2) return { data: [], total: 0, totalPages: 0 };
    
    const headers = data[0];
    const rows = data.slice(1);

    const colIndex = {
      ngayTao: 1,            // B
      tenNV: 2,              // C
      maDon: 6,              // G
      khachHangID: 7,        // H
      tenKhach: 9,           // J
      tinhTrang: 35,         // AJ
      trangThaiTao: 38,      // AM
      thanhTien: 56,         // BE
      doanhSoKPI: 69,        // BR
    };
    
    // Lọc đơn hàng hủy
    let filteredData = rows.filter(row => 
        row[colIndex.tinhTrang] === 'Hủy đơn' &&
        row[colIndex.trangThaiTao] === 'Đơn hàng'
    );
    
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    const total = filteredData.length;
    const totalPages = Math.ceil(total / pageSize);
    const startIndex = (page - 1) * pageSize;
    const paginatedData = filteredData.slice(startIndex, startIndex + pageSize);
    
    const result = paginatedData.map(row => ({
        ngayTao: row[colIndex.ngayTao],
        tenNhanVien: row[colIndex.tenNV],
        maDon: row[colIndex.maDon],
        tenKhach: row[colIndex.tenKhach],
        thanhTien: parseFloat(row[colIndex.thanhTien] || 0),
        doanhSoKPI: parseFloat(row[colIndex.doanhSoKPI] || 0)
    }));
    
    return {
        data: result,
        total,
        totalPages,
        currentPage: page
    };
}

/**
 * 4.1.4 Top 100 khách hàng doanh số cao nhất
 */
async function getTopKhachHang(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);
    

    const colIndex = {
      ngayTao: 1,            // B
      khachHangID: 7,        // H
      tenKhach: 9,           // J
      tinhTrang: 35,         // AJ
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      thanhTien: 56,         // BE
      doanhSoKPI: 69,        // BR
    };
    
    // Lọc đơn hàng đã phê duyệt
    let filteredData = rows.filter(row => 
        row[colIndex.trangThaiTao] === 'Đơn hàng' &&
        row[colIndex.pheDuyet] === 'Phê duyệt'
    );
    
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Nhóm theo khách hàng
    const khachHangData = {};
    filteredData.forEach(row => {
        const khachHangID = row[colIndex.khachHangID];
        const tenKhach = row[colIndex.tenKhach];
        const thanhTien = parseFloat(row[colIndex.thanhTien] || 0);
        const doanhSo = parseFloat(row[colIndex.doanhSoKPI] || 0);
        
        if (!khachHangData[khachHangID]) {
            khachHangData[khachHangID] = {
                tenKhach,
                tongThanhTien: 0,
                tongDoanhSo: 0,
                soDonHang: 0
            };
        }
        
        khachHangData[khachHangID].tongThanhTien += thanhTien;
        khachHangData[khachHangID].tongDoanhSo += doanhSo;
        khachHangData[khachHangID].soDonHang++;
    });
    
    // Chuyển thành mảng và sắp xếp
    const result = Object.keys(khachHangData).map(id => ({
        khachHangID: id,
        ...khachHangData[id]
    }));
    
    result.sort((a, b) => b.tongDoanhSo - a.tongDoanhSo);
    
    return result.slice(0, 100);
}

/**
 * 4.1.5 Doanh số khách hàng cũ (từ 2 đơn hàng trở lên)
 */
async function getDoanhSoKhachHangCu(page = 1, pageSize = 10, filterType = 'month', startDate = null, endDate = null) {
    const donHangData = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!donHangData || donHangData.length < 2) return { data: [], total: 0, totalPages: 0 };
    
    const headers = donHangData[0];
    const rows = donHangData.slice(1);
    
    const colIndex = {
        ngayTao: headers.indexOf('B'),
        tenNV: headers.indexOf('C'),
        khachHangID: headers.indexOf('H'),
        tenKhach: headers.indexOf('J'),
        trangThaiTao: headers.indexOf('AM'),
        pheDuyet: headers.indexOf('AN'),
        thanhTien: headers.indexOf('BE'),
        doanhSoKPI: headers.indexOf('BR')
    };

    const colIndex = {
      ngayTao: 1,            // B
      tenNV: 2,              // C
      khachHangID: 7,        // H
      tenKhach: 9,           // J
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      thanhTien: 56,         // BE
      hoaHongQC: 65,         // BN
      doanhSoKPI: 69,        // BR
    };
    
    // Lọc đơn hàng đã phê duyệt
    let filteredData = rows.filter(row => 
        row[colIndex.trangThaiTao] === 'Đơn hàng' &&
        row[colIndex.pheDuyet] === 'Phê duyệt'
    );
    
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Nhóm theo khách hàng và đếm số đơn
    const khachHangStats = {};
    filteredData.forEach(row => {
        const khachHangID = row[colIndex.khachHangID];
        const tenKhach = row[colIndex.tenKhach];
        const tenNV = row[colIndex.tenNV];
        const thanhTien = parseFloat(row[colIndex.thanhTien] || 0);
        const doanhSo = parseFloat(row[colIndex.doanhSoKPI] || 0);
        
        if (!khachHangStats[khachHangID]) {
            khachHangStats[khachHangID] = {
                tenKhach,
                tenNhanVien: tenNV,
                tongThanhTien: 0,
                tongDoanhSo: 0,
                soDonHang: 0
            };
        }
        
        khachHangStats[khachHangID].tongThanhTien += thanhTien;
        khachHangStats[khachHangID].tongDoanhSo += doanhSo;
        khachHangStats[khachHangID].soDonHang++;
    });
    
    // Lọc khách hàng có từ 2 đơn trở lên
    const khachHangCu = Object.keys(khachHangStats)
        .filter(id => khachHangStats[id].soDonHang >= 2)
        .map(id => ({
            khachHangID: id,
            ...khachHangStats[id]
        }));
    
    const total = khachHangCu.length;
    const totalPages = Math.ceil(total / pageSize);
    const startIndex = (page - 1) * pageSize;
    const paginatedData = khachHangCu.slice(startIndex, startIndex + pageSize);
    
    return {
        data: paginatedData,
        total,
        totalPages,
        currentPage: page
    };
}

/**
 * 4.1.6 Doanh số khách hàng mới (chỉ 1 đơn hàng)
 */
async function getDoanhSoKhachHangMoi(page = 1, pageSize = 10, filterType = 'month', startDate = null, endDate = null) {
    const donHangData = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!donHangData || donHangData.length < 2) return { data: [], total: 0, totalPages: 0 };
    
    const headers = donHangData[0];
    const rows = donHangData.slice(1);

    const colIndex = {
      ngayTao: 1,            // B
      tenNV: 2,              // C
      khachHangID: 7,       // H
      tenKhach: 9,          //  j
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      thanhTien: 56,         // BE
      doanhSoKPI: 69       // BR
    };
    
    let filteredData = rows.filter(row => 
        row[colIndex.trangThaiTao] === 'Đơn hàng' &&
        row[colIndex.pheDuyet] === 'Phê duyệt'
    );
    
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    const khachHangStats = {};
    filteredData.forEach(row => {
        const khachHangID = row[colIndex.khachHangID];
        const tenKhach = row[colIndex.tenKhach];
        const tenNV = row[colIndex.tenNV];
        const thanhTien = parseFloat(row[colIndex.thanhTien] || 0);
        const doanhSo = parseFloat(row[colIndex.doanhSoKPI] || 0);
        
        if (!khachHangStats[khachHangID]) {
            khachHangStats[khachHangID] = {
                tenKhach,
                tenNhanVien: tenNV,
                tongThanhTien: 0,
                tongDoanhSo: 0,
                soDonHang: 0
            };
        }
        
        khachHangStats[khachHangID].tongThanhTien += thanhTien;
        khachHangStats[khachHangID].tongDoanhSo += doanhSo;
        khachHangStats[khachHangID].soDonHang++;
    });
    
    const khachHangMoi = Object.keys(khachHangStats)
        .filter(id => khachHangStats[id].soDonHang === 1)
        .map(id => ({
            khachHangID: id,
            ...khachHangStats[id]
        }));
    
    const total = khachHangMoi.length;
    const totalPages = Math.ceil(total / pageSize);
    const startIndex = (page - 1) * pageSize;
    const paginatedData = khachHangMoi.slice(startIndex, startIndex + pageSize);
    
    return {
        data: paginatedData,
        total,
        totalPages,
        currentPage: page
    };
}

/**
 * 4.1.7 Khách chuyển từ báo giá → đơn hàng
 */
async function getKhachChuyenDoi(page = 1, pageSize = 10, filterType = 'month', startDate = null, endDate = null) {
    const donHangData = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!donHangData || donHangData.length < 2) return { data: [], total: 0, totalPages: 0 };
    
    const headers = donHangData[0];
    const rows = donHangData.slice(1);

    const colIndex = {
      ngayTao: 1,            // B
      tenNV: 2,              // C
      maDon: 6,              // G
      khachHangID: 7,        // H
      tenKhach: 9,          // J
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      thanhTien: 56,         // BE
      doanhSoKPI: 69,        // BR
    };
    
    // Tìm khách có cả báo giá và đơn hàng
    const khachData = {};
    rows.forEach(row => {
        const khachHangID = row[colIndex.khachHangID];
        const trangThai = row[colIndex.trangThaiTao];
        
        if (!khachData[khachHangID]) {
            khachData[khachHangID] = {
                coBaoGia: false,
                coDonHang: false,
                tenKhach: row[colIndex.tenKhach],
                tenNhanVien: row[colIndex.tenNV],
                tongDoanhSo: 0
            };
        }
        
        if (trangThai === 'Báo giá') {
            khachData[khachHangID].coBaoGia = true;
        } else if (trangThai === 'Đơn hàng' && row[colIndex.pheDuyet] === 'Phê duyệt') {
            khachData[khachHangID].coDonHang = true;
            khachData[khachHangID].tongDoanhSo += parseFloat(row[colIndex.doanhSoKPI] || 0);
        }
    });
    
    // Lọc khách có chuyển đổi
    const khachChuyenDoi = Object.keys(khachData)
        .filter(id => khachData[id].coBaoGia && khachData[id].coDonHang)
        .map(id => ({
            khachHangID: id,
            ...khachData[id]
        }));
    
    const total = khachChuyenDoi.length;
    const totalPages = Math.ceil(total / pageSize);
    const startIndex = (page - 1) * pageSize;
    const paginatedData = khachChuyenDoi.slice(startIndex, startIndex + pageSize);
    
    return {
        data: paginatedData,
        total,
        totalPages,
        currentPage: page
    };
}

/**
 * 4.1.8 Đơn hàng được phê duyệt theo nhân viên
 */
async function getDonHangPheDuyet(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);

    const colIndex = {
      ngayTao: 1,            // B
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      nhanVienPheDuyet: 40,  // AO
    };
    
    let filteredData = rows.filter(row => 
        row[colIndex.trangThaiTao] === 'Đơn hàng' &&
        row[colIndex.pheDuyet] === 'Phê duyệt'
    );
    
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    const result = {};
    filteredData.forEach(row => {
        const nvPheDuyet = row[colIndex.nhanVienPheDuyet] || 'Chưa xác định';
        if (!result[nvPheDuyet]) {
            result[nvPheDuyet] = 0;
        }
        result[nvPheDuyet]++;
    });
    
    return Object.keys(result).map(nv => ({
        tenNhanVien: nv,
        soDonPheDuyet: result[nv]
    }));
}

/**
 * 4.1.9 Khách hàng mới được tạo
 */
async function getKhachHangMoi(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Data_khach_hang!A:AH');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        ngayTao: 19,    // headers.indexOf('AG'),
        nguoiTao: 20,   // headers.indexOf('AH'),
        khachHangID: 2, // headers.indexOf('C'),
        loaiKhach: 3,   // headers.indexOf('D'),
        tenKhach: 4,     // headers.indexOf('E'),
        nguonKhach: 15   // headers.indexOf('AC')
    };
    
    let filteredData = filterByDate(rows, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Nhóm theo người tạo
    const result = {};
    filteredData.forEach(row => {
        const nguoiTao = row[colIndex.nguoiTao] || 'Chưa xác định';
        if (!result[nguoiTao]) {
            result[nguoiTao] = [];
        }
        
        result[nguoiTao].push({
            khachHangID: row[colIndex.khachHangID],
            tenKhach: row[colIndex.tenKhach],
            loaiKhach: row[colIndex.loaiKhach],
            nguonKhach: row[colIndex.nguonKhach],
            ngayTao: row[colIndex.ngayTao]
        });
    });
    
    return result;
}

/**
 * 4.1.10 Khách hàng đại lý mới
 */
async function getKhachHangDaiLyMoi(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Data_khach_hang!A:AH');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);

    const colIndex = {
        ngayTao: 19,    // headers.indexOf('T'),
        nguoiTao: 20,   // headers.indexOf('U'),
        khachHangID: 2, // headers.indexOf('C'),
        loaiKhach: 3,   // headers.indexOf('D'),
        tenKhach: 4,     // headers.indexOf('E'),
    };
    
    let filteredData = rows.filter(row => row[colIndex.loaiKhach] === 'Đại lý');
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    const result = {};
    filteredData.forEach(row => {
        const nguoiTao = row[colIndex.nguoiTao] || 'Chưa xác định';
        if (!result[nguoiTao]) {
            result[nguoiTao] = [];
        }
        
        result[nguoiTao].push({
            khachHangID: row[colIndex.khachHangID],
            tenKhach: row[colIndex.tenKhach],
            ngayTao: row[colIndex.ngayTao]
        });
    });
    
    return result;
}

/**
 * 4.1.11 Khách hàng được bàn giao
 */
async function getKhachHangBanGiao(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Data_khach_hang!A:AH');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        ngayBanGiao:18, // headers.indexOf('AF'),
        nhanVienPhuTrach: 17,   // headers.indexOf('AE'),
        khachHangID: 2,     // headers.indexOf('C'),
        tenKhach: 4,        //headers.indexOf('E'),
        loaiKhach: 3        // headers.indexOf('D')
    };
    
    let filteredData = rows.filter(row => row[colIndex.ngayBanGiao]);
    filteredData = filterByDate(filteredData, colIndex.ngayBanGiao, filterType, startDate, endDate);
    
    const result = {};
    filteredData.forEach(row => {
        const nvPhuTrach = row[colIndex.nhanVienPhuTrach] || 'Chưa xác định';
        if (!result[nvPhuTrach]) {
            result[nvPhuTrach] = [];
        }
        
        result[nvPhuTrach].push({
            khachHangID: row[colIndex.khachHangID],
            tenKhach: row[colIndex.tenKhach],
            loaiKhach: row[colIndex.loaiKhach],
            ngayBanGiao: row[colIndex.ngayBanGiao]
        });
    });
    
    return result;
}

/**
 * 4.1.12 Tổng hợp hoa hồng kinh doanh
 */
async function getHoaHongKinhDoanh(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);

    const colIndex = {
      ngayTao: 1,            // B
      tenNV: 2,              // C
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      hoaHongKD: 70          // BS
    };
    
    let filteredData = rows.filter(row => 
        row[colIndex.trangThaiTao] === 'Đơn hàng' &&
        row[colIndex.pheDuyet] === 'Phê duyệt'
    );
    
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    const result = {};
    filteredData.forEach(row => {
        const nv = row[colIndex.tenNV] || 'Chưa xác định';
        const hoaHong = parseFloat(row[colIndex.hoaHongKD] || 0);
        
        if (!result[nv]) {
            result[nv] = 0;
        }
        result[nv] += hoaHong;
    });
    
    return Object.keys(result).map(nv => ({
        tenNhanVien: nv,
        tongHoaHong: result[nv]
    }));
}

/**
 * 4.1.13 Tổng hợp hoa hồng Quảng cáo truyền thông
 */
async function getHoaHongQuangCao(filterType = 'month', startDate = null, endDate = null) {
    const donHangData = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
    const khachHangData = await readSheet(SPREADSHEET_ID, 'Data_khach_hang!A:AH');
    
    if (!donHangData || donHangData.length < 2) return [];
    if (!khachHangData || khachHangData.length < 2) return [];
    
    const donHangHeaders = donHangData[0];
    const khachHangHeaders = khachHangData[0];
    
    const donHangColIndex = {
      ngayTao: 1,            // B
      khachHangID: 7,        // H
      trangThaiTao: 38,      // AM
      pheDuyet: 39,          // AN
      hoaHongQC: 65,         // BN
    };

    const khachHangColIndex = {
        khachHangID: 2,  // khachHangHeaders.indexOf('C'),
        nguoiTao: 20    // khachHangHeaders.indexOf('AH')
    };

    
    
    // Tạo map khách hàng - người tạo
    const khachHangMap = {};
    khachHangData.slice(1).forEach(row => {
        khachHangMap[row[khachHangColIndex.khachHangID]] = row[khachHangColIndex.nguoiTao];
    });
    
    // Lọc đơn hàng
    let filteredData = donHangData.slice(1).filter(row => 
        row[donHangColIndex.trangThaiTao] === 'Đơn hàng' &&
        row[donHangColIndex.pheDuyet] === 'Phê duyệt'
    );
    
    filteredData = filterByDate(filteredData, donHangColIndex.ngayTao, filterType, startDate, endDate);
    
    // Chỉ tính hoa hồng cho khách hàng được tạo bởi Hân hoặc Quỳnh Anh
    const result = {};
    filteredData.forEach(row => {
        const khachHangID = row[donHangColIndex.khachHangID];
        const nguoiTao = khachHangMap[khachHangID];
        
        if (nguoiTao === 'Nguyễn Thị Hân' || nguoiTao === 'Ngô Quỳnh Anh') {
            const hoaHong = parseFloat(row[donHangColIndex.hoaHongQC] || 0);
            
            if (!result[nguoiTao]) {
                result[nguoiTao] = 0;
            }
            result[nguoiTao] += hoaHong;
        }
    });
    
    return Object.keys(result).map(nguoiTao => ({
        tenNhanVien: nguoiTao,
        tongHoaHongQC: result[nguoiTao]
    }));
}

/**
 * 4.2.1 Số lượng bài đăng bán hàng
 */
async function getBaiDangBanHang(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'Bao_cao_bai_dang_ban_hang!A:G');
    if (!data || data.length < 2) return { theoNhanVien: [], theoKenh: [] };
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        ngayBaoCao: 3,      //headers.indexOf('D'),
        tenNV: 2,           //headers.indexOf('C'),
        kenhDang: 4,        //headers.indexOf('E'),
        link: 5B9BD5        //headers.indexOf('F')
    };
    
    let filteredData = filterByDate(rows, colIndex.ngayBaoCao, filterType, startDate, endDate);
    
    // Theo nhân viên
    const theoNhanVien = {};
    filteredData.forEach(row => {
        const nv = row[colIndex.tenNV] || 'Chưa xác định';
        if (!theoNhanVien[nv]) {
            theoNhanVien[nv] = 0;
        }
        theoNhanVien[nv]++;
    });
    
    // Theo kênh
    const theoKenh = {};
    filteredData.forEach(row => {
        const kenh = row[colIndex.kenhDang] || 'Chưa xác định';
        if (!theoKenh[kenh]) {
            theoKenh[kenh] = 0;
        }
        theoKenh[kenh]++;
    });
    
    return {
        theoNhanVien: Object.keys(theoNhanVien).map(nv => ({
            tenNhanVien: nv,
            soBaiDang: theoNhanVien[nv]
        })),
        theoKenh: Object.keys(theoKenh).map(kenh => ({
            kenhDang: kenh,
            soBaiDang: theoKenh[kenh]
        }))
    };
}

/**
 * 4.2.2 Báo cáo kết quả chiến dịch quảng cáo
 */
async function getKetQuaChienDich(filterType = 'month', startDate = null, endDate = null) {
    const chienDichData = await readSheet(SPREADSHEET_QC_TT_ID, 'Chien_dich_quang_cao!A:M');
    const chiTietData = await readSheet(SPREADSHEET_QC_TT_ID, 'Quang_cao_chi_tiet!A:I');
    
    if (!chienDichData || chienDichData.length < 2) return [];
    
    const headers = chienDichData[0];
    const rows = chienDichData.slice(1);
    
    const colIndex = {
        thangNam: 3,        //headers.indexOf('D'),
        maChienDich: 1,     // headers.indexOf('B'),
        tenChienDich: 4,    //headers.indexOf('E'),
        kenhChay: 5,        // headers.indexOf('F'),
        chiPhiDuKien: 6,    // headers.indexOf('G'),
        chiPhiThucTe: 7,    //headers.indexOf('H'),
        soLead: 8,          //headers.indexOf('I'),
        nhanSuPhuTrach: 10, //headers.indexOf('K'),
        ngayTao: 12         //headers.indexOf('M')
    };
    
    let filteredData = filterByDate(rows, colIndex.ngayTao, filterType, startDate, endDate);
    
    const result = filteredData.map(row => {
        const chiPhiDuKien = parseFloat(row[colIndex.chiPhiDuKien] || 0);
        const chiPhiThucTe = parseFloat(row[colIndex.chiPhiThucTe] || 0);
        const soLead = parseFloat(row[colIndex.soLead] || 0);
        
        // Tính chỉ số hiệu quả
        const tyLeTieuThu = chiPhiDuKien > 0 ? (chiPhiThucTe / chiPhiDuKien) * 100 : 0;
        const chiPhiTrungBinhLead = soLead > 0 ? (chiPhiThucTe / soLead) : 0;
        
        return {
            maChienDich: row[colIndex.maChienDich],
            tenChienDich: row[colIndex.tenChienDich],
            kenhChay: row[colIndex.kenhChay],
            chiPhiDuKien,
            chiPhiThucTe,
            soLead,
            nhanSuPhuTrach: row[colIndex.nhanSuPhuTrach],
            tyLeTieuThu,
            chiPhiTrungBinhLead,
            danhGia: tyLeTieuThu > 100 ? 'Vượt ngân sách' : 
                    tyLeTieuThu >= 80 ? 'Đạt mục tiêu' : 
                    tyLeTieuThu >= 50 ? 'Cần cải thiện' : 'Hiệu quả thấp'
        };
    });
    
    return result;
}

/**
 * 4.3 Báo cáo Chăm sóc khách hàng
 */
async function getChamSocKhachHang(filterType = 'month', startDate = null, endDate = null, nhanVien = 'all') {
    const chamSocData = await readSheet(SPREADSHEET_ID, 'Cham_soc_khach_hang!A:L');
    const khachHangData = await readSheet(SPREADSHEET_ID, 'Data_khach_hang!A:AH');
    
    if (!chamSocData || chamSocData.length < 2) return { tongHop: [], chiTiet: [] };
    
    const headers = chamSocData[0];
    const rows = chamSocData.slice(1);
    
    const colIndex = {
        ngayChamSoc: 5,         //headers.indexOf('F'),
        tenNV: 7,               //headers.indexOf('H'),
        maNV: 6,                //headers.indexOf('G'),
        khachHangID: 1,         //headers.indexOf('B'),
        tenKhach: 2,            //headers.indexOf('C'),
        hinhThuc: 3,            //headers.indexOf('D'),
        noiDung: 4,             //headers.indexOf('E'),
        ketQua: 8,              //headers.indexOf('I'),
        noiDungTiep: 9,         //headers.indexOf('J'),
        ngayHenTiep: 10         //headers.indexOf('K')
    };
    
    let filteredData = filterByDate(rows, colIndex.ngayChamSoc, filterType, startDate, endDate);
    
    if (nhanVien !== 'all') {
        filteredData = filteredData.filter(row => 
            row[colIndex.tenNV] === nhanVien || row[colIndex.maNV] === nhanVien
        );
    }
    
    // Tổng hợp theo nhân viên
    const tongHop = {};
    filteredData.forEach(row => {
        const nv = row[colIndex.tenNV] || 'Chưa xác định';
        if (!tongHop[nv]) {
            tongHop[nv] = 0;
        }
        tongHop[nv]++;
    });
    
    // Chi tiết chăm sóc
    const chiTiet = filteredData.map(row => ({
        ngayChamSoc: row[colIndex.ngayChamSoc],
        tenNhanVien: row[colIndex.tenNV],
        khachHangID: row[colIndex.khachHangID],
        tenKhach: row[colIndex.tenKhach],
        hinhThuc: row[colIndex.hinhThuc],
        ketQua: row[colIndex.ketQua],
        noiDungTiep: row[colIndex.noiDungTiep],
        ngayHenTiep: row[colIndex.ngayHenTiep]
    }));
    
    return {
        tongHop: Object.keys(tongHop).map(nv => ({
            tenNhanVien: nv,
            soLanChamSoc: tongHop[nv]
        })),
        chiTiet
    };
}

/**
 * 4.4 Báo cáo Kỹ thuật - Thiết kế
 */
async function getBaoCaoKyThuat(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_ID, 'data_file_ban_ve_dinh_kem!A:N');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        maDonHang: 1,       //headers.indexOf('B'),
        soLanThietKe: 2,        //headers.indexOf('C'),
        loaiBanVe: 9,       //headers.indexOf('J'),
        thoiGianBatDau: 10,     // headers.indexOf('K'),
        thoiGianTiepNhan: 11,   // headers.indexOf('L'),
        thoiGianHoanThanh: 12,      //headers.indexOf('M'),
        nhanSuKyThuat: 13           //headers.indexOf('N')
    };
    
    let filteredData = filterByDate(rows, colIndex.thoiGianTiepNhan, filterType, startDate, endDate);
    
    const result = filteredData.map(row => {
        const thoiGianTiepNhan = parseDate(row[colIndex.thoiGianTiepNhan]);
        const thoiGianHoanThanh = parseDate(row[colIndex.thoiGianHoanThanh]);
        
        // Tính thời gian tiếp nhận (phút từ lúc bắt đầu)
        let thoiGianTiepNhanPhut = null;
        if (row[colIndex.thoiGianBatDau] && thoiGianTiepNhan) {
            const thoiGianBatDau = parseDate(row[colIndex.thoiGianBatDau]);
            if (thoiGianBatDau) {
                thoiGianTiepNhanPhut = Math.round((thoiGianTiepNhan - thoiGianBatDau) / (1000 * 60));
            }
        }
        
        // Tính thời gian thực hiện
        let thoiGianThucHien = null;
        if (thoiGianTiepNhan && thoiGianHoanThanh) {
            thoiGianThucHien = Math.round((thoiGianHoanThanh - thoiGianTiepNhan) / (1000 * 60 * 60 * 24)); // Số ngày
        }
        
        return {
            maDonHang: row[colIndex.maDonHang],
            soLanThietKe: parseInt(row[colIndex.soLanThietKe] || 0),
            loaiBanVe: row[colIndex.loaiBanVe],
            nhanSuKyThuat: row[colIndex.nhanSuKyThuat],
            thoiGianTiepNhanPhut,
            thoiGianThucHienNgay: thoiGianThucHien,
            danhGiaTiepNhan: thoiGianTiepNhanPhut !== null ? 
                (thoiGianTiepNhanPhut <= 30 ? 'Đạt' : 'Chậm trễ') : 'Không xác định'
        };
    });
    
    return result;
}

/**
 * 4.5 Báo cáo Thực hiện công việc
 */
async function getBaoCaoThucHienCV(filterType = 'month', startDate = null, endDate = null) {
    const giaoViecData = await readSheet(SPREADSHEET_ID, 'Giao_viec_kinh_doanh!A:K');
    const chiTietData = await readSheet(SPREADSHEET_ID, 'Giao_viec_kd_chi_tiet!A:O');
    
    if (!giaoViecData || giaoViecData.length < 2) return [];
    
    const headers = giaoViecData[0];
    const rows = giaoViecData.slice(1);
    
    const colIndex = {
        ngayTao: 1,     //headers.indexOf('B'),
        nguoiTiepNhan: 9,   //headers.indexOf('J'),
        maNguoiTiepNhan: 10,    // headers.indexOf('K'),
        tenCongViec: 7      //headers.indexOf('H')
    };
    
    let filteredData = filterByDate(rows, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Nhóm theo người tiếp nhận
    const result = {};
    filteredData.forEach(row => {
        const nguoiTiepNhan = row[colIndex.nguoiTiepNhan] || 'Chưa xác định';
        if (!result[nguoiTiepNhan]) {
            result[nguoiTiepNhan] = {
                tongCongViec: 0,
                daTiepNhan: 0,
                daHoanThanh: 0
            };
        }
        result[nguoiTiepNhan].tongCongViec++;
        
        // Kiểm tra tiếp nhận (có người tiếp nhận là đã tiếp nhận)
        if (nguoiTiepNhan !== 'Chưa xác định') {
            result[nguoiTiepNhan].daTiepNhan++;
        }
        
        // Cần kết hợp với chi tiết công việc để biết trạng thái hoàn thành
        // Ở đây giả sử đã tiếp nhận là đang xử lý
    });
    
    // Tính tỷ lệ
    Object.keys(result).forEach(nguoi => {
        result[nguoi].tyLeTiepNhan = result[nguoi].tongCongViec > 0 ? 
            (result[nguoi].daTiepNhan / result[nguoi].tongCongViec) * 100 : 0;
        
        // Giả sử 70% công việc đã tiếp nhận là hoàn thành
        result[nguoi].daHoanThanh = Math.round(result[nguoi].daTiepNhan * 0.7);
        result[nguoi].tyLeHoanThanh = result[nguoi].daTiepNhan > 0 ? 
            (result[nguoi].daHoanThanh / result[nguoi].daTiepNhan) * 100 : 0;
    });
    
    return Object.keys(result).map(nguoi => ({
        tenNhanVien: nguoi,
        ...result[nguoi]
    }));
}

/**
 * 4.6 Báo cáo số ngày làm việc (chấm công)
 */
async function getBaoCaoChamCong(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_HC_ID, 'Cham_cong!A:R');
    if (!data || data.length < 2) return { daDuyet: [], chuaDuyet: [] };
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        ngayTao: 1,     //headers.indexOf('B'),
        trangThai: 2,       //headers.indexOf('C'),
        loaiViec: 3,        //headers.indexOf('D'),
        nguoiTao: 11,       //headers.indexOf('L'),
        boPhan: 13,         //headers.indexOf('N'),
        pheDuyet: 17        //headers.indexOf('R')
    };
    
    let filteredData = filterByDate(rows, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Chỉ lấy trạng thái làm việc
    filteredData = filteredData.filter(row => row[colIndex.trangThai] === 'Làm việc');
    
    // Nhóm theo nhân sự
    const nhomDuLieu = {};
    filteredData.forEach(row => {
        const nhanSu = row[colIndex.nguoiTao] || 'Chưa xác định';
        if (!nhomDuLieu[nhanSu]) {
            nhomDuLieu[nhanSu] = {
                daDuyet: 0,
                chuaDuyet: 0,
                boPhan: row[colIndex.boPhan]
            };
        }
        
        if (row[colIndex.pheDuyet] === 'Duyệt') {
            nhomDuLieu[nhanSu].daDuyet++;
        } else {
            nhomDuLieu[nhanSu].chuaDuyet++;
        }
    });
    
    const daDuyet = [];
    const chuaDuyet = [];
    
    Object.keys(nhomDuLieu).forEach(nhanSu => {
        const data = {
            tenNhanSu: nhanSu,
            boPhan: nhomDuLieu[nhanSu].boPhan,
            soNgayDaDuyet: nhomDuLieu[nhanSu].daDuyet,
            soNgayChuaDuyet: nhomDuLieu[nhanSu].chuaDuyet,
            tongSoNgay: nhomDuLieu[nhanSu].daDuyet + nhomDuLieu[nhanSu].chuaDuyet
        };
        
        if (nhomDuLieu[nhanSu].chuaDuyet > 0) {
            chuaDuyet.push(data);
        } else {
            daDuyet.push(data);
        }
    });
    
    return { daDuyet, chuaDuyet };
}

/**
 * 4.7 Báo cáo số ngày đi khảo sát công trình
 */
async function getBaoCaoKhaoSat(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_HC_ID, 'Cham_cong!A:R');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        ngayTao: 1,     //headers.indexOf('B'),
        trangThai: 2,       //headers.indexOf('C'),
        loaiViec: 3,        //headers.indexOf('D'),
        nguoiTao: 11,       //headers.indexOf('L'),
        boPhan: 13,         //headers.indexOf('N'),
        pheDuyet: 17        //headers.indexOf('R')
    };
    
    let filteredData = rows.filter(row => row[colIndex.loaiViec] === 'Khảo sát công trình');
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Chỉ lấy đã duyệt
    filteredData = filteredData.filter(row => row[colIndex.pheDuyet] === 'Duyệt');
    
    // Nhóm theo nhân sự
    const result = {};
    filteredData.forEach(row => {
        const nhanSu = row[colIndex.nguoiTao] || 'Chưa xác định';
        if (!result[nhanSu]) {
            result[nhanSu] = {
                soNgay: 0,
                boPhan: row[colIndex.boPhan],
                danhSachCongTrinh: []
            };
        }
        result[nhanSu].soNgay++;
        
        // Thêm mô tả công trình
        if (row[colIndex.moTa]) {
            result[nhanSu].danhSachCongTrinh.push({
                ngay: row[colIndex.ngayTao],
                moTa: row[colIndex.moTa]
            });
        }
    });
    
    return Object.keys(result).map(nhanSu => ({
        tenNhanSu: nhanSu,
        boPhan: result[nhanSu].boPhan,
        soNgayKhaoSat: result[nhanSu].soNgay,
        danhSachCongTrinh: result[nhanSu].danhSachCongTrinh
    }));
}

/**
 * 4.8 Báo cáo số ngày công làm việc tại văn phòng
 */
async function getBaoCaoVanPhong(filterType = 'month', startDate = null, endDate = null) {
    const data = await readSheet(SPREADSHEET_HC_ID, 'Cham_cong!A:R');
    if (!data || data.length < 2) return [];
    
    const headers = data[0];
    const rows = data.slice(1);
    
    const colIndex = {
        ngayTao: 1,     //headers.indexOf('B'),
        trangThai: 2,       //headers.indexOf('C'),
        loaiViec: 3,        //headers.indexOf('D'),
        nguoiTao: 11,       //headers.indexOf('L'),
        boPhan: 13,         //headers.indexOf('N'),
        pheDuyet: 17        //headers.indexOf('R')
    };
    
    let filteredData = rows.filter(row => row[colIndex.loaiViec] === 'Làm việc tại văn phòng');
    filteredData = filterByDate(filteredData, colIndex.ngayTao, filterType, startDate, endDate);
    
    // Chỉ lấy đã duyệt
    filteredData = filteredData.filter(row => row[colIndex.pheDuyet] === 'Duyệt');
    
    // Nhóm theo nhân sự
    const result = {};
    filteredData.forEach(row => {
        const nhanSu = row[colIndex.nguoiTao] || 'Chưa xác định';
        if (!result[nhanSu]) {
            result[nhanSu] = {
                soNgay: 0,
                boPhan: row[colIndex.boPhan],
                danhSachCongViec: []
            };
        }
        result[nhanSu].soNgay++;
        
        // Thêm mô tả công việc
        if (row[colIndex.moTa]) {
            result[nhanSu].danhSachCongViec.push({
                ngay: row[colIndex.ngayTao],
                moTa: row[colIndex.moTa]
            });
        }
    });
    
    return Object.keys(result).map(nhanSu => ({
        tenNhanSu: nhanSu,
        boPhan: result[nhanSu].boPhan,
        soNgayVanPhong: result[nhanSu].soNgay,
        danhSachCongViec: result[nhanSu].danhSachCongViec
    }));
}

// ============================================
// ROUTES
// ============================================

app.get('/debug-data', async (req, res) => {
    try {
        const data = await readSheet(SPREADSHEET_ID, 'Don_hang!A:BR');
        const headers = data[0];
        const rows = data.slice(1);
        
        // Kiểm tra cột ngày tạo
        const colIndex = headers.indexOf('B');
        
        const sampleData = rows.slice(0, 10).map((row, index) => {
            return {
                row: index + 2,
                dateString: row[colIndex],
                parsedDate: parseDate(row[colIndex]),
                isValid: parseDate(row[colIndex]) !== null
            };
        });
        
        // Kiểm tra một số cột quan trọng
        const importantCols = {
            'B': 'Ngày tạo',
            'C': 'Tên nhân viên',
            'AM': 'Trạng thái tạo',
            'AN': 'Phê duyệt',
            'BR': 'Doanh số KPI'
        };
        
        const columnData = {};
        for (const [col, name] of Object.entries(importantCols)) {
            const idx = headers.indexOf(col);
            if (idx !== -1) {
                const sampleValues = rows.slice(0, 5).map(row => row[idx]);
                columnData[name] = {
                    index: idx,
                    sampleValues: sampleValues
                };
            }
        }
        
        res.json({
            totalRows: rows.length,
            headers: headers,
            dateColumnIndex: colIndex,
            dateColumnName: headers[colIndex],
            sampleData: sampleData,
            columns: columnData
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Route chính cho báo cáo KPI
app.get('/baocao-kpi-phong-kinh-doanh', async (req, res) => {
    try {
        const {
            loaiBaoCao = 'tongHop',
            filterType = 'none', // Mặc định là 'none' thay vì 'month'
            startDate,
            endDate,
            nhanVien = 'all',
            page = 1
        } = req.query;
        
        let data = {};
        let reportTitle = 'Báo cáo tổng hợp KPI';
        
        // Lấy danh sách nhân viên
        const dsNhanVien = await getNhanVienList();
        
        // Xử lý các loại báo cáo...
        switch(loaiBaoCao) {
            case 'baoGiaDonHang':
                data = await getBaoCaoBaoGiaDonHang(filterType, startDate, endDate, nhanVien);
                reportTitle = 'Báo cáo báo giá & đơn hàng theo nhân viên';
                break;
                
            case 'doanhSoTheoNV':
                data = await getDoanhSoTheoNhanVien(filterType, startDate, endDate, nhanVien);
                reportTitle = 'Doanh số theo nhân viên';
                break;
                
            case 'donHangHuy':
                data = await getDonHangHuy(parseInt(page), 10, filterType, startDate, endDate);
                reportTitle = 'Đơn hàng hủy';
                break;
                
            case 'topKhachHang':
                data = await getTopKhachHang(filterType, startDate, endDate);
                reportTitle = 'Top 100 khách hàng doanh số cao nhất';
                break;
                
            case 'khachHangCu':
                data = await getDoanhSoKhachHangCu(parseInt(page), 10, filterType, startDate, endDate);
                reportTitle = 'Doanh số khách hàng cũ';
                break;
                
            case 'khachHangMoi':
                data = await getDoanhSoKhachHangMoi(parseInt(page), 10, filterType, startDate, endDate);
                reportTitle = 'Doanh số khách hàng mới';
                break;
                
            case 'khachChuyenDoi':
                data = await getKhachChuyenDoi(parseInt(page), 10, filterType, startDate, endDate);
                reportTitle = 'Khách chuyển từ báo giá → đơn hàng';
                break;
                
            case 'donHangPheDuyet':
                data = await getDonHangPheDuyet(filterType, startDate, endDate);
                reportTitle = 'Đơn hàng được phê duyệt';
                break;
                
            case 'khachHangMoiTao':
                data = await getKhachHangMoi(filterType, startDate, endDate);
                reportTitle = 'Khách hàng mới được tạo';
                break;
                
            case 'khachHangDaiLyMoi':
                data = await getKhachHangDaiLyMoi(filterType, startDate, endDate);
                reportTitle = 'Khách hàng đại lý mới';
                break;
                
            case 'khachHangBanGiao':
                data = await getKhachHangBanGiao(filterType, startDate, endDate);
                reportTitle = 'Khách hàng được bàn giao';
                break;
                
            case 'hoaHongKD':
                data = await getHoaHongKinhDoanh(filterType, startDate, endDate);
                reportTitle = 'Tổng hợp hoa hồng kinh doanh';
                break;
                
            case 'hoaHongQC':
                data = await getHoaHongQuangCao(filterType, startDate, endDate);
                reportTitle = 'Tổng hợp hoa hồng Quảng cáo truyền thông';
                break;
                
            case 'baiDangBanHang':
                data = await getBaiDangBanHang(filterType, startDate, endDate);
                reportTitle = 'Số lượng bài đăng bán hàng';
                break;
                
            case 'ketQuaChienDich':
                data = await getKetQuaChienDich(filterType, startDate, endDate);
                reportTitle = 'Kết quả chiến dịch quảng cáo';
                break;
                
            case 'chamSocKH':
                data = await getChamSocKhachHang(filterType, startDate, endDate, nhanVien);
                reportTitle = 'Báo cáo Chăm sóc khách hàng';
                break;
                
            case 'baoCaoKyThuat':
                data = await getBaoCaoKyThuat(filterType, startDate, endDate);
                reportTitle = 'Báo cáo Kỹ thuật - Thiết kế';
                break;
                
            case 'thucHienCV':
                data = await getBaoCaoThucHienCV(filterType, startDate, endDate);
                reportTitle = 'Báo cáo Thực hiện công việc';
                break;
                
            case 'chamCong':
                data = await getBaoCaoChamCong(filterType, startDate, endDate);
                reportTitle = 'Báo cáo số ngày làm việc (chấm công)';
                break;
                
            case 'khaoSat':
                data = await getBaoCaoKhaoSat(filterType, startDate, endDate);
                reportTitle = 'Báo cáo số ngày đi khảo sát công trình';
                break;
                
            case 'vanPhong':
                data = await getBaoCaoVanPhong(filterType, startDate, endDate);
                reportTitle = 'Báo cáo số ngày công làm việc tại văn phòng';
                break;
                
            default:
                // Tổng hợp nhiều báo cáo
                const [
                    baoGiaDonHang,
                    doanhSoTheoNV,
                    donHangHuy,
                    topKhachHang
                ] = await Promise.all([
                    getBaoCaoBaoGiaDonHang(filterType, startDate, endDate),
                    getDoanhSoTheoNhanVien(filterType, startDate, endDate),
                    getDonHangHuy(1, 5, filterType, startDate, endDate),
                    getTopKhachHang(filterType, startDate, endDate)
                ]);
                
                data = {
                    baoGiaDonHang,
                    doanhSoTheoNV,
                    donHangHuy,
                    topKhachHang
                };
                break;
        }
        
        res.render('BaocaoKPIphongkinhdoanh', {
            data,
            loaiBaoCao,
            filterType: filterType || 'none', // Đảm bảo có giá trị
            startDate,
            endDate,
            nhanVien,
            page: parseInt(page),
            reportTitle,
            nhanVienList: dsNhanVien,
            formatNumber: app.locals.formatNumber,
            formatCurrency: app.locals.formatCurrency,
            formatDate: app.locals.formatDate,
            formatDateTime: app.locals.formatDateTime,
            formatMonthYear: app.locals.formatMonthYear,
            formatQuarterYear: app.locals.formatQuarterYear
        });
        
    } catch (error) {
        console.error('Lỗi khi xử lý báo cáo KPI:', error);
        res.status(500).send('Lỗi server khi xử lý báo cáo');
    }
});

// Route xuất Excel
app.get('/export-excel-kpi', async (req, res) => {
    try {
        const { filterType = 'month', startDate, endDate } = req.query;
        
        // Tạo workbook mới
        const workbook = new ExcelJS.Workbook();
        workbook.creator = 'Hệ thống KPI Phòng Kinh Doanh';
        workbook.created = new Date();
        
        // Lấy tất cả dữ liệu báo cáo
        const reports = await Promise.all([
            getBaoCaoBaoGiaDonHang(filterType, startDate, endDate),
            getDoanhSoTheoNhanVien(filterType, startDate, endDate),
            getTopKhachHang(filterType, startDate, endDate),
            getKetQuaChienDich(filterType, startDate, endDate),
            getBaoCaoKyThuat(filterType, startDate, endDate),
            getBaoCaoChamCong(filterType, startDate, endDate)
        ]);
        
        // Sheet 1: Báo cáo báo giá & đơn hàng
        const sheet1 = workbook.addWorksheet('Báo giá & Đơn hàng');
        sheet1.columns = [
            { header: 'Nhân viên', key: 'nhanVien', width: 30 },
            { header: 'Tổng báo giá', key: 'tongBaoGia', width: 15 },
            { header: 'Tổng đơn hàng', key: 'tongDonHang', width: 15 },
            { header: 'Tỷ lệ chuyển đổi (%)', key: 'tyLeChuyenDoi', width: 20 },
            { header: 'Ghi chú', key: 'ghiChu', width: 40 }
        ];
        
        Object.entries(reports[0]).forEach(([nhanVien, data]) => {
            sheet1.addRow({
                nhanVien,
                tongBaoGia: data.tongBaoGia,
                tongDonHang: data.tongDonHang,
                tyLeChuyenDoi: data.tyLeChuyenDoi.toFixed(2),
                ghiChu: ''
            });
        });
        
        // Sheet 2: Doanh số theo nhân viên
        const sheet2 = workbook.addWorksheet('Doanh số');
        sheet2.columns = [
            { header: 'Nhân viên', key: 'nhanVien', width: 30 },
            { header: 'Doanh số thực tế', key: 'doanhSoThucTe', width: 20, style: { numFmt: '#,##0' } },
            { header: 'KPI mục tiêu', key: 'kpiMucTieu', width: 20, style: { numFmt: '#,##0' } },
            { header: 'Tỷ lệ hoàn thành (%)', key: 'tyLeHoanThanh', width: 20 },
            { header: 'Đánh giá', key: 'danhGia', width: 15 },
            { header: 'Ghi chú', key: 'ghiChu', width: 40 }
        ];
        
        reports[1].forEach(item => {
            sheet2.addRow({
                nhanVien: item.tenNhanVien,
                doanhSoThucTe: item.doanhSoThucTe,
                kpiMucTieu: item.kpiMucTieu,
                tyLeHoanThanh: item.tyLeHoanThanh.toFixed(2),
                danhGia: item.danhGia,
                ghiChu: ''
            });
        });
        
        // Sheet 3: Top khách hàng
        const sheet3 = workbook.addWorksheet('Top khách hàng');
        sheet3.columns = [
            { header: 'Khách hàng ID', key: 'khachHangID', width: 20 },
            { header: 'Tên khách hàng', key: 'tenKhach', width: 30 },
            { header: 'Số đơn hàng', key: 'soDonHang', width: 15 },
            { header: 'Tổng doanh số', key: 'tongDoanhSo', width: 20, style: { numFmt: '#,##0' } },
            { header: 'Ghi chú', key: 'ghiChu', width: 40 }
        ];
        
        reports[2].forEach(item => {
            sheet3.addRow({
                khachHangID: item.khachHangID,
                tenKhach: item.tenKhach,
                soDonHang: item.soDonHang,
                tongDoanhSo: item.tongDoanhSo,
                ghiChu: ''
            });
        });
        
        // Sheet 4: Chiến dịch quảng cáo
        const sheet4 = workbook.addWorksheet('Chiến dịch QC');
        sheet4.columns = [
            { header: 'Mã chiến dịch', key: 'maChienDich', width: 20 },
            { header: 'Tên chiến dịch', key: 'tenChienDich', width: 30 },
            { header: 'Kênh chạy', key: 'kenhChay', width: 15 },
            { header: 'Chi phí dự kiến', key: 'chiPhiDuKien', width: 20, style: { numFmt: '#,##0' } },
            { header: 'Chi phí thực tế', key: 'chiPhiThucTe', width: 20, style: { numFmt: '#,##0' } },
            { header: 'Số LEAD', key: 'soLead', width: 15 },
            { header: 'Tỷ lệ tiêu thu (%)', key: 'tyLeTieuThu', width: 20 },
            { header: 'Đánh giá', key: 'danhGia', width: 20 },
            { header: 'Ghi chú', key: 'ghiChu', width: 40 }
        ];
        
        reports[3].forEach(item => {
            sheet4.addRow({
                maChienDich: item.maChienDich,
                tenChienDich: item.tenChienDich,
                kenhChay: item.kenhChay,
                chiPhiDuKien: item.chiPhiDuKien,
                chiPhiThucTe: item.chiPhiThucTe,
                soLead: item.soLead,
                tyLeTieuThu: item.tyLeTieuThu.toFixed(2),
                danhGia: item.danhGia,
                ghiChu: ''
            });
        });
        
        // Định dạng header
        [sheet1, sheet2, sheet3, sheet4].forEach(sheet => {
            sheet.getRow(1).font = { bold: true };
            sheet.getRow(1).fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: 'FF0070C0' }
            };
            sheet.getRow(1).alignment = { vertical: 'middle', horizontal: 'center' };
        });
        
        // Ghi file
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', 'attachment; filename=bao-cao-kpi.xlsx');
        
        await workbook.xlsx.write(res);
        res.end();
        
    } catch (error) {
        console.error('Lỗi khi xuất Excel:', error);
        res.status(500).send('Lỗi khi xuất file Excel');
    }
});

// Route cho biên bản cuộc họp (Mục 5)
app.get('/bien-ban-cuoc-hop', async (req, res) => {
    try {
        // Lấy danh sách nhân viên - ĐỔI TÊN BIẾN
        const dsNhanVien = await getNhanVienList(); // Đổi tên biến
        
        res.render('BienBanCuocHop', {
            title: 'Biên bản đánh giá hiện trạng tồn tại',
            nhanVienList: dsNhanVien || [] // Vẫn truyền với tên cũ cho EJS
        });
    } catch (error) {
        console.error('Lỗi khi tải trang biên bản cuộc họp:', error);
        res.status(500).send('Lỗi server khi tải trang biên bản cuộc họp');
    }
});


app.post('/luu-bien-ban', (req, res) => {
    try {
        const bienBanData = req.body;
        // Lưu biên bản vào database hoặc file
        // Ở đây có thể lưu vào Google Sheets hoặc database
        res.json({ success: true, message: 'Đã lưu biên bản thành công' });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Lỗi khi lưu biên bản' });
    }
});


//// LÀM THANH TOÁN KHOÁN LẮP ĐẶT
// Route làm thanh toán lắp đặt
app.get("/lamthanhtoanlapdat", async (req, res) => {
    try {
        // Lấy danh sách mã đơn hàng từ sheet Don_hang
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A:AC",
        });

        const data = response.data.values || [];
        const maDonHangList = [];

        // Lọc mã đơn hàng có AC = "Lắp đặt" hoặc "Sửa chữa"
        for (let i = 1; i < data.length; i++) {
            const row = data[i];
            const maDonHang = row[6]; // Cột G
            const loaiDonHang = row[28]; // Cột AC
            
            if (maDonHang && (loaiDonHang === "Lắp đặt" || loaiDonHang === "Sửa chữa")) {
                maDonHangList.push(maDonHang);
            }
        }

        res.render("lamthanhtoanlapdat", {
            maDonHangList: maDonHangList
        });

    } catch (error) {
        console.error("❌ Lỗi khi lấy danh sách đơn hàng:", error);
        res.status(500).send("Lỗi server khi lấy dữ liệu");
    }
});

// API lấy thông tin đơn hàng thanh toán khoán lắp đặt
app.get("/api/donhang/:maDonHang", async (req, res) => {
    try {
        const { maDonHang } = req.params;

        // Lấy dữ liệu từ các sheet
        const [donHangRes, pvcRes, nkRes] = await Promise.all([
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang!A:AC",
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang_PVC_ct!A:W",
            }),
            sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: "Don_hang_nk_ct!A:P",
            })
        ]);

        const donHangData = donHangRes.data.values || [];
        const pvcData = pvcRes.data.values || [];
        const nkData = nkRes.data.values || [];

        // Tìm thông tin đơn hàng
        let thongTinDonHang = null;
        for (let i = 1; i < donHangData.length; i++) {
            const row = donHangData[i];
            if (row[6] === maDonHang) { // Cột G
                thongTinDonHang = {
                    tenKhachHang: row[9] || '', // Cột J
                    diaChiThucHien: row[20] || '', // Cột U
                    tenNguoiLienHe: row[17] || '', // Cột R
                    nhomSanPham: row[26] || '', // Cột AA
                    loaiDonHang: row[28] || '' // Cột AC
                };
                break;
            }
        }

        if (!thongTinDonHang) {
            return res.status(404).json({ error: "Không tìm thấy đơn hàng" });
        }

        // Lấy danh sách sản phẩm
        let danhSachSanPham = [];
        const nhomSanPham = thongTinDonHang.nhomSanPham;

        if (nhomSanPham !== "NK" && nhomSanPham !== "PKNK") {
            // Tìm trong sheet Don_hang_PVC_ct
            for (let i = 1; i < pvcData.length; i++) {
                const row = pvcData[i];
                if (row[1] === maDonHang) { // Cột B
                    danhSachSanPham.push({
                        tenSanPham: row[8] || '', // Cột I
                        dai: row[16] || '', // Cột Q
                        rong: row[17] || '', // Cột R
                        cao: row[18] || '', // Cột S
                        soLuong: row[21] || '', // Cột V
                        donViTinh: row[22] || '' // Cột W
                    });
                }
            }
        } else {
            // Tìm trong sheet Don_hang_nk_ct
            for (let i = 1; i < nkData.length; i++) {
                const row = nkData[i];
                if (row[1] === maDonHang) { // Cột B
                    danhSachSanPham.push({
                        tenSanPham: row[8] || '', // Cột I
                        dai: row[9] || '', // Cột J
                        rong: row[10] || '', // Cột K
                        cao: row[11] || '', // Cột L
                        soLuong: row[14] || '', // Cột O
                        donViTinh: row[13] || '' // Cột N
                    });
                }
            }
        }

        res.json({
            thongTinDonHang,
            danhSachSanPham
        });

    } catch (error) {
        console.error("❌ Lỗi khi lấy thông tin đơn hàng:", error);
        res.status(500).json({ error: "Lỗi server" });
    }
});

// Route xuất Excel làm thanh toán khoán lắp đặt
app.post("/export/lamthanhtoanlapdat", async (req, res) => {
    try {
        const { donHangData } = req.body;
        
        const workbook = new exceljs.Workbook();
        workbook.creator = 'Hệ thống thanh toán lắp đặt';
        workbook.created = new Date();

        // Tạo sheet cho từng đơn hàng
        for (const donHang of donHangData) {
            const sheet = workbook.addWorksheet(donHang.maDonHang);
            
            // Tiêu đề
            sheet.mergeCells('A1:L1');
            const title = sheet.getCell('A1');
            title.value = `THANH TOÁN LẮP ĐẶT - ${donHang.maDonHang}`;
            title.font = { bold: true, size: 16, color: { argb: 'FFFFFF' } };
            title.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: '4472C4' }
            };
            title.alignment = { horizontal: 'center', vertical: 'middle' };

            // Thông tin đơn hàng
            sheet.getCell('A2').value = 'Thông tin đơn hàng:';
            sheet.getCell('A2').font = { bold: true };
            
            sheet.getCell('A3').value = 'Tên khách hàng:';
            sheet.getCell('B3').value = donHang.tenKhachHang;
            
            sheet.getCell('A4').value = 'Địa chỉ thực hiện:';
            sheet.getCell('B4').value = donHang.diaChiThucHien;
            
            sheet.getCell('A5').value = 'Tên người liên hệ:';
            sheet.getCell('B5').value = donHang.tenNguoiLienHe;
            
            sheet.getCell('A6').value = 'Nhóm sản phẩm:';
            sheet.getCell('B6').value = donHang.nhomSanPham;

            // Header bảng
            const headers = ['STT', 'Tên sản phẩm', 'Dài (mm)', 'Rộng (mm)', 'Cao (mm)', 
                           'Diện tích (m²)', 'Số lượng', 'Tổng số lượng (m²)', 
                           'Đơn vị tính', 'Đơn giá (VNĐ)', 'Thành tiền (VNĐ)', 'Ghi chú'];
            
            const headerRow = sheet.getRow(8);
            headerRow.values = headers;
            headerRow.eachCell((cell, colNumber) => {
                cell.font = { bold: true, color: { argb: 'FFFFFF' } };
                cell.fill = {
                    type: 'pattern',
                    pattern: 'solid',
                    fgColor: { argb: '5B9BD5' }
                };
                cell.border = {
                    top: { style: 'thin' },
                    left: { style: 'thin' },
                    bottom: { style: 'thin' },
                    right: { style: 'thin' }
                };
                cell.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
            });

            // Dữ liệu sản phẩm
            let rowIndex = 9;
            let tongThanhTien = 0;

            donHang.danhSachSanPham.forEach((sp, index) => {
                // Lấy giá trị
                const dai = parseFloat(sp.dai) || 0;
                const rong = parseFloat(sp.rong) || 0;
                const cao = parseFloat(sp.cao) || 0;
                const soLuong = parseFloat(sp.soLuong) || 0;
                const donGia = parseFloat(sp.donGia) || 0;
                const donViTinh = (sp.donViTinh || '').toLowerCase(); // THÊM DÒNG NÀY
                
                // Tính diện tích: chia cho 1,000,000
                let dienTich = 0;
                if (cao === 0) {
                    dienTich = (dai * rong) / 1000000;
                } else {
                    dienTich = (rong * cao) / 1000000;
                }
                
                // Tính tổng số lượng
                const tongSoLuong = dienTich * soLuong;
                
                // Tính thành tiền theo đơn vị
                let thanhTien = 0;
                if (donViTinh === 'm2') {
                    thanhTien = donGia * tongSoLuong;
                } else {
                    thanhTien = donGia * soLuong;
                }
                
                tongThanhTien += thanhTien;

                const row = sheet.getRow(rowIndex);
                
                row.values = [
                    index + 1,
                    sp.tenSanPham,
                    dai,
                    rong,
                    cao,
                    dienTich,
                    soLuong,
                    tongSoLuong,
                    sp.donViTinh || '',
                    donGia,
                    thanhTien,
                    sp.ghiChu || ''
                ];

                // Style cho dòng
                row.eachCell((cell, colNumber) => {
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    
                    // Định dạng số
                    if (colNumber >= 3 && colNumber <= 5) { // Dài, Rộng, Cao (chỉnh sửa cột này)
                        cell.numFmt = '#,##0';
                        cell.alignment = { horizontal: 'right' };
                    } else if (colNumber === 6) { // Diện tích
                        cell.numFmt = '#,##0.000';
                        cell.alignment = { horizontal: 'right' };
                    } else if (colNumber === 7) { // Số lượng
                        cell.numFmt = '#,##0';
                        cell.alignment = { horizontal: 'right' };
                    } else if (colNumber === 8) { // Tổng số lượng
                        cell.numFmt = '#,##0.000';
                        cell.alignment = { horizontal: 'right' };
                    } else if (colNumber === 10 || colNumber === 11) { // Đơn giá và thành tiền
                        cell.numFmt = '#,##0';
                        cell.alignment = { horizontal: 'right' };
                    }
                    
                    // Màu xen kẽ
                    if (rowIndex % 2 === 0) {
                        cell.fill = {
                            type: 'pattern',
                            pattern: 'solid',
                            fgColor: { argb: 'F2F2F2' }
                        };
                    }
                });

                rowIndex++;
            });

            // Dòng tổng
            const totalRow = sheet.getRow(rowIndex);
            totalRow.getCell(10).value = 'TỔNG CỘNG:';
            totalRow.getCell(11).value = tongThanhTien;
            
            totalRow.eachCell((cell, colNumber) => {
                if (colNumber >= 10) {
                    cell.font = { bold: true };
                    cell.fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'C6EFCE' }
                    };
                    cell.border = {
                        top: { style: 'thin' },
                        left: { style: 'thin' },
                        bottom: { style: 'thin' },
                        right: { style: 'thin' }
                    };
                    if (colNumber === 11) {
                        cell.numFmt = '#,##0';
                    }
                }
            });

            // Auto fit columns
            sheet.columns.forEach((column, index) => {
                if (index === 1) column.width = 30; // Tên sản phẩm
                else if (index === 11) column.width = 25; // Ghi chú
                else column.width = 15;
            });
        }

        // Thiết lập response
        const fileName = `Thong-ke-thanh-toan-lap-dat-${new Date().toISOString().split('T')[0]}.xlsx`;
        
        res.setHeader(
            'Content-Type',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        );
        res.setHeader(
            'Content-Disposition',
            `attachment; filename=${encodeURIComponent(fileName)}`
        );

        await workbook.xlsx.write(res);
        res.end();

    } catch (error) {
        console.error("❌ Lỗi khi xuất Excel thanh toán lắp đặt:", error);
        res.status(500).json({ error: "Lỗi khi xuất file Excel" });
    }
});


////THÊM NHÂN SỰ ĐƠN HÀNG VÀO SHETT TT_KHOAN_LAP_DAT

/// webhook (đặt sau các import và trước phần khởi động server)
// Middleware để xác thực webhook
const authenticateWebhook = (req, res, next) => {
    // Lấy token từ các nguồn khác nhau
    const tokenSources = [
        req.headers['x-auth-token'],
        req.headers['authorization']?.replace('Bearer ', ''),
        req.query.token,
        req.body?.token // Nếu gửi trong body
    ];
    
    const authToken = tokenSources.find(t => t !== undefined && t !== '');
    const expectedToken = process.env.WEBHOOK_AUTH_TOKEN;
    
    // Nếu không có token trong env, cho phép tất cả (chỉ dùng cho dev)
    if (!expectedToken) {
        console.warn('⚠️  Cảnh báo: WEBHOOK_AUTH_TOKEN chưa được cấu hình');
        return next();
    }
    
    // Nếu có token nhưng không khớp
    if (authToken !== expectedToken) {
        console.warn(`❌ Token không hợp lệ. Nhận được: ${authToken ? authToken.substring(0, 10) + '...' : 'null'}`);
        return res.status(401).json({
            success: false,
            message: 'Unauthorized: Invalid or missing authentication token',
            hint: 'Include token in header: X-Auth-Token or query parameter: ?token=YOUR_TOKEN'
        });
    }
    
    // Token hợp lệ
    console.log('✅ Token xác thực thành công');
    next();
};

// Thêm route GET để test 
app.get('/webhook/import-khoan-lap-dat', async (req, res) => {
    try {
        console.log('📥 GET request received from AppSheet (test)');
        
        // Kiểm tra token từ query parameter
        const authToken = req.query.token || req.headers['x-auth-token'];
        const expectedToken = process.env.WEBHOOK_AUTH_TOKEN;
        
        if (expectedToken && authToken !== expectedToken) {
            return res.status(401).json({
                success: false,
                message: 'Unauthorized: Invalid token',
                hint: 'Token provided: ' + (authToken ? 'yes' : 'no')
            });
        }
        
        // Trả về thông báo thành công cho GET request
        res.status(200).json({
            success: true,
            message: 'Webhook endpoint is active',
            method: 'GET',
            timestamp: new Date().toISOString(),
            expected_method: 'POST (for actual import)'
        });
        
    } catch (error) {
        console.error('❌ Error in GET handler:', error);
        res.status(500).json({
            success: false,
            message: 'Server error',
            error: error.message
        });
    }
});

// Route POST chính 
app.post('/webhook/import-khoan-lap-dat', express.json(), async (req, res) => {
    try {
        console.log('📥 POST request received from AppSheet');
        
        // Kiểm tra token
        const authToken = req.query.token || req.headers['x-auth-token'] || req.body?.token;
        const expectedToken = process.env.WEBHOOK_AUTH_TOKEN;
        
        if (expectedToken && authToken !== expectedToken) {
            console.log('❌ Token mismatch. Expected:', expectedToken, 'Got:', authToken);
            return res.status(401).json({
                success: false,
                message: 'Unauthorized: Invalid token'
            });
        }
        
        console.log('✅ Token verified, starting import...');
        
        // Gọi hàm import
        const result = await importLastRowWithCoefficients();
        
        res.status(200).json({
            success: true,
            message: 'Data imported successfully',
            timestamp: new Date().toISOString(),
            rows_processed: result || 'unknown'
        });
        
    } catch (error) {
        console.error('❌ Error in POST handler:', error);
        res.status(500).json({
            success: false,
            message: 'Error importing data',
            error: error.message,
            stack: error.stack
        });
    }
});
/// hàm xử lý dữ liệu để ghi nhân sự và đơn hàng vào sheet TT_KHOAN_LAP_DAT
async function importLastRowWithCoefficients() {
    try {
        console.log('🚀 Starting import function...');
        
        const SPREADSHEET_HC_ID = process.env.SPREADSHEET_HC_ID;
        const SHEET1_NAME = 'danh_sach_don_tra_khoan_lap_dat';
        const SHEET2_NAME = 'TT_khoan_lap_dat';
        const SHEET3_NAME = 'Data_he_so_khoan_lap_dat';
        const SHEET4_NAME = 'Nhan_vien';

        // 1. Lấy dữ liệu nhân viên
        const nhanVienResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: `${SHEET4_NAME}!A:B`,
        });
        
        const nhanVienData = nhanVienResponse.data.values || [];
        const nvMap = {};
        
        for (let i = 1; i < nhanVienData.length; i++) {
            const row = nhanVienData[i];
            if (row && row.length >= 2) {
                const maNV = row[0] || '';
                const tenNV = row[1] || '';
                if (tenNV) {
                    nvMap[tenNV.toString().trim().toLowerCase()] = maNV;
                }
            }
        }
        
        console.log(`✅ Loaded ${Object.keys(nvMap).length} employee records`);

        // 2. Lấy hệ số
        const heSoResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: `${SHEET3_NAME}!A:E`,
        });
        
        const data3 = heSoResponse.data.values || [];
        const hsMap = {};
        
        for (let i = 1; i < data3.length; i++) {
            const row3 = data3[i];
            if (row3 && row3.length >= 5) {
                const key = row3[1] || '';
                const val = row3[4] || '';
                if (key) {
                    hsMap[key.toString().trim()] = val;
                }
            }
        }

        // 3. Lấy dữ liệu sheet1
        const sheet1Response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: SHEET1_NAME,
        });
        
        const sheet1Data = sheet1Response.data.values || [];
        
        if (sheet1Data.length < 2) {
            console.log('⚠️ No data in sheet1');
            return 0;
        }

        // Lấy dòng cuối cùng
        const lastRowValues = sheet1Data[sheet1Data.length - 1];
        
        // Ánh xạ cột (bắt đầu từ index 0)
        const colB = lastRowValues[1] || '';  // Cột B
        const colC = lastRowValues[2] || '';  // Cột C
        const colD = lastRowValues[3] || '';  // Cột D
        const colJ = lastRowValues[9] || '';  // Cột J
        const colK = lastRowValues[10] || ''; // Cột K

        console.log('📊 Last row data:', { 
            colB, 
            colC, 
            colD, 
            colJ, 
            colK,
            hasColK: !!colK && colK.trim() !== ''
        });

        // 4. Lấy dòng hiện tại trong sheet2
        const sheet2Response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_HC_ID,
            range: `${SHEET2_NAME}!A:M`,
        });
        
        const sheet2Data = sheet2Response.data.values || [];
        const startRow = sheet2Data.length + 1;

        const rowsToWrite = [];

        // Helper function
        const getMaNVFromTen = (ten) => {
            if (!ten || typeof ten !== 'string') return '';
            return nvMap[ten.trim().toLowerCase()] || '';
        };

        // 5.1 Chủ nhiệm (LUÔN LUÔN CÓ)
        const maChuNhiem = getMaNVFromTen(colJ);
        rowsToWrite.push([
            uuidv4(),
            colJ || '',  // Đảm bảo không bị undefined
            maChuNhiem,
            colC || '',
            colD || '',
            'Chủ nhiệm',
            '1,20',
            '', '', '', '', '',
            colB || ''
        ]);
        console.log(`👨‍💼 Chủ nhiệm: "${colJ}" - Mã: "${maChuNhiem}"`);

        // 5.2 Hỗ trợ (CHỈ KHI colK CÓ DỮ LIỆU)
        if (colK && typeof colK === 'string' && colK.trim() !== '') {
            // Tách danh sách người hỗ trợ
            const persons = colK.split(/\s*,\s*/).filter(p => p.trim() !== '');
            console.log(`👥 Danh sách hỗ trợ: ${persons.length} người`, persons);
            
            persons.forEach((p) => {
                const trimmedP = p.trim();
                const coeff = hsMap[trimmedP] !== undefined ? hsMap[trimmedP] : '';
                const maHoTro = getMaNVFromTen(trimmedP);
                
                rowsToWrite.push([
                    uuidv4(),
                    trimmedP,
                    maHoTro,
                    colC || '',
                    colD || '',
                    'Hỗ trợ',
                    coeff,
                    '', '', '', '', '',
                    colB || ''
                ]);
                
                console.log(`   👤 "${trimmedP}" - Mã: "${maHoTro}" - HS: "${coeff}"`);
            });
        } else {
            console.log('👥 Không có người hỗ trợ (colK rỗng)');
        }

        // 6. Ghi dữ liệu
        if (rowsToWrite.length > 0) {
            console.log(`✍️ Writing ${rowsToWrite.length} rows to ${SHEET2_NAME} starting at row ${startRow}...`);
            
            try {
                await sheets.spreadsheets.values.append({
                    spreadsheetId: SPREADSHEET_HC_ID,
                    range: `${SHEET2_NAME}!A${startRow}`,
                    valueInputOption: 'USER_ENTERED',
                    insertDataOption: 'INSERT_ROWS',
                    resource: { values: rowsToWrite }
                });
                
                console.log('✅ Import completed successfully');
                return rowsToWrite.length;
            } catch (writeError) {
                console.error('❌ Error writing to sheet:', writeError);
                throw writeError;
            }
        } else {
            console.log('⚠️ No rows to write');
            return 0;
        }
        
    } catch (error) {
        console.error('❌ Error in import function:', error);
        // Log chi tiết hơn để debug
        if (error.response) {
            console.error('Google API Error:', {
                status: error.response.status,
                data: error.response.data
            });
        }
        throw error;
    }
}

// --- Start server ---
app.listen(PORT, () => console.log(`✅ Server is running on port ${PORT}`));