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

if (!SPREADSHEET_ID || !SPREADSHEET_HC_ID ||!GAS_WEBAPP_URL || !GAS_WEBAPP_URL_BBNT || !GOOGLE_CREDENTIALS_B64 || !GAS_WEBAPP_URL_BBSV || !GAS_WEBAPP_URL_DNC) {
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
let pushSubscriptions = [];

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



// --- Routes ---
app.get("/", (_req, res) => res.send("🚀 Server chạy ổn!"));

//---bbgn----
app.get("/bbgn", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBGN ...");

        // --- Lấy mã đơn hàng ---
        const bbgnRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "file_BBGN_ct!B:B",
        });
        const colB = bbgnRes.data.values ? bbgnRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet file_BBGN_ct.");

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
        res.render("bbgn", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            pathToFile: ""
        });

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbgn.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        pathToFile: ""
                    }
                );

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
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `file_BBGN_ct!D${lastRowWithData}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log("✔️ Đã ghi đường dẫn:", pathToFile);

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBGN:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//---bbnt----
app.get("/bbnt", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBNT ...");

        // --- Lấy mã đơn hàng ---
        const bbntRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "File_BBNT_ct!B:B",
        });
        const colB = bbntRes.data.values ? bbntRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet File_BBNT_ct.");

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
            pathToFile: ""
        });

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
                const renderedHtml = await renderFileAsync(
                    path.join(__dirname, "views", "bbnt.ejs"),
                    {
                        donHang,
                        products,
                        logoBase64,
                        watermarkBase64,
                        autoPrint: false,
                        maDonHang,
                        pathToFile: ""
                    }
                );

                const resp = await fetch(GAS_WEBAPP_URL_BBNT, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: new URLSearchParams({
                        orderCode: maDonHang,
                        html: renderedHtml
                    })
                });

                const data = await resp.json();
                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `BBNT/${data.fileName}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `File_BBNT_ct!D${lastRowWithData}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log("✔️ Đã ghi đường dẫn:", pathToFile);

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi chi tiết:", err);
        res.status(500).send("Lỗi server: " + err.message);
    }
});

//---ggh---
app.get("/ggh", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất GGH ...");

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
        res.render("ggh", {
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
                    path.join(__dirname, "views", "ggh.ejs"),
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

//---lenhpvc-----
app.get("/lenhpvc", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Lệnh PVC ...");
        await new Promise(resolve => setTimeout(resolve, 4000));

        // --- Lấy mã đơn hàng ---
        const lenhRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "File_lenh_ct!B:B",
        });
        const colB = lenhRes.data.values ? lenhRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet File_lenh_ct.");

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

        // --- Lấy chi tiết sản phẩm PVC ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_PVC_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        // Lọc và map dữ liệu theo cấu trúc của lệnh sản xuất
        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r, i) => ({
                maDonHangChiTiet: r[2],
                tenThuongMai: r[9],
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
        const lenhValue = donHang[36] || '';

        // --- Render ngay cho client ---
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

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
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

                // Gọi GAS webapp tương ứng (cần thêm biến môi trường GAS_WEBAPP_URL_LENHPVC)
                const GAS_WEBAPP_URL_LENHPVC = process.env.GAS_WEBAPP_URL_LENHPVC;
                if (GAS_WEBAPP_URL_LENHPVC) {
                    const resp = await fetch(GAS_WEBAPP_URL_LENHPVC, {
                        method: "POST",
                        headers: { "Content-Type": "application/x-www-form-urlencoded" },
                        body: new URLSearchParams({
                            orderCode: maDonHang,
                            html: renderedHtml
                        })
                    });

                    const data = await resp.json();
                    console.log("✔️ AppScript trả về:", data);

                    const pathToFile = data.pathToFile || `LENH_PVC/${data.fileName}`;
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `File_lenh_ct!D${lastRowWithData}`,
                        valueInputOption: "RAW",
                        requestBody: { values: [[pathToFile]] },
                    });
                    console.log("✔️ Đã ghi đường dẫn:", pathToFile);
                } else {
                    console.log("⚠️ Chưa cấu hình GAS_WEBAPP_URL_LENHPVC");
                }

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất Lệnh PVC:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//---baogiapvc----
app.get("/baogiapvc", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Báo Giá PVC ...");
        console.log("📘 SPREADSHEET_ID:", process.env.SPREADSHEET_ID);
        await new Promise(resolve => setTimeout(resolve, 2500));
        // --- Lấy mã đơn hàng ---
        const baoGiaRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "File_bao_gia_ct!B:B",
        });
        const colB = baoGiaRes.data.values ? baoGiaRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet File_bao_gia_ct.");

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

        // --- Lấy chi tiết sản phẩm PVC ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_PVC_ct!A1:AC",
        });
        const ctRows = (ctRes.data.values || []).slice(1);

        // Lọc và map dữ liệu theo cấu trúc của báo giá
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

        // --- Tính tổng các giá trị ---
        let tongTien = 0;
        let chietKhau = parseFloat(donHang[40]) || 0;
        let tamUng = parseFloat(donHang[41]) || 0;

        products.forEach(product => {
            tongTien += parseFloat(product.thanhTien) || 0;
        });

        let tongThanhTien = tongTien - chietKhau - tamUng;

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render ngay cho client ---
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

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
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

                // Gọi GAS webapp tương ứng (cần thêm biến môi trường GAS_WEBAPP_URL_BAOGIA)
                const GAS_WEBAPP_URL_BAOGIAPVC = process.env.GAS_WEBAPP_URL_BAOGIAPVC;
                if (GAS_WEBAPP_URL_BAOGIAPVC) {
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
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `File_bao_gia_ct!D${lastRowWithData}`,
                        valueInputOption: "RAW",
                        requestBody: { values: [[pathToFile]] },
                    });
                    console.log("✔️ Đã ghi đường dẫn:", pathToFile);
                } else {
                    console.log("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BAOGIA");
                }

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất Báo Giá PVC:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//----baogiank----
app.get("/baogiank", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Báo Giá Nhôm Kính ...");

        // --- Lấy mã đơn hàng ---
        const baoGiaRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "File_bao_gia_ct!B:B",
        });
        const colB = baoGiaRes.data.values ? baoGiaRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet File_bao_gia_ct.");

        console.log(`✔️ Mã đơn hàng: ${maDonHang} (dòng ${lastRowWithData})`);

        // --- Lấy đơn hàng ---
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BW", // Mở rộng đến cột BW
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
            range: "Don_hang_nk_ct!A1:U", // Mở rộng đến cột U
        });
        const ctRows = (ctRes.data.values || []).slice(1);
        
        // Lọc và map dữ liệu theo cấu trúc của báo giá nhôm kính
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

        // --- Tính tổng các giá trị ---
        let tongTien = 0;
        let chietKhau = parseFloat(donHang[40]) || 0; // Cột AN
        let tamUng = parseFloat(donHang[41]) || 0; // Cột AO
        
        products.forEach(product => {
            tongTien += parseFloat(product.thanhTien) || 0;
        });

        let tongThanhTien = tongTien - chietKhau - tamUng;

        // Tính tổng diện tích và số lượng
        let tongDienTich = 0;
        let tongSoLuong = 0;
        
        products.forEach(product => {
            const dienTich = parseFloat(product.dienTich) || 0;
            const soLuong = parseFloat(product.soLuong) || 0;
            tongDienTich += dienTich * soLuong;
            tongSoLuong += soLuong;
        });

        tongDienTich = parseFloat(tongDienTich.toFixed(2));

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64('1766zFeBWPEmjTGQGrrtM34QFbV8fHryb'); // Watermark ID từ code GAS

        // --- Render ngay cho client ---
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
            pathToFile: ""
        });

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
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
                        pathToFile: ""
                    }
                );

                // Gọi GAS webapp tương ứng (cần thêm biến môi trường GAS_WEBAPP_URL_BAOGIANK)
                const GAS_WEBAPP_URL_BAOGIANK = process.env.GAS_WEBAPP_URL_BAOGIANK;
                if (GAS_WEBAPP_URL_BAOGIANK) {
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
                        range: `File_bao_gia_ct!D${lastRowWithData}`,
                        valueInputOption: "RAW",
                        requestBody: { values: [[pathToFile]] },
                    });
                    console.log("✔️ Đã ghi đường dẫn:", pathToFile);
                } else {
                    console.log("⚠️ Chưa cấu hình GAS_WEBAPP_URL_BAOGIANK");
                }

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất Báo Giá Nhôm Kính:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//----lenhnk------------
app.get("/lenhnk", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất Lệnh Nhôm Kính ...");

        // --- Lấy mã đơn hàng ---
        const lenhRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "File_lenh_ct!B:B",
        });
        const colB = lenhRes.data.values ? lenhRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet File_lenh_ct.");

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

        // --- Lấy chi tiết sản phẩm Nhôm Kính ---
        const ctRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang_nk_ct!A1:U",
        });
        const ctRows = (ctRes.data.values || []).slice(1);
        
        // Lọc và map dữ liệu theo cấu trúc của lệnh sản xuất nhôm kính
        const products = ctRows
            .filter((r) => r[1] === maDonHang)
            .map((r, i) => ({
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
        const lenhValue = donHang[36] || '';

        // --- Render ngay cho client ---
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

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
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

                // Gọi GAS webapp tương ứng (cần thêm biến môi trường GAS_WEBAPP_URL_LENHNK)
                const GAS_WEBAPP_URL_LENHNK = process.env.GAS_WEBAPP_URL_LENHNK;
                if (GAS_WEBAPP_URL_LENHNK) {
                    const resp = await fetch(GAS_WEBAPP_URL_LENHNK, {
                        method: "POST",
                        headers: { "Content-Type": "application/x-www-form-urlencoded" },
                        body: new URLSearchParams({
                            orderCode: maDonHang,
                            html: renderedHtml
                        })
                    });

                    const data = await resp.json();
                    console.log("✔️ AppScript trả về:", data);

                    const pathToFile = data.pathToFile || `LENH_NK/${data.fileName}`;
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `File_lenh_ct!D${lastRowWithData}`,
                        valueInputOption: "RAW",
                        requestBody: { values: [[pathToFile]] },
                    });
                    console.log("✔️ Đã ghi đường dẫn:", pathToFile);
                } else {
                    console.log("⚠️ Chưa cấu hình GAS_WEBAPP_URL_LENHNK");
                }

            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất Lệnh Nhôm Kính:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//-----bbgnnk----
app.get("/bbgnnk", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBGN NK ...");

        // --- Lấy mã đơn hàng ---
        const bbgnnkRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "file_BBGN_ct!B:B",
        });
        const colB = bbgnnkRes.data.values ? bbgnnkRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang) {
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet file_BBGN_ct.");
        }

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
                ghiChu: " ",
            }));

        console.log(`✔️ Tìm thấy ${products.length} sản phẩm.`);

        // --- Logo & Watermark ---
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // --- Render ngay cho client ---
        res.render("bbgnnk", {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            pathToFile: "",
        });

        // --- Sau khi render xong thì gọi AppScript ngầm ---
        (async () => {
            try {
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
                if (GAS_WEBAPP_URL_BBGNNK) {
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
                    await sheets.spreadsheets.values.update({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `file_BBGN_ct!D${lastRowWithData}`,
                        valueInputOption: "RAW",
                        requestBody: { values: [[pathToFile]] },
                    });
                    console.log("✔️ Đã ghi đường dẫn:", pathToFile);
                }
            } catch (err) {
                console.error("❌ Lỗi gọi AppScript:", err);
            }
        })();

    } catch (err) {
        console.error("❌ Lỗi khi xuất BBGN NK:", err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
});

//---bbntnk----
app.get("/bbntnk", async (req, res) => {
  try {
    console.log("▶️ Bắt đầu xuất BBNTNK ...");

    // 1. Lấy mã đơn hàng từ sheet file_BBNT_ct
    const bbntRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "file_BBNT_ct!B:B",
    });
    const colB = bbntRes.data.values ? bbntRes.data.values.flat() : [];
    const lastRowWithData = colB.length;
    const maDonHang = colB[lastRowWithData - 1];
    if (!maDonHang) return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet file_BBNT_ct.");

    console.log(`✔️ Mã đơn hàng: ${maDonHang} (dòng ${lastRowWithData})`);

    // 2. Lấy đơn hàng
    const donHangRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Don_hang!A1:BJ",
    });
    const rows = donHangRes.data.values || [];
    const data = rows.slice(1);
    const donHang =
      data.find((r) => r[5] === maDonHang) || data.find((r) => r[6] === maDonHang);
    if (!donHang) return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

    // 3. Lấy chi tiết sản phẩm
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

    // 4. Logo & watermark
    const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
    const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

    // 5. Render ngay
    res.render("bbntnk", {
      donHang,
      products,
      logoBase64,
      watermarkBase64,
      autoPrint: true,
      maDonHang,
      pathToFile: "",
    });

    // 6. Gọi AppScript lưu HTML
    (async () => {
      try {
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
        if (GAS_WEBAPP_URL_BBNTNK) {
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
          await sheets.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `file_BBNT_ct!D${lastRowWithData}`,
            valueInputOption: "RAW",
            requestBody: { values: [[pathToFile]] },
          });
          console.log("✔️ Đã ghi đường dẫn:", pathToFile);
        }
      } catch (err) {
        console.error("❌ Lỗi gọi AppScript BBNTNK:", err);
      }
    })();
  } catch (err) {
    console.error("❌ Lỗi khi xuất BBNTNK:", err.stack || err.message);
    res.status(500).send("Lỗi server: " + (err.message || err));
  }
});

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
app.get("/bbsv", async (req, res) => {
    try {
        console.log("▶️ Bắt đầu xuất BBSV ...");

        // --- Lấy mã biên bản sự việc từ sheet Bien_ban_su_viec ---
        const bbsvRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Bien_ban_su_viec!B:B",
        });
        const colB = bbsvRes.data.values ? bbsvRes.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maBBSV = colB[lastRowWithData - 1];
        
        if (!maBBSV)
            return res.send("⚠️ Không tìm thấy dữ liệu ở cột B sheet Bien_ban_su_viec.");

        console.log(`✔️ Mã biên bản sự việc: ${maBBSV} (dòng ${lastRowWithData})`);

        // --- Lấy dữ liệu từ sheet Bien_ban_su_viec ---
        const bbsvDetailRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Bien_ban_su_viec!A:Z",
        });
        const bbsvRows = bbsvDetailRes.data.values || [];
        const bbsvData = bbsvRows.slice(1);
        const bbsvRecord = bbsvData.find((r) => r[1] === maBBSV);
        
        if (!bbsvRecord)
            return res.send("❌ Không tìm thấy biên bản sự việc với mã: " + maBBSV);

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
                // Format date object if needed
                ngayLapBB = `ngày ${ngayLapBB.getDate()} tháng ${ngayLapBB.getMonth() + 1} năm ${ngayLapBB.getFullYear()}`;
            }
        }

        // Xử lý ngày yêu cầu xử lý
        let ngayYeuCauXuLy = bbsvRecord[8] || ''; // Cột I (index 8)
        if (ngayYeuCauXuLy) {
            if (typeof ngayYeuCauXuLy === 'string' && ngayYeuCauXuLy.includes('/')) {
                // Giữ nguyên định dạng dd/mm/yyyy
            } else if (ngayYeuCauXuLy instanceof Date) {
                // Format date object to dd/mm/yyyy
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

                // Cập nhật đường dẫn file vào sheet
                const pathToFile = data.pathToFile || `BBSV/${data.fileName}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `Bien_ban_su_viec!K${lastRowWithData}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log("✔️ Đã ghi đường dẫn:", pathToFile);

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
      const amount = amountchuathue / (1 + taxRate / 100);            // đơn giá chưa thuế
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




function formatNumber(num) {
  if (!num) return "0";
  num = Math.abs(num); // luôn lấy giá trị dương
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
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

        // --- Tính tổng ---
        let tongTien = 0;
        products.forEach(p => tongTien += parseFloat(p.thanhTien) || 0);

        // --- Xử lý chiết khấu ---
        let chietKhauValue = donHang[32] || "0";
        let chietKhauPercent = parseFloat(chietKhauValue.toString().replace('%', '')) || 0;
        let chietKhau = chietKhauValue.toString().includes('%')
            ? (tongTien * chietKhauPercent) / 100
            : chietKhauPercent;

        let tamUng = parseFloat(donHang[33]) || 0;
        let tongThanhTien = tongTien - chietKhau - tamUng;

        // --- Tính tổng diện tích và số lượng ---
        let tongDienTich = 0, tongSoLuong = 0;
        products.forEach(p => {
            const dienTich = parseFloat(p.dienTich) || 0;
            const soLuong = parseFloat(p.soLuong) || 0;
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

// Endpoint để trình duyệt đăng ký nhận push notifications
app.post('/subscribe', async (req, res) => {
  const subscription = req.body;
  
  // Kiểm tra xem subscription đã tồn tại chưa để tránh trùng lặp
  const exists = pushSubscriptions.some(sub => sub.endpoint === subscription.endpoint);
  if (!exists) {
    pushSubscriptions.push(subscription);
    await saveSubscriptions();
    console.log('✅ New browser subscription added.');
  }
  
  res.status(201).json({ message: 'Subscription saved successfully.' });
});

// Endpoint nhận webhook từ AppSheet
app.post('/webhook-from-appsheet', async (req, res) => {
  try {
    console.log('📨 === WEBHOOK RECEIVED ===');
    console.log('📦 Full request body:', JSON.stringify(req.body, null, 2));
    console.log('🔍 Request headers:', req.headers);
    
    const { title, body, icon, data } = req.body;
    
    if (!title) {
      console.log('❌ Title is missing in webhook payload');
      return res.status(400).json({ error: 'Title is required.' });
    }
    
    console.log(`✅ Webhook validated: "${title}"`);
    
    // Kiểm tra xem có subscription nào không
    console.log(`📋 Total subscriptions in memory: ${pushSubscriptions.length}`);
    
    if (pushSubscriptions.length === 0) {
      console.log('⚠️ No browser subscriptions found. Has the user visited index.html and clicked subscribe?');
      return res.json({ success: false, message: 'No subscribers yet.' });
    }
    
    const payload = JSON.stringify({ title, body, icon, data });
    
    // Gửi thông báo và log chi tiết kết quả
    const results = [];
    for (let i = 0; i < pushSubscriptions.length; i++) {
      const sub = pushSubscriptions[i];
      try {
        console.log(`➡️ Sending to subscription ${i + 1}: ${sub.endpoint.substring(0, 50)}...`);
        await webPush.sendNotification(sub, payload);
        console.log(`✅ Successfully sent to subscription ${i + 1}`);
        results.push({ index: i, status: 'success' });
      } catch (err) {
        console.error(`❌ Failed to send to subscription ${i + 1}:`, {
          statusCode: err.statusCode,
          message: err.message,
          endpoint: sub.endpoint.substring(0, 50)
        });
        
        // Xóa subscription không hợp lệ
        if (err.statusCode === 410) {
          pushSubscriptions.splice(i, 1);
          i--; // Điều chỉnh index sau khi xóa
          console.log(`🗑️ Removed expired subscription ${i + 1}`);
        }
        results.push({ index: i, status: 'failed', error: err.message });
      }
    }
    
    // Lưu lại danh sách subscriptions sau khi xóa các subscription hết hạn
    await saveSubscriptions();
    
    console.log(`📊 Notification send summary: ${results.filter(r => r.status === 'success').length} succeeded, ${results.filter(r => r.status === 'failed').length} failed`);
    
    res.json({ 
      success: true, 
      message: `Processed for ${pushSubscriptions.length} subscriber(s)`,
      results 
    });
    
  } catch (error) {
    console.error('💥 CRITICAL Webhook processing error:', error);
    res.status(500).json({ error: 'Internal server error.', details: error.message });
  }
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
        // Xử lý bảng 5: TỔNG LƯƠNG KHOÁN DỊCH VỤ
const parseNumberFromSheet = (value) => {
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
};

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

        // Format số với dấu phẩy phân cách hàng nghìn
        // Format số với dấu phẩy phân cách hàng nghìn
const formatNumber = (num) => {
    if (num === null || num === undefined) return '0';
    const number = parseFloat(num);
    if (isNaN(number)) return '0';
    return new Intl.NumberFormat('vi-VN').format(number);
};

        // Nếu yêu cầu xuất Excel
        if (exportExcel === 'true') {
            const workbook = new exceljs.Workbook();
            
            // Sheet 1: DANH SÁCH ĐƠN HÀNG TRẢ KHOÁN
            const sheet1 = workbook.addWorksheet('Danh sách đơn hàng trả khoán');
            
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
            sheet1.addRow(headers1);
            
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
            sheet2.addRow(headers2);
            
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
            const sheet3 = workbook.addWorksheet('Tổng hợp chi trả khoán');
            
            sheet3.mergeCells('A1:D1');
            sheet3.getCell('A1').value = 'TỔNG HỢP CHI TRẢ KHOÁN GIAO VẬN';
            sheet3.getCell('A1').font = { bold: true, size: 16 };
            sheet3.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet3.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers3 = ['STT', 'Tên nhân sự', 'Thành tiền', 'Ghi chú'];
            sheet3.addRow(headers3);
            
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
            sheet4.addRow(headers4);
            
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
            
            // Sheet 5: TỔNG LƯƠNG KHOÁN DỊCH VỤ
            const sheet5 = workbook.addWorksheet('Tong_luong_khoan_theo_nhan_su');
            
            sheet5.mergeCells('A1:K1');
            sheet5.getCell('A1').value = 'TỔNG LƯƠNG KHOÁN DỊCH VỤ';
            sheet5.getCell('A1').font = { bold: true, size: 16 };
            sheet5.getCell('A1').alignment = { horizontal: 'center' };
            
            sheet5.getCell('A2').value = `Tháng/Năm: ${monthYear}`;
            
            const headers5 = [
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
            sheet5.addRow(headers5);
            
            const headerRow5 = sheet5.getRow(4);
            headerRow5.font = { bold: true };
            headerRow5.alignment = { horizontal: 'center' };
            
            table5Data.forEach(item => {
                sheet5.addRow([
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
            
            sheet5.columns = [
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
            
            for (let i = 4; i <= sheet5.rowCount; i++) {
                for (let j = 1; j <= 11; j++) {
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
       // Render template với dữ liệu
res.render("baocaoluongkhoan", {
    monthYear,
    data: {
        table1: paginatedTable1Data,
        table2: table2Data,
        table3: table3Data,
        table4: table4Data,
        table5: table5Data
    },
    currentPage,
    totalPages,
    table1Data: paginatedTable1Data,
    table2Data,
    table3Data,
    table4Data,
    table5Data,
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
// --- Start server ---
app.listen(PORT, () => console.log(`✅ Server is running on port ${PORT}`));