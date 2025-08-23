import dotenv from "dotenv";
import express from "express";
import { google } from "googleapis";
import path, { dirname } from "path";
import { fileURLToPath } from "url";
import ejs from "ejs";
import fetch from "node-fetch";

dotenv.config();

// --- __dirname trong ESM ---
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// --- IDs file Drive ---
const LOGO_FILE_ID = "1Rwo4pJt222dLTXN9W6knN3A5LwJ5TDIa";
const WATERMARK_FILE_ID = "1fNROb-dRtRl2RCCDCxGPozU3oHMSIkHr";

// --- ENV ---
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const GAS_WEBAPP_URL =
    process.env.GAS_WEBAPP_URL ||
    "https://script.google.com/macros/s/AKfycbyYKqYXMlDMG9n_LrpjjNqOtnA6MElh_ds00og0j59-E2UtvGq9YQZVI3lBTUb60Zo-/exec";
const GOOGLE_CREDENTIALS_B64 = process.env.GOOGLE_CREDENTIALS_B64;

if (!SPREADSHEET_ID || !GAS_WEBAPP_URL || !GOOGLE_CREDENTIALS_B64) {
    console.error("❌ Thiếu biến môi trường: SPREADSHEET_ID / GAS_WEBAPP_URL / GOOGLE_CREDENTIALS_B64");
    process.exit(1);
}

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
const app = express();
const PORT = process.env.PORT || 3000;

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

async function loadDriveImageBase64(fileId) {
    try {
        const meta = await drive.files.get({ fileId, fields: "mimeType" });
        const bin = await drive.files.get({ fileId, alt: "media" }, { responseType: "arraybuffer" });
        const buffer = Buffer.from(bin.data, "binary");
        return `data:${meta.data.mimeType};base64,${buffer.toString("base64")}`;
    } catch (e) {
        console.error(`⚠️ Không tải được file Drive ${fileId}:`, e.message);
        return "";
    }
}

// --- Hàm gọi AppScript chung ---
async function callAppScript(orderCode, renderedHtml, type) {
    const resp = await fetch(GAS_WEBAPP_URL, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
            orderCode,
            html: renderedHtml,
            type, // để GAS phân biệt BBGN / BBNT
        }),
    });

    const txt = await resp.text();
    let data;
    try {
        data = JSON.parse(txt);
    } catch (e) {
        console.error("❌ Parse JSON lỗi, raw text:", txt);
        return null;
    }
    return data;
}

// --- Hàm xử lý BBGN/BBNT chung ---
async function handleRoute(type, sheetName, viewName, res) {
    try {
        console.log(`▶️ Bắt đầu xuất ${type.toUpperCase()} ...`);

        // Lấy mã đơn hàng
        const respSheet = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${sheetName}!B:B`,
        });
        const colB = respSheet.data.values ? respSheet.data.values.flat() : [];
        const lastRowWithData = colB.length;
        const maDonHang = colB[lastRowWithData - 1];
        if (!maDonHang) return res.send(`⚠️ Không tìm thấy dữ liệu ở cột B sheet ${sheetName}.`);

        console.log(`✔️ Mã đơn hàng: ${maDonHang} (dòng ${lastRowWithData})`);

        // Lấy đơn hàng
        const donHangRes = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: "Don_hang!A1:BJ",
        });
        const rows = donHangRes.data.values || [];
        const data = rows.slice(1);
        const donHang = data.find((r) => r[5] === maDonHang) || data.find((r) => r[6] === maDonHang);
        if (!donHang) return res.send("❌ Không tìm thấy đơn hàng với mã: " + maDonHang);

        // Lấy chi tiết sản phẩm
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

        // Logo & watermark
        const logoBase64 = await loadDriveImageBase64(LOGO_FILE_ID);
        const watermarkBase64 = await loadDriveImageBase64(WATERMARK_FILE_ID);

        // Render ra client
        res.render(viewName, {
            donHang,
            products,
            logoBase64,
            watermarkBase64,
            autoPrint: true,
            maDonHang,
            pathToFile: "",
        });

        // Gọi AppScript ngầm
        (async () => {
            try {
                const renderedHtml = await ejs.renderFile(path.join(__dirname, "views", `${viewName}.ejs`), {
                    donHang,
                    products,
                    logoBase64,
                    watermarkBase64,
                    autoPrint: false,
                    maDonHang,
                    pathToFile: "",
                });

                const data = await callAppScript(maDonHang, renderedHtml, type);
                if (!data) return;

                console.log("✔️ AppScript trả về:", data);

                const pathToFile = data.pathToFile || `${type.toUpperCase()}/${data.fileName}`;
                await sheets.spreadsheets.values.update({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `${sheetName}!D${lastRowWithData}`,
                    valueInputOption: "RAW",
                    requestBody: { values: [[pathToFile]] },
                });
                console.log("✔️ Đã ghi đường dẫn:", pathToFile);
            } catch (err) {
                console.error(`❌ Lỗi gọi AppScript cho ${type}:`, err);
            }
        })();
    } catch (err) {
        console.error(`❌ Lỗi khi xuất ${type}:`, err.stack || err.message);
        res.status(500).send("Lỗi server: " + (err.message || err));
    }
}

// --- Routes ---
app.get("/", (_req, res) => res.send("🚀 Server chạy ổn! /bbgn hoặc /bbnt để xuất biểu mẫu."));

app.get("/bbgn", (req, res) => handleRoute("bbgn", "file_BBGN_ct", "bbgn", res));
app.get("/bbnt", (req, res) => handleRoute("bbnt", "file_BBNT_ct", "bbnt", res));

// Debug
app.get("/debug", (_req, res) => {
    res.json({
        spreadsheetId: SPREADSHEET_ID,
        clientEmail: credentials.client_email,
        gasWebappUrl: GAS_WEBAPP_URL,
    });
});

// Start server
app.listen(PORT, () => console.log(`✅ Server is running on port ${PORT}`));
