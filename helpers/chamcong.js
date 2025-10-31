// helpers/chamcong.js
// ESM module
import { parseISO } from "date-fns"; // optional, but safe. If not installed remove usage or install date-fns

// NOTE: expects you to pass `sheets` (the google sheets client) and SPREADSHEET_HC_ID
export async function getSheetValues(sheets, spreadsheetId, range) {
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range,
  });
  return resp.data.values || [];
}

/** very robust date parser:
 * accepts:
 *  - "dd/mm/yyyy" or "d/m/yyyy" (strings)
 *  - "yyyy-mm-dd" (ISO)
 *  - JS Date objects
 *  - numbers (Excel serial dates) -> convert to date
 * returns {d,m,y} or null
 */
function parseToDMY(v) {
  if (v == null) return null;
  // already a Date
  if (v instanceof Date && !isNaN(v)) {
    return { d: v.getDate(), m: v.getMonth() + 1, y: v.getFullYear() };
  }

  const s = String(v).trim();

  // numeric? could be Excel serial
  if (/^\d+(\.\d+)?$/.test(s)) {
    const n = Number(s);
    // Heuristic: if n > 1900 and looks like year then treat as year (unlikely), else treat as excel serial
    if (n > 40000) {
      // maybe it's already a year-like large number or timestamp; try Date parse
      const dt = new Date(n);
      if (!isNaN(dt)) return { d: dt.getDate(), m: dt.getMonth() + 1, y: dt.getFullYear() };
    }
    // Excel serial number -> convert (Excel epoch 1899-12-30)
    // Some Google Sheets export dates as numbers like 44561
    if (n > 2000 && n < 60000) {
      const epoch = new Date(Date.UTC(1899, 11, 30)); // 1899-12-30
      const dt = new Date(epoch.getTime() + Math.round(n) * 24 * 3600 * 1000);
      return { d: dt.getUTCDate(), m: dt.getUTCMonth() + 1, y: dt.getUTCFullYear() };
    }
  }

  // dd/mm/yyyy or d/m/yyyy
  const dmY = s.match(/^(\d{1,2})\s*[/\-]\s*(\d{1,2})\s*[/\-]\s*(\d{4})$/);
  if (dmY) {
    return { d: Number(dmY[1]), m: Number(dmY[2]), y: Number(dmY[3]) };
  }

  // yyyy-mm-dd or ISO
  const isoMatch = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (isoMatch) {
    return { d: Number(isoMatch[3]), m: Number(isoMatch[2]), y: Number(isoMatch[1]) };
  }

  // try Date.parse fallback
  const dtmp = Date.parse(s);
  if (!isNaN(dtmp)) {
    const dt = new Date(dtmp);
    return { d: dt.getDate(), m: dt.getMonth() + 1, y: dt.getFullYear() };
  }

  return null;
}

// normalize employee code for robust matching
function normCode(v) {
  if (v == null) return "";
  let s = String(v).trim();
  // remove trailing .0 caused by numeric cells
  if (s.endsWith(".0")) s = s.slice(0, -2);
  // remove spaces
  s = s.replace(/\s+/g, "");
  return s.toLowerCase();
}

// find header index by name (case-insensitive contains)
function findHeaderIndex(headerRow, candidates) {
  if (!headerRow || !Array.isArray(headerRow)) return -1;
  const lower = headerRow.map(h => (h || "").toString().toLowerCase());
  for (const c of candidates) {
    const lc = c.toLowerCase();
    const i = lower.findIndex(cell => cell.includes(lc));
    if (i >= 0) return i;
  }
  return -1;
}

/**
 * buildAttendanceData(sheets, SPREADSHEET_HC_ID, month, year, options)
 * returns { days: [{day,weekday}], records: [ { stt, maNV, hoTen, chucVu, perDay:[{caSang,caChieu,weekday}], soNgayCong, congLe, ngayPhep, tongTangCa } ] }
 */
export async function buildAttendanceData(sheets, SPREADSHEET_HC_ID, month, year, options = {}) {
  const DEBUG = options.debug || false;

  // read sheets raw
  const chamRaw = await getSheetValues(sheets, SPREADSHEET_HC_ID, "Cham_cong!A:Z");
  const nvRaw = await getSheetValues(sheets, SPREADSHEET_HC_ID, "Nhan_vien!A:AH");

  if (DEBUG) {
    console.log("Cham_cong rows:", Math.max(0, chamRaw.length - 1));
    console.log("Nhan_vien rows:", Math.max(0, nvRaw.length - 1));
  }

  // identify header rows (assume first row is header if it contains text)
  const nvHeader = nvRaw[0] && nvRaw[0].some(c => typeof c === "string") ? nvRaw[0] : null;
  // fallback indices if header not present
  const NV_IDX = {
    ma: nvHeader ? findHeaderIndex(nvHeader, ["mã", "ma", "code"]) : 0,      // A
    name: nvHeader ? findHeaderIndex(nvHeader, ["họ", "tên", "name"]) : 1,   // B
    nhom: nvHeader ? findHeaderIndex(nvHeader, ["nhóm", "group"]) : 8,      // I
    chucVu: nvHeader ? findHeaderIndex(nvHeader, ["chức", "chuc", "position"]) : 9, // J
    tinhtrang: nvHeader ? findHeaderIndex(nvHeader, ["tình", "tinh", "trạng", "trang"]) : 33 // AH
  };

  // if some indices are -1 fallback to default numeric positions
  if (NV_IDX.ma === -1) NV_IDX.ma = 0;
  if (NV_IDX.name === -1) NV_IDX.name = 1;
  if (NV_IDX.nhom === -1) NV_IDX.nhom = 8;
  if (NV_IDX.chucVu === -1) NV_IDX.chucVu = 9;
  if (NV_IDX.tinhtrang === -1) NV_IDX.tinhtrang = 33;

  // build active staff list
  const staff = [];
  for (let i = (nvHeader ? 1 : 0); i < nvRaw.length; i++) {
    const row = nvRaw[i];
    if (!row) continue;
    const ma = row[NV_IDX.ma] ? String(row[NV_IDX.ma]).trim() : "";
    const hoTen = row[NV_IDX.name] ? String(row[NV_IDX.name]).trim() : "";
    const nhom = row[NV_IDX.nhom] ? String(row[NV_IDX.nhom]).trim() : "";
    const chucVu = row[NV_IDX.chucVu] ? String(row[NV_IDX.chucVu]).trim() : "";
    const tinhTrang = row[NV_IDX.tinhtrang] ? String(row[NV_IDX.tinhtrang]).trim() : "";

    if (!ma) continue;
    if (tinhTrang !== "Đang hoạt động") continue;

    staff.push({
      maRaw: ma,
      ma: normCode(ma),
      hoTen,
      chucVu,
      nhom,
      defaultNgayCong: nhom === "Quản lý" ? 26 : null
    });
  }

  if (DEBUG) console.log("Active staff count:", staff.length);

  // build Cham_cong map: key = normCode(maNV) + '_' + dd_mm_yyyy
  // Determine Cham_cong header (if present)
  const chamHeader = chamRaw[0] && chamRaw[0].some(c => typeof c === "string") ? chamRaw[0] : null;

  // fallback indices for Cham_cong
  const CH_IDX = {
    date: chamHeader ? findHeaderIndex(chamHeader, ["ngày", "date", "day", "b"]) : 1, // B
    status: chamHeader ? findHeaderIndex(chamHeader, ["trạng", "status", "c"]) : 2, // C
    name: chamHeader ? findHeaderIndex(chamHeader, ["họ", "tên", "l"]) : 11, // L
    ma: chamHeader ? findHeaderIndex(chamHeader, ["mã", "ma", "m"]) : 12, // M
    q: chamHeader ? findHeaderIndex(chamHeader, ["q", "công", "ket qua", "q"]) : 16, // Q
    t: chamHeader ? findHeaderIndex(chamHeader, ["tăng ca", "t", "tc"]) : 19 // T
  };

  // fallback if -1
  if (CH_IDX.date === -1) CH_IDX.date = 1;
  if (CH_IDX.status === -1) CH_IDX.status = 2;
  if (CH_IDX.name === -1) CH_IDX.name = 11;
  if (CH_IDX.ma === -1) CH_IDX.ma = 12;
  if (CH_IDX.q === -1) CH_IDX.q = 16;
  if (CH_IDX.t === -1) CH_IDX.t = 19;

  // build map
  const map = new Map();
  for (let i = (chamHeader ? 1 : 0); i < chamRaw.length; i++) {
    const row = chamRaw[i];
    if (!row) continue;
    const dateCell = row[CH_IDX.date];
    const parsed = parseToDMY(dateCell);
    if (!parsed) continue;
    const d = parsed.d, m = parsed.m, y = parsed.y;
    // only store rows for the requested year/month? we can store all, but we'll filter later
    const maCell = row[CH_IDX.ma] || "";
    const maKey = normCode(maCell);
    const key = `${maKey}_${String(d).padStart(2,"0")}_${String(m).padStart(2,"0")}_${String(y)}`;
    map.set(key, {
      status: row[CH_IDX.status] ? String(row[CH_IDX.status]).trim() : "",
      q: Number(String(row[CH_IDX.q] || "").replace(",", ".")) || 0,
      t: Number(String(row[CH_IDX.t] || "").replace(",", ".")) || 0,
      raw: row
    });
  }

  if (DEBUG) console.log("Cham_cong map size:", map.size);

  // prepare days of month
  const numDays = new Date(year, month, 0).getDate();
  const days = [];
  for (let d = 1; d <= numDays; d++) {
    const dt = new Date(year, month - 1, d);
    days.push({ day: d, weekday: dt.getDay() }); // weekday: 0=Sunday
  }

  // build records for each staff
  const records = staff.map((s, idx) => {
    let perDay = [];
    let tongTangCa = 0;
    let countV = 0;
    let sumQ = 0;
    let countLV = 0;
    let countP = 0;

    for (const dd of days) {
      const key = `${s.ma}_${String(dd.day).padStart(2,"0")}_${String(month).padStart(2,"0")}_${String(year)}`;
      const rec = map.get(key);
      let caS = "", caC = "", isHoliday = false;
      if (rec) {
        const st = (rec.status || "").toString();
        const q = rec.q || 0;
        const t = rec.t || 0;
        tongTangCa += t;
        // holiday detection: exact match or contains 'lễ' (case-insensitive)
        if (/lễ|le|holiday/i.test(st)) {
          caS = caC = "L";
          countLV += 1;
          isHoliday = true;
        } else if (st === "Nghỉ việc riêng") {
          caS = caC = "X";
        } else if (st === "Nghỉ phép") {
          caS = caC = "P";
          countP += 1;
        } else {
          // working day
          if (q === 1) { caS = "V"; caC = "V"; countV += 2; }
          else if (q === 0.5) { caS = "V"; caC = "X"; countV += 1; }
          else if (q > 0 && q < 0.5) { caS = `${(q * 8).toFixed(2).replace(/\.00$/,'')}h`; }
          else if (q > 0.5 && q < 1) { caS = "V"; caC = `${((q - 0.5) * 8).toFixed(2).replace(/\.00$/,'')}h`; countV +=1; }
          else if (q === 0) { /* nothing */ }
          else if (q > 1) { // improbable but handle
            caS = "V"; caC = "V"; countV += 2;
          }
          sumQ += q;
        }
      }
      perDay.push({ caS, caC, weekday: dd.weekday, isHoliday });
    }

    // SoNgayCong = COUNTIF(...,"V")/2 + SUM(...)/8 - BP16
    // We don't know BP16; assume BP16 == countLV (holiday adjustments) as earlier
    const soNgayCong = (countV / 2) + (sumQ) - countLV;
    const congLe = countLV / 2;
    const ngayPhep = countP / 2;
    const tongCongTrucTiep = soNgayCong; // you can adjust if needed

    return {
      stt: null, // will fill after sorting
      maNV: s.maRaw,
      maNorm: s.ma,
      hoTen: s.hoTen,
      chucVu: s.chucVu,
      nhom: s.nhom,
      defaultNgayCong: s.defaultNgayCong,
      perDay,
      soNgayCong: Number(Number(soNgayCong || 0).toFixed(2)),
      congLe: Number(Number(congLe || 0).toFixed(2)),
      ngayPhep: Number(Number(ngayPhep || 0).toFixed(2)),
      tongCongTrucTiep: Number(Number(tongCongTrucTiep || 0).toFixed(2)),
      tongTangCa: Number(Number(tongTangCa || 0).toFixed(2))
    };
  });

  // assign STT in natural order (sorted by maNV or hoTen depending)
  records.sort((a,b) => {
    // prefer sorting by ma numeric if possible
    const na = a.maNorm.replace(/\D/g,'') || a.maNorm;
    const nb = b.maNorm.replace(/\D/g,'') || b.maNorm;
    if (!isNaN(Number(na)) && !isNaN(Number(nb))) return Number(na) - Number(nb);
    if (a.hoTen && b.hoTen) return a.hoTen.localeCompare(b.hoTen, 'vi');
    return a.maNV.localeCompare(b.maNV);
  });
  records.forEach((r, i) => r.stt = i + 1);

  // debug: if no records or no map matches for that month, log some examples
  if (DEBUG) {
    const anyData = Array.from(map.keys()).filter(k => k.includes(`_${String(month).padStart(2,"0")}_${String(year)}`));
    console.log("Number of Cham_cong entries for month:", anyData.length);
    if (anyData.length < 1) {
      // print first 6 map keys to help debugging
      console.log("Cham_cong map sample keys:", Array.from(map.keys()).slice(0,6));
      console.log("Staff sample ma's:", staff.slice(0,6).map(s=>s.maRaw));
    }
  }

  return { days, records };
}
