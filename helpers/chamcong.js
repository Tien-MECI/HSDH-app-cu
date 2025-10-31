// helpers/chamcong.js
import { getSheetValues } from "./googleSheet.js";

function parseDDMMYYYY(s) {
  if (!s) return null;
  const [d, m, y] = s.split("/").map(Number);
  return { d, m, y };
}

function monthDays(month, year) {
  const num = new Date(year, month, 0).getDate();
  const days = [];
  for (let i = 1; i <= num; i++) {
    const d = new Date(year, month - 1, i);
    days.push({ day: i, weekday: d.getDay() });
  }
  return days;
}

export async function buildAttendanceData(sheets, SPREADSHEET_HC_ID, month, year) {
  const cham = await getSheetValues(sheets, SPREADSHEET_HC_ID, "Cham_cong!A:T");
  const nv = await getSheetValues(sheets, SPREADSHEET_HC_ID, "Nhan_vien!A:AH");

  const records = [];

  const headersNV = nv[0];
  const colA = headersNV.indexOf("Mã nhân viên");
  const colB = headersNV.indexOf("Họ và tên nhân viên");
  const colI = headersNV.indexOf("Nhóm");
  const colJ = headersNV.indexOf("Chức vụ");
  const colAH = headersNV.indexOf("Tình trạng");

  const nvHoatDong = nv.filter((r, i) => i > 0 && r[colAH] === "Đang hoạt động");

  const days = monthDays(month, year);

  for (const emp of nvHoatDong) {
    const maNV = emp[colA];
    const hoTen = emp[colB];
    const chucVu = emp[colJ];
    const nhom = emp[colI];

    const rowsNV = cham.filter(r => r[12] === hoTen || r[13] === maNV);
    const rowObj = {
      maNV,
      hoTen,
      chucVu,
      nhom,
      congNgay: [],
      tongCong: 0,
      tongTangCa: 0,
    };

    for (const d of days) {
      const match = rowsNV.find(r => {
        const date = parseDDMMYYYY(r[1]);
        return date && date.d === d.day && date.m === month && date.y === year;
      });

      let codeS = "", codeC = "";

      if (match) {
        const trangThai = match[2];
        const q = parseFloat(match[16] || 0);
        const t = parseFloat(match[19] || 0);

        if (trangThai === "Nghỉ việc riêng") { codeS = codeC = "X"; }
        else if (trangThai === "Nghỉ phép") { codeS = codeC = "P"; }
        else {
          if (q === 1) codeS = codeC = "V";
          else if (q === 0.5) { codeS = "V"; codeC = "X"; }
          else if (q > 0.5 && q < 1) { codeS = "V"; codeC = `${Math.round((q - 0.5) * 8)}h`; }
          else if (q < 0.5 && q > 0) { codeS = `${Math.round(q * 8)}h`; codeC = ""; }
        }

        rowObj.tongCong += q;
        rowObj.tongTangCa += t;
      }

      rowObj.congNgay.push({ codeS, codeC, weekday: d.weekday });
    }

    records.push(rowObj);
  }

  return { days, records };
}
