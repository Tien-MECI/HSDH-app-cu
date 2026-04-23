import { google } from 'googleapis';

console.log('🚀 Đang load module ycvt.js...');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * prepareYcvtData
 * - auth: OAuth2 client
 * - spreadsheetId: id của workbook chính (chứa Don_hang_PVC_ct, Don_hang, File_BOM_ct)
 * - spreadsheetHcId: id workbook chứa Data_bom
 */
async function prepareYcvtData(auth, spreadsheetId, spreadsheetHcId, maDonHang = null) {
  console.log('▶️ Bắt đầu prepareYcvtData...');
  const sheets = google.sheets({ version: 'v4', auth });

  // --- Throttle / Retry configuration (không thay đổi logic xử lý dữ liệu) ---
  // Bạn có thể set env SHEETS_WRITE_PER_MINUTE để điều chỉnh giới hạn (mặc định 60)
  const WRITE_LIMIT = process.env.SHEETS_WRITE_PER_MINUTE ? parseInt(process.env.SHEETS_WRITE_PER_MINUTE, 10) : 60;
  const WRITE_WINDOW_MS = 60 * 1000; // 1 phút
  const writeTimestamps = []; // lưu các timestamp của write requests (ms)

  function pruneOldTimestamps() {
    const now = Date.now();
    while (writeTimestamps.length && (now - writeTimestamps[0]) > WRITE_WINDOW_MS) {
      writeTimestamps.shift();
    }
    // Giới hạn kích thước của writeTimestamps để tránh tiêu tốn bộ nhớ
    if (writeTimestamps.length > WRITE_LIMIT * 2) {
      writeTimestamps.splice(0, writeTimestamps.length - WRITE_LIMIT * 2);
    }
    if (global.gc) {
      global.gc(); // Trigger garbage collection
    }
  }

  async function throttleIfNeeded() {
    pruneOldTimestamps();
    if (writeTimestamps.length < WRITE_LIMIT) return;
    console.log(`🧠 Memory usage before throttling: ${JSON.stringify(process.memoryUsage())}`);
    const now = Date.now();
    const oldest = writeTimestamps[0];
    const waitFor = WRITE_WINDOW_MS - (now - oldest) + 50; // add small buffer
    await sleep(waitFor);
    pruneOldTimestamps();
    console.log(`🧠 Memory usage after throttling: ${JSON.stringify(process.memoryUsage())}`);
  }

  async function makeWriteRequest(fn) {
    const maxRetries = 6;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      await throttleIfNeeded();
      try {
        const res = await fn();
        // đăng kí 1 write
        writeTimestamps.push(Date.now());
        pruneOldTimestamps();
        return res;
      } catch (err) {
        const msg = (err && err.message) ? err.message : '';
        const status = err?.response?.status;
        const isQuota = msg.includes('Quota exceeded') || status === 429 || status === 403;
        if (attempt === maxRetries - 1) {
          // không còn retry nữa
          throw err;
        }
        // chỉ dùng backoff cho lỗi tạm thời / quota
        const delay = Math.min(500 * Math.pow(2, attempt), 30000);
        console.warn(`⚠️ Write request failed (attempt ${attempt + 1}): ${msg || status}. Backing off ${delay}ms`);
        await sleep(delay);
      }
    }
  }

  async function batchPaste(valueRanges) {
    if (!valueRanges || valueRanges.length === 0) return;
    return makeWriteRequest(() => sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: spreadsheetHcId,
      requestBody: { valueInputOption: 'RAW', data: valueRanges }
    }));
  }

  async function batchClear(ranges) {
    if (!ranges || ranges.length === 0) return;
    return makeWriteRequest(() => sheets.spreadsheets.values.batchClear({
      spreadsheetId: spreadsheetHcId,
      requestBody: { ranges }
    }));
  }

  try {
    // 1) Lấy dữ liệu ban đầu
    const [data1Res, data2Res, data3Res, data5Res] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId, range: 'Don_hang_PVC_ct!A1:AE' }),
      sheets.spreadsheets.values.get({ spreadsheetId, range: 'Don_hang!A1:CF' }),
      sheets.spreadsheets.values.get({ spreadsheetId: spreadsheetHcId, range: 'Data_bom!A1:N' }),
      sheets.spreadsheets.values.get({ spreadsheetId, range: 'File_BOM_ct!A1:D' })
    ]);

    const data1 = data1Res.data.values || [];
    const data2 = data2Res.data.values || [];
    const data3 = data3Res.data.values || [];
    const data5 = data5Res.data.values || [];

    console.log(`✔️ Lấy dữ liệu xong: Don_hang_PVC_ct=${data1.length}, Don_hang=${data2.length}, Data_bom=${data3.length}, File_BOM_ct=${data5.length}`);

    // --- 2) Xác định d4Value ---
    let d4Value = maDonHang || '';
    let lastRowWithData = 0;

    if (!maDonHang) {
      for (let i = data5.length - 1; i >= 0; i--) {
        const v = (data5[i] && data5[i][1]) ? String(data5[i][1]).trim() : '';
        if (v !== '') { d4Value = v; lastRowWithData = i + 1; break; }
      }
    } else {
      // Nếu có maDonHang truyền vào, tìm dòng tương ứng (để log)
      const idx = data5.findIndex(r => r[1] === maDonHang);
      lastRowWithData = idx !== -1 ? idx + 1 : 0;
    }
    if (!d4Value) throw new Error('Không tìm thấy mã đơn hàng trong File_BOM_ct cột B');
    console.log(`✔️ Mã đơn hàng: ${d4Value} (dòng ${lastRowWithData})`);

    // 3) Tìm hValues từ Don_hang_PVC_ct
    const hValues = data1.slice(1)
      .map((r, idx) => ({ row: r, idx }))
      .filter(o => String(o.row[1] || '').trim() === String(d4Value).trim())
      .map((o, i) => ({ stt: i + 1, hValue: o.row[7] || '', rowData: o.row }));

    console.log(`✔️ Tìm thấy ${hValues.length} hValue trong Don_hang_PVC_ct`);

    // 4) Xử lý tuần tự từng hValue
    const columnsToCopyBase = [17, 18, 19, 20, 21, 22, 23, 24, 29]; // 9 cột
    let tableData = [];

    for (const hObj of hValues) {
      const hValue = hObj.hValue;
      if (!hValue) continue;

      // tìm tất cả row trong Data_bom có C == hValue
      const matchesC = [];
      for (let i = 0; i < data3.length; i++) {
        const row = data3[i] || [];
        if (String(row[2] || '').trim() === String(hValue).trim()) matchesC.push(i);
      }
      if (matchesC.length === 0) continue;

      // build dữ liệu để paste
      const targetValues = columnsToCopyBase.map(colIndex =>
        hObj.rowData[colIndex - 1] !== undefined ? hObj.rowData[colIndex - 1] : ''
      );

      const pasteValueRanges = [];
      const pastedRanges = [];
      for (const idx of matchesC) {
        const rowNum = idx + 1;
        const range = `Data_bom!F${rowNum}:N${rowNum}`;
        pasteValueRanges.push({ range, values: [targetValues] });
        pastedRanges.push(range);
      }

      // 5) paste riêng cho hValue này
      if (pasteValueRanges.length > 0) {
        console.log(`📥 Paste ${hValue}: ${pasteValueRanges.length} ranges...`);
        await batchPaste(pasteValueRanges);
        await sleep(600);

        // 6) đọc lại Data_bom
        let updatedData3 = null;
        for (let attempts = 0; attempts < 5; attempts++) {
          const res = await sheets.spreadsheets.values.get({
            spreadsheetId: spreadsheetHcId,
            range: 'Data_bom!A1:N'
          });
          updatedData3 = res.data.values || [];
          const someBpopulated = updatedData3.some(r => r && r[1] && String(r[1]).trim() !== '');
          if (someBpopulated) break;
          await sleep(600);
        }

        // 7) thu thập dữ liệu theo A == hValue
        for (const row of updatedData3) {
          if (String(row?.[0] || '').trim() === String(hValue).trim()) {
            const sliceBN = row.slice(1, 14);
            while (sliceBN.length < 13) sliceBN.push('');
            tableData.push({ stt: hObj.stt, row: sliceBN });
          }
        }

        // 8) clear lại
        if (pastedRanges.length > 0) {
          console.log(`🧹 Clear ${hValue}: ${pastedRanges.length} ranges...`);
          await batchClear(pastedRanges);
        }
      }
    }

    // 6) Summary
    const uniqueB = [...new Set(tableData.map(item => item.row[1]).filter(v => v && v !== 'Mã SP' && v !== 'Mã vật tư sản xuất'))];
    const uniqueC = [...new Set(tableData.map(item => item.row[2]).filter(v => v && v !== 'Mã vật tư xuất kèm' && v !== 'Mã vật tư sản xuất'))];

    const summaryDataB = uniqueB.map((b, i) => {
      const relatedRows = tableData.filter(item => item.row[1] === b || item.row[2] === b);
      const sum = relatedRows.reduce((s, item) => {
        const value = item.row[9] || '';
        const parsedValue = parseFloat(value.toString().replace(/\./g, '').replace(',', '.')) || 0;
        return s + parsedValue;
      }, 0);
      return {
        stt: i + 1,
        code: b,
        desc: relatedRows[0]?.row[3] || '',
        sum,
        DVT: relatedRows[0]?.row[10] || ''
      };
    });

    const summaryDataC = uniqueC.map((c, i) => {
      const relatedRows = tableData.filter(item =>
        item.row[1] === c || item.row[2] === c
      );
      const sum = relatedRows.reduce((s, item) => {
        const value = item.row[9] || '';
        const parsedValue = parseFloat(value.toString().replace(/\./g, '').replace(',', '.')) || 0;
        return s + parsedValue;
      }, 0);
      const desc = relatedRows.find(item => item.row[3])?.row[3] || '';
      const DVT = relatedRows.find(item => item.row[10])?.row[10] || '';
      return { stt: summaryDataB.length + i + 1, code: c, sum, desc, DVT };
    });

    // 7) Thông tin Don_hang
    const matchingRows = data2.slice(1).filter(
      row => String(row[5] || '').trim() === String(d4Value).trim() || String(row[6] || '').trim() === String(d4Value).trim()
    );
    const l4Value = matchingRows[0] ? (matchingRows[0][9] || '') : '';
    const d5Values = matchingRows.map(r => r[31]).filter(v => v).join(', ');
    const ghichuKT = matchingRows.map(r => r[55]).filter(v => v).join(', ');
    const h5Values = matchingRows.map(r => r[37]).filter(v => v).join(', ');
    const h6Values = matchingRows.map(r => r[29]).filter(v => v).join(', ');
    const Loaiycthuchien = matchingRows.map(r => r[28]).filter(v => v).join(', ');
    const d6Values = matchingRows
      .map(r => r[40] ? new Date(r[40]).toLocaleDateString('vi-VN', { day: '2-digit', month: '2-digit', year: 'numeric' }) : '')
      .filter(v => v).join('<br>');

    // 8) Flags kiểm tra dữ liệu
    const hasDataF = tableData.some(item => item.row[4] && item.row[4].toString().trim() !== '');
    const hasDataG = tableData.some(item => item.row[5] && item.row[5].toString().trim() !== '');
    const hasDataH = tableData.some(item => item.row[6] && item.row[6].toString().trim() !== '');
    const hasDataI = tableData.some(item => item.row[7] && item.row[7].toString().trim() !== '');
    const hasDataJ = tableData.some(item => item.row[8] && item.row[8].toString().trim() !== '');
    const hasDataM = tableData.some(item => item.row[11] && item.row[11].toString().trim() !== '');

    return {
      d4Value,
      l4Value,
      ghichuKT,
      d3: new Date().toLocaleDateString('vi-VN', { day: '2-digit', month: '2-digit', year: 'numeric' }),
      d5Values,
      h5Values,
      h6Values,
      Loaiycthuchien,
      d6Values,
      tableData,
      summaryDataB,
      summaryDataC,
      hasDataF,
      hasDataG,
      hasDataH,
      hasDataI,
      hasDataJ,
      hasDataM,
      lastRowWithData
    };

  } catch (err) {
    console.error('❌ Lỗi trong prepareYcvtData:', err.stack || err.message);
    throw err;
  }
}

export { prepareYcvtData };
