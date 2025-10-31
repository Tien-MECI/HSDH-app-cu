// helpers/googleSheet.js
import { google } from "googleapis";

export async function getSheetValues(sheets, spreadsheetId, range) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range,
  });
  return res.data.values || [];
}
