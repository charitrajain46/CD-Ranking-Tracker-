// ═══════════════════════════════════════════════════════════════
//  quick_run_apps_script.js
//  ─────────────────────────────────────────────────────────────
//  QUICK RUN — On-demand ranking for:
//    "Quick Run Intermediate"  sheet
//    "Quick Run Final"         sheet
//
//  HOW TO ADD THIS TO YOUR PROJECT:
//  ─────────────────────────────────────────────────────────────
//  1. Open your Google Spreadsheet
//  2. Extensions → Apps Script
//  3. In the left sidebar, click the + next to "Files"
//  4. Choose "Script", name it "quick_run"
//  5. Delete the empty function that appears
//  6. Paste the entire contents of this file
//  7. Click Save (Ctrl+S / Cmd+S)
//  8. Re-deploy:  Deploy → Manage deployments → edit → New version → Deploy
//     (Use the SAME deployment — no new deployment needed)
//
//  NOTE: This file shares all helper functions from your main
//  intermediate_apps_script.js (Bright Data config, BATCH_SIZE,
//  MAX_RUNTIME_MS, buildBrightDataOptions, parseRankFromResponse,
//  writeResult, updateFinalCell, detectSilo, etc.)
//  Those functions are NOT duplicated here.
// ═══════════════════════════════════════════════════════════════

var QR_INTER_SHEET   = "Quick Run Intermediate";
var QR_FINAL_SHEET   = "Quick Run Final";
var QR_RUN_START_COL = 6;   // Quick Run Final always = Run-1 (cols F–R)


// ───────────────────────────────────────────────────────────────
//  MAIN ENTRY — called by quick_run.py
// ───────────────────────────────────────────────────────────────

function checkQuickRunRanks() {
  var ss         = SpreadsheetApp.getActiveSpreadsheet();
  var sheet      = ss.getSheetByName(QR_INTER_SHEET);
  var finalSheet = ss.getSheetByName(QR_FINAL_SHEET);
  var startTime  = Date.now();

  if (!sheet) {
    SpreadsheetApp.getUi().alert(
      "Quick Run Error",
      "Sheet '" + QR_INTER_SHEET + "' not found.\nRun quick_run.py first to create it.",
      SpreadsheetApp.getUi().ButtonSet.OK
    );
    return;
  }

  // Prevent date-format corruption in Quick Run Final
  ensureQRFinalColumnFormats(finalSheet, QR_RUN_START_COL);

  // ── Build Final row cache (cid|crsId → sheet row number) ────
  // IMPORTANT: key separator must be single "|" to match updateFinalCell() in Code.gs
  var finalRowCache = {};
  if (finalSheet) {
    var fData = finalSheet.getDataRange().getValues();
    for (var fi = 1; fi < fData.length; fi++) {
      var fCid = String(fData[fi][0] || "").trim();
      var fCrs = String(fData[fi][1] || "").trim();
      if (fCid && fCrs) {
        finalRowCache[fCid + "|" + fCrs] = fi + 1;  // 1-based row — single pipe matches updateFinalCell
      }
    }
  }

  // ── Build pending list (unranked + ERROR rows) ─────────────
  var data    = sheet.getDataRange().getValues();
  var pending = [];

  for (var i = 1; i < data.length; i++) {
    var row     = data[i];
    var keyword = String(row[4] || "").trim();
    var rank    = String(row[5] || "").trim();
    if (!keyword) continue;
    if (rank && rank !== "ERROR") continue;   // skip already-ranked rows
    pending.push({
      sheetRow : i + 1,
      keyword  : keyword,
      colId    : String(row[0] || "").trim(),
      crsId    : String(row[1] || "").trim()
    });
    if (Date.now() - startTime > MAX_RUNTIME_MS) break;
  }

  if (pending.length === 0) {
    SpreadsheetApp.getUi().alert("Quick Run: All rows are already ranked!");
    return;
  }

  // ── Parallel fetchAll loop ──────────────────────────────────
  var checked = 0, found = 0, errors = 0;

  for (var b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) break;

    var batch    = pending.slice(b, b + BATCH_SIZE);
    var requests = batch.map(function(item) {
      return buildBrightDataOptions(item.keyword);
    });

    var responses;
    try {
      responses = UrlFetchApp.fetchAll(requests);
    } catch(e) {
      errors += batch.length;
      Utilities.sleep(BATCH_SLEEP_MS);
      continue;
    }

    var timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    for (var j = 0; j < batch.length; j++) {
      var item   = batch[j];
      var result = parseRankFromResponse(responses[j]);

      if (result.error) {
        sheet.getRange(item.sheetRow, 6).setValue("ERROR");
        errors++;
        continue;
      }

      var display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);

      if (finalSheet && item.colId && item.crsId) {
        var silo = detectSilo(item.keyword);
        updateFinalCell(
          finalSheet, finalRowCache,
          item.colId, item.crsId,
          silo, display, result.url || "",
          timestamp, QR_RUN_START_COL
        );
      }
      checked++;
      if (result.rank !== null) found++;
    }

    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }

  // ── Recheck NOT_FOUND rows ──────────────────────────────────
  qrRecheckNotFound(sheet, finalSheet, finalRowCache, startTime);

  // ── Recheck CLEARED rows ────────────────────────────────────
  qrRecheckCleared(sheet, finalSheet, finalRowCache, startTime);

  // ── One-pass error retry ────────────────────────────────────
  var retryResult = qrRetryErrorRows(sheet, finalSheet, finalRowCache, startTime);

  SpreadsheetApp.getUi().alert(
    "Quick Run Complete!\n\n" +
    "Checked  : " + checked + " rows\n"     +
    "Found    : " + found   + " ranked\n"   +
    "Errors   : " + errors  + "\n"          +
    "Retried  : " + retryResult.retried  + " errors  →  " +
                   retryResult.recovered + " recovered"
  );
}


// ───────────────────────────────────────────────────────────────
//  RECHECK NOT_FOUND rows in Quick Run Intermediate
// ───────────────────────────────────────────────────────────────

function qrRecheckNotFound(sheet, finalSheet, finalRowCache, startTime) {
  var data    = sheet.getDataRange().getValues();
  var pending = [];

  for (var i = 1; i < data.length; i++) {
    var keyword = String(data[i][4] || "").trim();
    var rank    = String(data[i][5] || "").trim();
    if (!keyword || rank !== "NOT_FOUND") continue;
    pending.push({
      sheetRow : i + 1,
      keyword  : keyword,
      colId    : String(data[i][0] || "").trim(),
      crsId    : String(data[i][1] || "").trim()
    });
  }
  if (pending.length === 0) return;

  for (var b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) break;

    var batch    = pending.slice(b, b + BATCH_SIZE);
    var requests = batch.map(function(item) {
      return buildBrightDataOptions(item.keyword);
    });
    var responses;
    try { responses = UrlFetchApp.fetchAll(requests); }
    catch(e) { Utilities.sleep(BATCH_SLEEP_MS); continue; }

    var timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );
    for (var j = 0; j < batch.length; j++) {
      var item   = batch[j];
      var result = parseRankFromResponse(responses[j]);
      if (result.error || result.rank === null) continue;
      var display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);
      if (finalSheet && item.colId && item.crsId) {
        updateFinalCell(
          finalSheet, finalRowCache,
          item.colId, item.crsId,
          detectSilo(item.keyword), display, result.url || "",
          timestamp, QR_RUN_START_COL
        );
      }
    }
    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }
}


// ───────────────────────────────────────────────────────────────
//  RECHECK CLEARED rows in Quick Run Intermediate
// ───────────────────────────────────────────────────────────────

function qrRecheckCleared(sheet, finalSheet, finalRowCache, startTime) {
  var data    = sheet.getDataRange().getValues();
  var pending = [];

  for (var i = 1; i < data.length; i++) {
    var keyword = String(data[i][4] || "").trim();
    var rank    = String(data[i][5] || "").trim();
    if (!keyword || rank !== "CLEARED") continue;
    pending.push({
      sheetRow : i + 1,
      keyword  : keyword,
      colId    : String(data[i][0] || "").trim(),
      crsId    : String(data[i][1] || "").trim()
    });
  }
  if (pending.length === 0) return;

  for (var b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) break;

    var batch    = pending.slice(b, b + BATCH_SIZE);
    var requests = batch.map(function(item) {
      return buildBrightDataOptions(item.keyword);
    });
    var responses;
    try { responses = UrlFetchApp.fetchAll(requests); }
    catch(e) { Utilities.sleep(BATCH_SLEEP_MS); continue; }

    var timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );
    for (var j = 0; j < batch.length; j++) {
      var item   = batch[j];
      var result = parseRankFromResponse(responses[j]);
      if (result.error) continue;
      var display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);
      if (finalSheet && item.colId && item.crsId) {
        updateFinalCell(
          finalSheet, finalRowCache,
          item.colId, item.crsId,
          detectSilo(item.keyword), display, result.url || "",
          timestamp, QR_RUN_START_COL
        );
      }
    }
    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }
}


// ───────────────────────────────────────────────────────────────
//  ONE-PASS ERROR RETRY for Quick Run
// ───────────────────────────────────────────────────────────────

function qrRetryErrorRows(sheet, finalSheet, finalRowCache, startTime) {
  var data    = sheet.getDataRange().getValues();
  var pending = [];

  for (var i = 1; i < data.length; i++) {
    var keyword = String(data[i][4] || "").trim();
    var rank    = String(data[i][5] || "").trim();
    if (!keyword || rank !== "ERROR") continue;
    pending.push({
      sheetRow : i + 1,
      keyword  : keyword,
      colId    : String(data[i][0] || "").trim(),
      crsId    : String(data[i][1] || "").trim()
    });
  }

  var retried = 0, recovered = 0;
  if (pending.length === 0) return { retried: retried, recovered: recovered };

  for (var b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) break;

    var batch    = pending.slice(b, b + BATCH_SIZE);
    var requests = batch.map(function(item) {
      return buildBrightDataOptions(item.keyword);
    });
    var responses;
    try { responses = UrlFetchApp.fetchAll(requests); }
    catch(e) { Utilities.sleep(BATCH_SLEEP_MS); continue; }

    var timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );
    for (var j = 0; j < batch.length; j++) {
      retried++;
      var item   = batch[j];
      var result = parseRankFromResponse(responses[j]);
      if (result.error) continue;   // leave as ERROR — manual fix needed
      var display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);
      if (finalSheet && item.colId && item.crsId) {
        updateFinalCell(
          finalSheet, finalRowCache,
          item.colId, item.crsId,
          detectSilo(item.keyword), display, result.url || "",
          timestamp, QR_RUN_START_COL
        );
      }
      recovered++;
    }
    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }

  return { retried: retried, recovered: recovered };
}


// ───────────────────────────────────────────────────────────────
//  FORMAT GUARD — prevent date corruption in Quick Run Final
// ───────────────────────────────────────────────────────────────

function ensureQRFinalColumnFormats(finalSheet, runStartCol) {
  if (!finalSheet) return;
  try {
    var lastRow = finalSheet.getLastRow();
    if (lastRow <= 1) return;
    finalSheet.getRange(2, runStartCol, lastRow - 1, 13).setNumberFormat("@");
  } catch(e) {
    Logger.log("ensureQRFinalColumnFormats error: " + e.message);
  }
}
