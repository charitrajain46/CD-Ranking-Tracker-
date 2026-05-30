// ============================================================
// Collegedunia SERP Rank Checker — Multi-Run Edition
// ============================================================
// Each pipeline run adds a new 13-column group to Final:
//   Run 1: cols F–R   (run_start_col = 6)
//   Run 2: cols S–AE  (run_start_col = 19)
//   Run 3: cols AF–AR (run_start_col = 32)  … and so on
//
// phase1_populate.py writes run_start_col into Source!Z1 before
// each run. Apps Script reads it here — so it always writes to
// the correct column group without any manual change needed.
//
// Intermediate tab (never changes):
//   A=college_id  B=course_id  C=college_name  D=course_name
//   E=keyword  F=Rank  G=Found URL  H=updated_at
//
// Final tab grows rightward each run:
//   A–E  fixed identity cols (College_Id … Keywords)
//   F–R  Run 1 silo data (Admissions … Updated_at)
//   S–AE Run 2 silo data
//   …
// ============================================================

const BRIGHTDATA_API_TOKEN = "PASTE_YOUR_API_TOKEN_HERE";   // ← fill this
const BRIGHTDATA_ZONE      = "article_creation_content";
const TARGET_URL_PATTERN   = "collegedunia.com";

// ─── Tab names ───────────────────────────────────────────────
const INTER_TAB   = "Intermediate";
const FINAL_TAB   = "Final";
const SOURCE_TAB  = "Source";
const CONTENT_TAB = "Content";

// ─── Intermediate column positions (1-based) ─────────────────
const I_COLLEGE_ID   = 1;   // A
const I_COURSE_ID    = 2;   // B
const I_COLLEGE_NAME = 3;   // C
const I_COURSE_NAME  = 4;   // D
const I_KEYWORD      = 5;   // E
const I_RANK         = 6;   // F
const I_URL          = 7;   // G
const I_TIME         = 8;   // H

// ─── Final tab: BASE silo → column positions for Run 1 (1-based) ─
// For Run N, add offset = (run_start_col - 6) to each value.
const SILO_RANK_COL = {
  "Admissions":     6,   // F (run 1)
  "Fees":           8,   // H
  "Placements":    10,   // J
  "Scholarships":  12,   // L
  "Main":          14,   // N
  "Single_Course": 16,   // P
};
const SILO_URL_COL = {
  "Admissions":     7,   // G
  "Fees":           9,   // I
  "Placements":    11,   // K
  "Scholarships":  13,   // M
  "Main":          15,   // O
  "Single_Course": 17,   // Q
};
const FINAL_UPDATED_COL = 18;   // R — Updated_at (run 1 base)

// ─── Intermediate header ──────────────────────────────────────
const INTER_HEADER = [
  "college_id", "course_id", "college_name", "course_name",
  "keyword", "Rank", "Found URL", "updated_at",
];

const TOP_N          = 50;
const START_ROW      = 2;
const DELAY_MS       = 1000;   // kept for testFirst5 / runAndWrite (single-row helpers)
const COUNTRY        = "in";
const LANGUAGE       = "en";
const MAX_RUNTIME_MS = 5.5 * 60 * 1000;

// ─── Parallel batch settings ──────────────────────────────────
// BATCH_SIZE  : keywords fired simultaneously per fetchAll call.
//               10 is safe; raise to 20 only if Bright Data confirms higher concurrency.
// BATCH_SLEEP : ms pause between batches — gives Bright Data breathing room.
const BATCH_SIZE     = 10;
const BATCH_SLEEP_MS = 500;


// ============================================================
// RUN START COLUMN — reads from Source!Z1
// ============================================================

/**
 * Returns the 1-based column number in Final where the CURRENT run's
 * silo data starts. Written by phase1_populate.py into Source!Z1.
 *   Run 1 → 6  (col F)
 *   Run 2 → 19 (col S)
 *   Run 3 → 32 (col AF)
 */
function getRunStartCol() {
  try {
    var ss  = SpreadsheetApp.getActiveSpreadsheet();
    var src = ss.getSheetByName(SOURCE_TAB);
    if (!src) return 6;
    var val = src.getRange("Z1").getValue();
    var n   = parseInt(val);
    return (n > 0) ? n : 6;
  } catch(e) {
    return 6;   // default: first run
  }
}


// ============================================================
// CUSTOM MENU
// ============================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Rank Checker")
    .addItem("▶ Run Batch", "checkRanksBatched")
    .addItem("⏩ Start Auto-Run (every 10 min)", "startAutoRun")
    .addItem("⏹ Stop Auto-Run", "stopAutoRun")
    .addSeparator()
    .addItem("🔄 Sync to Final Sheet (manual)", "syncToFinal")
    .addItem("✅ Enable Auto-Sync on Delete", "enableAutoSync")
    .addItem("❌ Disable Auto-Sync on Delete", "disableAutoSync")
    .addSeparator()
    .addItem("↺ Re-check Not In Top 50 rows", "recheckNotFound")
    .addItem("↺ Re-check Empty rows", "recheckCleared")
    .addSeparator()
    .addItem("🔌 Test Connection", "testConnection")
    .addItem("🔎 Test First 5 Rows", "testFirst5")
    .addItem("🐛 Debug Row 2", "debugOneRow")
    .addToUi();
}


// ============================================================
// SILO DETECTION FROM KEYWORD SUFFIX
// ============================================================

function detectSilo(keyword) {
  const kw = String(keyword).trim();
  if (kw.endsWith(" Admissions"))   return "Admissions";
  if (kw.endsWith(" Fees"))         return "Fees";
  if (kw.endsWith(" Placements"))   return "Placements";
  if (kw.endsWith(" Scholarships")) return "Scholarships";
  if (kw.endsWith(")"))             return "Single_Course";
  return "Main";
}


// ============================================================
// SHEET / TAB HELPERS
// ============================================================

function getInterSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(INTER_TAB);
  if (!sh) throw new Error(`Tab "${INTER_TAB}" not found. Run setup.py first.`);
  return sh;
}

function getFinalSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(FINAL_TAB);
  if (!sh) {
    sh = ss.insertSheet(FINAL_TAB);
  }
  // Only write the base 18-col header if the sheet is COMPLETELY empty.
  // phase1_populate.py manages the header for multi-run cases — don't override.
  const lastCol = sh.getLastColumn();
  const lastRow = sh.getLastRow();
  if (lastRow === 0 || lastCol === 0) {
    const baseHeader = [
      "College_Id","Course_Id","College_Name","Course_Name","Keywords",
      "Admissions","Admissions_URL","Fees","Fees_URL",
      "Placements","Placements_URL","Scholarships","Scholarships_URL",
      "Main","Main_URL","Single_Course","Single_Course_URL","Updated_at",
    ];
    sh.getRange(1, 1, 1, baseHeader.length).setValues([baseHeader]);
    formatFinalHeader(sh, baseHeader.length);
  }
  return sh;
}

function ensureInterHeader(sheet) {
  const firstRow = sheet.getRange(1, 1, 1, INTER_HEADER.length).getValues()[0];
  if (firstRow.every(v => v === "" || v === null)) {
    sheet.getRange(1, 1, 1, INTER_HEADER.length).setValues([INTER_HEADER]);
    sheet.getRange(1, 1, 1, INTER_HEADER.length).setFontWeight("bold");
  }
}

function formatFinalHeader(sheet, numCols) {
  const n = numCols || sheet.getLastColumn();
  if (n <= 0) return;
  const headerRange = sheet.getRange(1, 1, 1, n);
  headerRange.setFontWeight("bold");
  headerRange.setBackground("#4a86e8");
  headerRange.setFontColor("#ffffff");
  sheet.setFrozenRows(1);
  sheet.setFrozenColumns(4);
}


// ============================================================
// FINAL TAB — LIVE PER-ROW UPDATE HELPERS
// ============================================================

/**
 * Force all 13 columns of the current run's group to "Plain text" format.
 * Prevents Google Sheets from auto-applying Date format to rank/URL columns
 * when numbers or date-like strings are written there.
 * Called ONCE per batch function — single range call, very fast.
 */
function ensureFinalColumnFormats(finalSheet, runStartCol) {
  try {
    const lastRow = finalSheet.getLastRow();
    if (lastRow <= 1) return;
    // 13 columns per run group: 6 rank + 6 URL + 1 Updated_at
    finalSheet.getRange(2, runStartCol, lastRow - 1, 13).setNumberFormat("@");
  } catch(e) {
    Logger.log("ensureFinalColumnFormats error: " + e.message);
  }
}


/**
 * Build a lookup map: "college_id|course_id" → row number in Final.
 * Called once per batch for fast targeted writes.
 */
function buildFinalRowCache(finalSheet) {
  const cache = {};
  try {
    const lastRow = finalSheet.getLastRow();
    if (lastRow <= 1) return cache;
    const data = finalSheet.getRange(2, 1, lastRow - 1, 2).getValues();
    for (let i = 0; i < data.length; i++) {
      const colId = String(data[i][0]).trim();
      const crsId = String(data[i][1]).trim();
      if (colId && crsId) {
        cache[colId + "|" + crsId] = i + 2;   // 1-based sheet row
      }
    }
  } catch(e) {
    Logger.log("buildFinalRowCache error: " + e.message);
  }
  return cache;
}

/**
 * Write one ranking result into the CURRENT RUN's column in Final.
 * runStartCol tells us which column group this run uses.
 *   Run 1: offset = 0  (writes to F, G, R …)
 *   Run 2: offset = 13 (writes to S, T, AE …)
 */
function updateFinalCell(finalSheet, finalRowCache, colId, crsId, silo, rank, url, time, runStartCol) {
  if (!finalSheet || !finalRowCache) return;
  const key      = colId + "|" + crsId;
  const finalRow = finalRowCache[key];
  if (!finalRow) return;   // pair not in Final yet

  const offset     = (runStartCol || 6) - 6;
  const rankCol    = SILO_RANK_COL[silo];
  const urlCol     = SILO_URL_COL[silo];
  if (!rankCol) return;   // unknown silo

  const targetRankCol    = rankCol    + offset;
  const targetUrlCol     = urlCol     + offset;
  const targetUpdatedCol = FINAL_UPDATED_COL + offset;

  try {
    finalSheet.getRange(finalRow, targetRankCol).setValue(rank);
    finalSheet.getRange(finalRow, targetUrlCol).setValue(url || "");
    finalSheet.getRange(finalRow, targetUpdatedCol).setValue(time);

    // Colour-code the rank cell
    const cell    = finalSheet.getRange(finalRow, targetRankCol);
    const rankStr = String(rank).trim();
    if (rankStr === "Not in Top 50" || rankStr === "") {
      cell.setBackground("#f2f2f2");
    } else {
      const num = parseInt(rankStr);
      if      (num <= 3)  cell.setBackground("#c6efce");
      else if (num <= 10) cell.setBackground("#bdd7ee");
      else if (num <= 20) cell.setBackground("#ffeb9c");
      else                cell.setBackground("#ffc7ce");
    }
  } catch(e) {
    Logger.log("updateFinalCell error: " + e.message);
  }
}


// ============================================================
// MAIN BATCH RUNNER
// ============================================================

function checkRanksBatched() {
  if (!checkCredentials()) return;

  // Read run_start_col ONCE — all rows in this batch write to the same column group
  const runStartCol   = getRunStartCol();
  Logger.log("checkRanksBatched: runStartCol = " + runStartCol);

  const sheet         = getInterSheet();
  ensureInterHeader(sheet);
  const lastRow       = sheet.getLastRow();
  const startTime     = Date.now();

  const finalSheet    = getFinalSheet();
  const finalRowCache = buildFinalRowCache(finalSheet);
  ensureFinalColumnFormats(finalSheet, runStartCol);   // prevent date-format corruption

  if (lastRow < START_ROW) return;

  // ── ONE bulk read ────────────────────────────────────────────
  const allData = sheet.getRange(START_ROW, 1, lastRow - START_ROW + 1, I_TIME).getValues();

  // ── Build pending list (unranked / error rows only) ──────────
  const pending = [];
  for (let i = 0; i < allData.length; i++) {
    const q        = String(allData[i][I_KEYWORD - 1] || "").trim();
    const existing = allData[i][I_RANK - 1];
    const colId    = String(allData[i][I_COLLEGE_ID - 1] || "").trim();
    const crsId    = String(allData[i][I_COURSE_ID  - 1] || "").trim();
    if (!q) continue;
    if (existing !== "" && existing !== null && String(existing).indexOf("ERROR") === -1) continue;
    pending.push({ sheetRow: START_ROW + i, keyword: q, colId: colId, crsId: crsId });
  }

  Logger.log("checkRanksBatched: " + pending.length + " rows to process in batches of " + BATCH_SIZE);

  let checked = 0, found = 0, errors = 0;

  // ── Parallel batches via fetchAll ────────────────────────────
  for (let b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) {
      Logger.log("Stopped at batch offset " + b + "/" + pending.length + " — run again to resume.");
      break;
    }

    const batch    = pending.slice(b, b + BATCH_SIZE);
    const requests = batch.map(function(item) { return buildBrightDataOptions(item.keyword); });

    let responses;
    try {
      responses = UrlFetchApp.fetchAll(requests);
    } catch(e) {
      Logger.log("fetchAll error (batch " + b + "): " + e.message);
      errors += batch.length;
      Utilities.sleep(BATCH_SLEEP_MS);
      continue;
    }

    const timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    for (let j = 0; j < batch.length; j++) {
      const item   = batch[j];
      const result = parseRankFromResponse(responses[j]);

      if (result.error) {
        sheet.getRange(item.sheetRow, I_RANK).setValue("ERROR");
        sheet.getRange(item.sheetRow, I_URL ).setValue(result.error.substring(0, 150));
        sheet.getRange(item.sheetRow, I_TIME).setValue(timestamp);
        errors++;
        continue;
      }

      const display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);

      if (finalSheet && finalRowCache && item.colId && item.crsId) {
        const silo = detectSilo(item.keyword);
        updateFinalCell(finalSheet, finalRowCache, item.colId, item.crsId,
                        silo, display, result.url || "", timestamp, runStartCol);
      }

      checked++;
      if (result.rank !== null) found++;
    }

    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }

  // ── One retry pass for any ERROR rows ───────────────────────
  // Each error row gets exactly one more attempt. Success → replaced.
  // Still failing → left as ERROR for manual correction.
  const retry = retryErrorRows(sheet, finalSheet, finalRowCache, runStartCol, startTime);
  if (retry.retried > 0) {
    Logger.log("Retry pass: " + retry.retried + " retried, " + retry.recovered + " recovered, " +
               (retry.retried - retry.recovered) + " still ERROR (fix manually)");
  }

  removeAutoTrigger();

  try {
    let msg = "✓ Batch complete!\nChecked: " + checked + "  Found: " + found + "  Errors: " + errors;
    if (retry.retried > 0) {
      msg += "\n\n↺ Retry pass: " + retry.recovered + "/" + retry.retried + " errors recovered";
      if (retry.retried - retry.recovered > 0) {
        msg += "\n  " + (retry.retried - retry.recovered) + " still ERROR → fix manually";
      }
    }
    msg += "\n\n✓ Rankings written to run column group starting at col " + runStartCol + ".";
    SpreadsheetApp.getUi().alert(msg);
  } catch(e) {}
}


// ============================================================
// SYNC INTERMEDIATE → FINAL (non-destructive, current run only)
// ============================================================

/**
 * Sync current run's rankings from Intermediate into Final.
 * ONLY updates the current run's column group — previous run data is preserved.
 * Does NOT delete any rows from Final.
 */
function syncToFinal() {
  try {
    const ss          = SpreadsheetApp.getActiveSpreadsheet();
    const interSheet  = ss.getSheetByName(INTER_TAB);
    if (!interSheet) return;

    const runStartCol  = getRunStartCol();
    const offset       = runStartCol - 6;
    const finalSheet   = getFinalSheet();
    const finalRowCache = buildFinalRowCache(finalSheet);
    const lastInterRow = interSheet.getLastRow();

    if (lastInterRow <= 1) {
      Logger.log("syncToFinal: Intermediate is empty — nothing to sync.");
      return;
    }

    const interData = interSheet.getRange(START_ROW, 1, lastInterRow - 1, I_TIME).getValues();

    // Aggregate by (college_id, course_id) key
    const map = {};
    for (const row of interData) {
      const colId  = String(row[I_COLLEGE_ID   - 1]).trim();
      const crsId  = String(row[I_COURSE_ID    - 1]).trim();
      if (!colId && !crsId) continue;

      const key = colId + "|" + crsId;
      if (!map[key]) {
        map[key] = {
          Admissions: { rank: "", url: "" }, Fees:    { rank: "", url: "" },
          Placements: { rank: "", url: "" }, Scholarships: { rank: "", url: "" },
          Main:       { rank: "", url: "" }, Single_Course: { rank: "", url: "" },
          last_updated: "",
        };
      }

      const keyword = String(row[I_KEYWORD - 1]).trim();
      const rank    = row[I_RANK - 1];
      const url     = String(row[I_URL  - 1] || "").trim();
      const time    = String(row[I_TIME - 1] || "").trim();
      const silo    = detectSilo(keyword);
      const rankStr = String(rank).trim();

      if (rankStr !== "" && rankStr !== "null" && rankStr.indexOf("ERROR") === -1) {
        map[key][silo] = { rank: rank, url: url };
        if (time && time > map[key].last_updated) {
          map[key].last_updated = time;
        }
      }
    }

    // Write current run's columns into Final (leave all other columns untouched)
    let updated = 0;
    for (const key of Object.keys(map)) {
      const finalRow = finalRowCache[key];
      if (!finalRow) continue;

      const d = map[key];
      const silos = ["Admissions","Fees","Placements","Scholarships","Main","Single_Course"];
      for (const silo of silos) {
        const rankCol = SILO_RANK_COL[silo] + offset;
        const urlCol  = SILO_URL_COL[silo]  + offset;
        if (d[silo].rank !== "") {
          finalSheet.getRange(finalRow, rankCol).setValue(d[silo].rank);
          finalSheet.getRange(finalRow, urlCol).setValue(d[silo].url);
        }
      }
      if (d.last_updated) {
        finalSheet.getRange(finalRow, FINAL_UPDATED_COL + offset).setValue(d.last_updated);
      }
      updated++;
    }

    SpreadsheetApp.flush();
    Logger.log(`syncToFinal: updated ${updated} rows in run column group (col ${runStartCol}).`);
  } catch(e) {
    Logger.log("syncToFinal error: " + e.message);
  }
}


// ─── Colour-code rank columns for the current run group ──────
function colorFinalRankCols(sheet, dataRows, runStartCol) {
  if (dataRows <= 0) return;
  const offset   = (runStartCol || 6) - 6;
  const rankCols = [6, 8, 10, 12, 14, 16].map(c => c + offset);
  for (const col of rankCols) {
    try {
      const vals = sheet.getRange(2, col, dataRows, 1).getValues();
      for (let i = 0; i < vals.length; i++) {
        const v    = String(vals[i][0]).trim();
        const cell = sheet.getRange(i + 2, col);
        if (v === "" || v === "Not in Top 50") {
          cell.setBackground("#f2f2f2");
        } else {
          const num = parseInt(v);
          if      (num <= 3)  cell.setBackground("#c6efce");
          else if (num <= 10) cell.setBackground("#bdd7ee");
          else if (num <= 20) cell.setBackground("#ffeb9c");
          else                cell.setBackground("#ffc7ce");
        }
      }
    } catch(e) {}
  }
}


// ============================================================
// AUTO-SYNC ON DELETE  (installable onChange trigger)
// ============================================================

function enableAutoSync() {
  disableAutoSync(/*silent=*/true);
  ScriptApp.newTrigger("onSheetChange")
    .forSpreadsheet(SpreadsheetApp.getActiveSpreadsheet())
    .onChange()
    .create();
  SpreadsheetApp.getUi().alert(
    "✅ Auto-Sync enabled!\n\nWhen you delete rows from Intermediate, " +
    "the Final tab current-run columns will update automatically."
  );
}

function disableAutoSync(silent) {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === "onSheetChange") ScriptApp.deleteTrigger(t);
  });
  if (!silent) {
    try { SpreadsheetApp.getUi().alert("Auto-Sync disabled."); } catch(e) {}
  }
}

function onSheetChange(e) {
  if (!e) return;
  if (e.changeType === "REMOVE_ROW" ||
      e.changeType === "INSERT_ROW" ||
      e.changeType === "OTHER") {
    syncToFinal();
  }
}

function onEdit(e) {
  if (!e) return;
  const sheet = e.source.getActiveSheet();
  if (sheet.getName() !== INTER_TAB) return;
  const col = e.range.getColumn();
  if (col >= I_RANK && col <= I_TIME) {
    syncToFinal();
  }
}


// ============================================================
// AUTO-RUN (timed trigger every 10 minutes)
// ============================================================

function startAutoRun() {
  if (!checkCredentials()) return;
  removeAutoTrigger();
  ScriptApp.newTrigger("checkRanksBatched").timeBased().everyMinutes(10).create();
  SpreadsheetApp.getUi().alert(
    "✓ Auto-run enabled.\nRunning first batch now...\nTo stop: Rank Checker → Stop Auto-Run"
  );
  checkRanksBatched();
}

function stopAutoRun() {
  removeAutoTrigger();
  try { SpreadsheetApp.getUi().alert("Auto-run stopped."); } catch(e) {}
}

function removeAutoTrigger() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === "checkRanksBatched") ScriptApp.deleteTrigger(t);
  });
}


// ============================================================
// TEST & DEBUG FUNCTIONS
// ============================================================

function testConnection() {
  if (!checkCredentials()) return;
  try {
    const data = callBrightData("collegedunia admissions");
    SpreadsheetApp.getUi().alert(
      "✓ Connection working!\nResponse keys: " + Object.keys(data).join(", ") +
      "\n\n" + JSON.stringify(data).substring(0, 400)
    );
  } catch(e) {
    SpreadsheetApp.getUi().alert("✗ Connection failed: " + e.message);
  }
}

function testFirst5() {
  if (!checkCredentials()) return;
  const sheet         = getInterSheet();
  ensureInterHeader(sheet);
  const runStartCol   = getRunStartCol();
  const finalSheet    = getFinalSheet();
  const finalRowCache = buildFinalRowCache(finalSheet);
  for (let row = START_ROW; row <= Math.min(START_ROW + 4, sheet.getLastRow()); row++) {
    const q = sheet.getRange(row, I_KEYWORD).getValue();
    if (!q) continue;
    runAndWrite(sheet, row, String(q).trim(), finalSheet, finalRowCache, runStartCol);
    Utilities.sleep(DELAY_MS);
  }
  SpreadsheetApp.getUi().alert("Test done! Check rows 2–6 in Intermediate and the Final tab.");
}

function recheckNotFound() {
  if (!checkCredentials()) return;
  const sheet         = getInterSheet();
  const runStartCol   = getRunStartCol();
  const finalSheet    = getFinalSheet();
  const finalRowCache = buildFinalRowCache(finalSheet);
  ensureFinalColumnFormats(finalSheet, runStartCol);   // prevent date-format corruption
  const startTime     = Date.now();
  const lastRow       = sheet.getLastRow();

  if (lastRow < START_ROW) return;
  const allData = sheet.getRange(START_ROW, 1, lastRow - START_ROW + 1, I_TIME).getValues();

  // ── Build pending list (only "Not in Top 50" rows) ───────────
  const pending = [];
  for (let i = 0; i < allData.length; i++) {
    const q     = String(allData[i][I_KEYWORD - 1] || "").trim();
    const rank  = String(allData[i][I_RANK    - 1] || "").trim();
    const colId = String(allData[i][I_COLLEGE_ID - 1] || "").trim();
    const crsId = String(allData[i][I_COURSE_ID  - 1] || "").trim();
    if (!q || rank !== "Not in Top 50") continue;
    pending.push({ sheetRow: START_ROW + i, keyword: q, colId: colId, crsId: crsId });
  }

  let checked = 0, found = 0, errors = 0;

  for (let b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) {
      Logger.log("recheckNotFound: stopped at batch " + b + " — run again to resume.");
      break;
    }

    const batch    = pending.slice(b, b + BATCH_SIZE);
    const requests = batch.map(function(item) { return buildBrightDataOptions(item.keyword); });

    let responses;
    try {
      responses = UrlFetchApp.fetchAll(requests);
    } catch(e) {
      Logger.log("recheckNotFound fetchAll error: " + e.message);
      errors += batch.length;
      Utilities.sleep(BATCH_SLEEP_MS);
      continue;
    }

    const timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    for (let j = 0; j < batch.length; j++) {
      const item   = batch[j];
      const result = parseRankFromResponse(responses[j]);

      if (result.error) {
        sheet.getRange(item.sheetRow, I_RANK).setValue("ERROR");
        sheet.getRange(item.sheetRow, I_URL ).setValue(result.error.substring(0, 150));
        sheet.getRange(item.sheetRow, I_TIME).setValue(timestamp);
        errors++;
        continue;
      }

      const display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);

      if (finalSheet && finalRowCache && item.colId && item.crsId) {
        const silo = detectSilo(item.keyword);
        updateFinalCell(finalSheet, finalRowCache, item.colId, item.crsId,
                        silo, display, result.url || "", timestamp, runStartCol);
      }

      checked++;
      if (result.rank !== null) found++;
    }

    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }

  try {
    SpreadsheetApp.getUi().alert(
      "Re-checked: " + checked + "  Newly found: " + found + "  Errors: " + errors
    );
  } catch(e) {}
}

function recheckCleared() {
  if (!checkCredentials()) return;
  const sheet         = getInterSheet();
  ensureInterHeader(sheet);
  const runStartCol   = getRunStartCol();
  const finalSheet    = getFinalSheet();
  const finalRowCache = buildFinalRowCache(finalSheet);
  ensureFinalColumnFormats(finalSheet, runStartCol);   // prevent date-format corruption
  const startTime     = Date.now();
  const lastRow       = sheet.getLastRow();

  if (lastRow < START_ROW) return;
  const allData = sheet.getRange(START_ROW, 1, lastRow - START_ROW + 1, I_TIME).getValues();

  // ── Build pending list (only empty rank rows) ────────────────
  const pending = [];
  for (let i = 0; i < allData.length; i++) {
    const q        = String(allData[i][I_KEYWORD - 1] || "").trim();
    const existing = allData[i][I_RANK - 1];
    const colId    = String(allData[i][I_COLLEGE_ID - 1] || "").trim();
    const crsId    = String(allData[i][I_COURSE_ID  - 1] || "").trim();
    if (!q || (existing !== "" && existing !== null)) continue;
    pending.push({ sheetRow: START_ROW + i, keyword: q, colId: colId, crsId: crsId });
  }

  let checked = 0, found = 0, errors = 0;

  for (let b = 0; b < pending.length; b += BATCH_SIZE) {
    if (Date.now() - startTime > MAX_RUNTIME_MS) {
      Logger.log("recheckCleared: stopped at batch " + b + " — run again to resume.");
      break;
    }

    const batch    = pending.slice(b, b + BATCH_SIZE);
    const requests = batch.map(function(item) { return buildBrightDataOptions(item.keyword); });

    let responses;
    try {
      responses = UrlFetchApp.fetchAll(requests);
    } catch(e) {
      Logger.log("recheckCleared fetchAll error: " + e.message);
      errors += batch.length;
      Utilities.sleep(BATCH_SLEEP_MS);
      continue;
    }

    const timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    for (let j = 0; j < batch.length; j++) {
      const item   = batch[j];
      const result = parseRankFromResponse(responses[j]);

      if (result.error) {
        sheet.getRange(item.sheetRow, I_RANK).setValue("ERROR");
        sheet.getRange(item.sheetRow, I_URL ).setValue(result.error.substring(0, 150));
        sheet.getRange(item.sheetRow, I_TIME).setValue(timestamp);
        errors++;
        continue;
      }

      const display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);

      if (finalSheet && finalRowCache && item.colId && item.crsId) {
        const silo = detectSilo(item.keyword);
        updateFinalCell(finalSheet, finalRowCache, item.colId, item.crsId,
                        silo, display, result.url || "", timestamp, runStartCol);
      }

      checked++;
      if (result.rank !== null) found++;
    }

    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }

  try {
    SpreadsheetApp.getUi().alert(
      "Re-checked " + checked + " empty rows  Found: " + found + "  Errors: " + errors
    );
  } catch(e) {}
}

function debugOneRow() {
  if (!checkCredentials()) return;
  const DEBUG_ROW = 2;
  const sheet = getInterSheet();
  const q     = String(sheet.getRange(DEBUG_ROW, I_KEYWORD).getValue()).trim();
  Logger.log("Keyword: " + q);
  const data = callBrightData(q);
  Logger.log("Response keys: " + Object.keys(data).join(", "));
  Logger.log("\nAll matches for '" + TARGET_URL_PATTERN + "':");
  const hits = [];
  findInJson(data, "", TARGET_URL_PATTERN.toLowerCase(), hits);
  if (hits.length === 0) Logger.log("  → Not found in response.");
  else hits.forEach(h => Logger.log("  " + h));
  SpreadsheetApp.getUi().alert("Debug done — open Logs (View → Logs) to see details.");
}


// ============================================================
// HELPERS
// ============================================================

function checkCredentials() {
  if (BRIGHTDATA_API_TOKEN === "PASTE_YOUR_API_TOKEN_HERE") {
    SpreadsheetApp.getUi().alert(
      "Please paste your Bright Data API token into BRIGHTDATA_API_TOKEN at the top of the script."
    );
    return false;
  }
  return true;
}

function isMatch(url) {
  if (!url) return false;
  return url.toLowerCase().includes(TARGET_URL_PATTERN.toLowerCase());
}

/**
 * Fetch ranking, write to Intermediate, and immediately update Final.
 * runStartCol determines which column group in Final to write to.
 */
function runAndWrite(sheet, row, question, finalSheet, finalRowCache, runStartCol) {
  try {
    const result  = serpSearch(question);
    const display = result.rank !== null
      ? (result.section === "organic"
          ? result.rank
          : result.rank + " (" + result.section + ")")
      : "Not in Top 50";

    const timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    // Write to Intermediate
    sheet.getRange(row, I_RANK).setValue(display);
    sheet.getRange(row, I_URL).setValue(result.url || "");
    sheet.getRange(row, I_TIME).setValue(timestamp);

    // Colour-code Intermediate rank cell
    const cell = sheet.getRange(row, I_RANK);
    if (result.rank !== null) {
      if      (result.rank <= 3)  cell.setBackground("#c6efce");
      else if (result.rank <= 10) cell.setBackground("#bdd7ee");
      else if (result.rank <= 20) cell.setBackground("#ffeb9c");
      else                        cell.setBackground("#ffc7ce");
    } else {
      cell.setBackground("#f2f2f2");
    }

    // ── Live update of the Final tab (current run's column group) ──
    if (finalSheet && finalRowCache) {
      const colId = String(sheet.getRange(row, I_COLLEGE_ID).getValue()).trim();
      const crsId = String(sheet.getRange(row, I_COURSE_ID).getValue()).trim();
      const silo  = detectSilo(question);
      updateFinalCell(finalSheet, finalRowCache, colId, crsId, silo, display, result.url || "", timestamp, runStartCol);
    }

    SpreadsheetApp.flush();
    return result;
  } catch(e) {
    sheet.getRange(row, I_RANK).setValue("ERROR");
    sheet.getRange(row, I_URL).setValue(e.message.substring(0, 150));
    return { error: e.message };
  }
}


/**
 * Same as runAndWrite but accepts pre-read colId/crsId to skip
 * 2 extra individual cell reads per row (saves ~50-80ms per row).
 */
function runAndWriteFast(sheet, row, question, colId, crsId, finalSheet, finalRowCache, runStartCol) {
  try {
    const result  = serpSearch(question);
    const display = result.rank !== null
      ? (result.section === "organic"
          ? result.rank
          : result.rank + " (" + result.section + ")")
      : "Not in Top 50";

    const timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    sheet.getRange(row, I_RANK).setValue(display);
    sheet.getRange(row, I_URL).setValue(result.url || "");
    sheet.getRange(row, I_TIME).setValue(timestamp);

    const cell = sheet.getRange(row, I_RANK);
    if (result.rank !== null) {
      if      (result.rank <= 3)  cell.setBackground("#c6efce");
      else if (result.rank <= 10) cell.setBackground("#bdd7ee");
      else if (result.rank <= 20) cell.setBackground("#ffeb9c");
      else                        cell.setBackground("#ffc7ce");
    } else {
      cell.setBackground("#f2f2f2");
    }

    if (finalSheet && finalRowCache && colId && crsId) {
      const silo = detectSilo(question);
      updateFinalCell(finalSheet, finalRowCache, colId, crsId, silo, display, result.url || "", timestamp, runStartCol);
    }

    SpreadsheetApp.flush();
    return result;
  } catch(e) {
    sheet.getRange(row, I_RANK).setValue("ERROR");
    sheet.getRange(row, I_URL).setValue(e.message.substring(0, 150));
    return { error: e.message };
  }
}


// ============================================================
// PARALLEL HELPERS  (used by checkRanksBatched, recheckNotFound,
//                    recheckCleared — NOT by single-row test fns)
// ============================================================

/**
 * Build a fetchAll-compatible request object for one keyword.
 * (Equivalent of buildOptions_ in the NCERT script.)
 */
function buildBrightDataOptions(keyword) {
  const searchUrl =
    "https://www.google.com/search?q=" + encodeURIComponent(keyword) +
    "&num=" + TOP_N +
    "&gl=" + COUNTRY +
    "&hl=" + LANGUAGE +
    "&brd_json=1";
  return {
    url:         "https://api.brightdata.com/request",
    method:      "post",
    contentType: "application/json",
    headers:     { "Authorization": "Bearer " + BRIGHTDATA_API_TOKEN },
    payload:     JSON.stringify({ zone: BRIGHTDATA_ZONE, url: searchUrl, format: "raw" }),
    muteHttpExceptions: true,
  };
}

/**
 * Find rank/url/section inside an already-parsed Bright Data JSON object.
 * Shared by both serpSearch (single) and parseRankFromResponse (batch).
 */
function findRankInData(data) {
  // 1 — Organic
  const organic = data.organic || data.organic_results || [];
  for (let i = 0; i < organic.length; i++) {
    const r    = organic[i];
    const link = r.link || r.url || "";
    if (isMatch(link)) {
      return { rank: r.rank || r.position || (i + 1), url: link, section: "organic" };
    }
    const sitelinks = r.sitelinks || [];
    for (const s of (Array.isArray(sitelinks) ? sitelinks : [])) {
      const sl = s.link || s.url || "";
      if (isMatch(sl)) {
        return { rank: r.rank || r.position || (i + 1), url: sl, section: "sitelink" };
      }
    }
  }
  // 2 — Discussions
  const discussions = data.discussions_and_forums || data.discussions || [];
  for (let i = 0; i < discussions.length; i++) {
    const link = discussions[i].link || discussions[i].url || "";
    if (isMatch(link)) return { rank: i + 1, url: link, section: "discussion" };
  }
  // 3 — PAA
  const paa = data.people_also_ask || data.related_questions || [];
  for (let i = 0; i < paa.length; i++) {
    const link = paa[i].link || paa[i].source_link || paa[i].url || "";
    if (isMatch(link)) return { rank: i + 1, url: link, section: "PAA" };
  }
  // 4 — Knowledge panel
  if (data.knowledge && data.knowledge.source) {
    const link = data.knowledge.source.link || "";
    if (isMatch(link)) return { rank: 0, url: link, section: "knowledge" };
  }
  // 5 — Featured snippet
  const fs = data.featured_snippet || data.answer_box;
  if (fs) {
    const link = fs.link || fs.url || "";
    if (isMatch(link)) return { rank: 0, url: link, section: "featured" };
  }
  return { rank: null, url: null, section: null };
}

/**
 * Parse one raw HTTPResponse from fetchAll into {rank, url, section, error}.
 * Never throws — errors are returned as { error: "..." }.
 */
function parseRankFromResponse(response) {
  try {
    const code = response.getResponseCode();
    const body = response.getContentText();
    if (code !== 200) {
      return { rank: null, url: "", section: null,
               error: "HTTP " + code + ": " + body.substring(0, 120) };
    }
    let data;
    try { data = JSON.parse(body); }
    catch(e) { return { rank: null, url: "", section: null, error: "Bad JSON" }; }
    return findRankInData(data);
  } catch(e) {
    return { rank: null, url: "", section: null, error: e.message };
  }
}

/**
 * One-time retry pass for ERROR rows.
 * Called automatically at the end of checkRanksBatched.
 * Each ERROR row gets exactly ONE more attempt:
 *   → success : overwrites ERROR with real rank + URL + fresh timestamp
 *   → fail    : leaves the ERROR cell untouched (user fixes manually)
 * Uses the same parallel fetchAll approach as the main batch.
 */
function retryErrorRows(sheet, finalSheet, finalRowCache, runStartCol, startTime) {
  const lastRow = sheet.getLastRow();
  if (lastRow < START_ROW) return { retried: 0, recovered: 0 };

  // Re-read to get the latest state (some ERRORs may already be fixed by main loop)
  const allData = sheet.getRange(START_ROW, 1, lastRow - START_ROW + 1, I_TIME).getValues();

  const pending = [];
  for (let i = 0; i < allData.length; i++) {
    const rank  = String(allData[i][I_RANK    - 1] || "").trim();
    const q     = String(allData[i][I_KEYWORD - 1] || "").trim();
    const colId = String(allData[i][I_COLLEGE_ID - 1] || "").trim();
    const crsId = String(allData[i][I_COURSE_ID  - 1] || "").trim();
    if (!q || rank.indexOf("ERROR") === -1) continue;
    pending.push({ sheetRow: START_ROW + i, keyword: q, colId: colId, crsId: crsId });
  }

  if (pending.length === 0) return { retried: 0, recovered: 0 };

  Logger.log("retryErrorRows: " + pending.length + " ERROR rows — retrying once each");

  let retried = 0, recovered = 0;

  for (let b = 0; b < pending.length; b += BATCH_SIZE) {
    // Stop if we're dangerously close to the 6-minute Apps Script limit
    if (Date.now() - startTime > MAX_RUNTIME_MS) {
      Logger.log("retryErrorRows: time limit reached, stopping at batch " + b);
      break;
    }

    const batch    = pending.slice(b, b + BATCH_SIZE);
    const requests = batch.map(function(item) { return buildBrightDataOptions(item.keyword); });

    let responses;
    try {
      responses = UrlFetchApp.fetchAll(requests);
    } catch(e) {
      Logger.log("retryErrorRows fetchAll error (batch " + b + "): " + e.message);
      Utilities.sleep(BATCH_SLEEP_MS);
      continue;
    }

    const timestamp = Utilities.formatDate(
      new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"
    );

    for (let j = 0; j < batch.length; j++) {
      const item   = batch[j];
      const result = parseRankFromResponse(responses[j]);
      retried++;

      if (result.error) {
        // Still failing — leave ERROR cell exactly as-is, user will fix manually
        Logger.log("Retry still failed: [" + item.keyword + "] — " + result.error);
        continue;
      }

      // Success — replace ERROR with real rank + URL + fresh timestamp
      const display = writeResult(sheet, item.sheetRow, item.keyword, result, timestamp);

      if (finalSheet && finalRowCache && item.colId && item.crsId) {
        const silo = detectSilo(item.keyword);
        updateFinalCell(finalSheet, finalRowCache, item.colId, item.crsId,
                        silo, display, result.url || "", timestamp, runStartCol);
      }

      recovered++;
      Logger.log("Retry recovered: [" + item.keyword + "] → " + display);
    }

    SpreadsheetApp.flush();
    Utilities.sleep(BATCH_SLEEP_MS);
  }

  return { retried: retried, recovered: recovered };
}


/**
 * Write one result row to Intermediate (rank + url + time + colour)
 * and update the corresponding cell in Final.
 * Used by all three parallel batch loops.
 */
function writeResult(sheet, sheetRow, keyword, result, timestamp, finalSheet, finalRowCache, runStartCol) {
  const display = (result.rank !== null)
    ? (result.section === "organic" ? result.rank : result.rank + " (" + result.section + ")")
    : "Not in Top 50";

  sheet.getRange(sheetRow, I_RANK).setValue(display);
  sheet.getRange(sheetRow, I_URL ).setValue(result.url || "");
  sheet.getRange(sheetRow, I_TIME).setValue(timestamp);

  const cell = sheet.getRange(sheetRow, I_RANK);
  if (result.rank !== null) {
    if      (result.rank <= 3)  cell.setBackground("#c6efce");
    else if (result.rank <= 10) cell.setBackground("#bdd7ee");
    else if (result.rank <= 20) cell.setBackground("#ffeb9c");
    else                        cell.setBackground("#ffc7ce");
  } else {
    cell.setBackground("#f2f2f2");
  }
  return display;
}


// ============================================================
// BRIGHT DATA API CALL  (single — used by test / debug helpers)
// ============================================================

function callBrightData(query) {
  const searchUrl =
    "https://www.google.com/search?q=" + encodeURIComponent(query) +
    "&num=" + TOP_N +
    "&gl=" + COUNTRY +
    "&hl=" + LANGUAGE +
    "&brd_json=1";

  const resp = UrlFetchApp.fetch("https://api.brightdata.com/request", {
    method:      "post",
    contentType: "application/json",
    headers:     { "Authorization": "Bearer " + BRIGHTDATA_API_TOKEN },
    payload:     JSON.stringify({
      zone:   BRIGHTDATA_ZONE,
      url:    searchUrl,
      format: "raw",
    }),
    muteHttpExceptions: true,
  });

  if (resp.getResponseCode() !== 200) {
    throw new Error(
      "Bright Data API " + resp.getResponseCode() + ": " +
      resp.getContentText().substring(0, 200)
    );
  }
  try {
    return JSON.parse(resp.getContentText());
  } catch(e) {
    throw new Error("Response is not JSON: " + resp.getContentText().substring(0, 200));
  }
}


// ============================================================
// SERP RANK FINDER
// ============================================================

/**
 * Single-row SERP search — used by runAndWrite, runAndWriteFast, testFirst5, debugOneRow.
 * Delegates rank-finding to findRankInData (same logic as the parallel path).
 */
function serpSearch(query) {
  const data = callBrightData(query);
  return findRankInData(data);
}


// ============================================================
// DEBUG HELPER
// ============================================================

function findInJson(obj, path, needle, results) {
  if (obj === null || obj === undefined) return;
  if (typeof obj === "string") {
    if (obj.toLowerCase().includes(needle)) {
      results.push(path + " = " + obj.substring(0, 150));
    }
    return;
  }
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => findInJson(v, path + "[" + i + "]", needle, results));
    return;
  }
  if (typeof obj === "object") {
    Object.keys(obj).forEach(k =>
      findInJson(obj[k], path ? path + "." + k : k, needle, results)
    );
  }
}
