/**
 * MasterMigration.gs — 一回限りの会社マスタ移行ゲート
 *
 * 通常利用者向けAPIには公開しない。技術管理者がApps Scriptエディタから実行する。
 * 1) MASTER_STAGING_CSV_FILE_ID にdry-run CSVを指定
 * 2) loadMasterImportStagingFromDrive_() でStagingへロード
 * 3) CSV上で全行を人手確認し、review_status=APPROVEDとして再ロード
 * 4) dryRunCanonicalMasterMigration_() の結果を確認
 * 5) 直近24時間のbackupを確認し、確認文字列を設定してapplyCanonicalMasterMigration_()
 */

var MASTER_MIGRATION_EXPECTED_ROWS_ = 127;
var MASTER_REVIEW_STATUSES_ = ['PENDING', 'APPROVED', 'REJECTED'];
var MASTER_SYSTEM_ONLY_DECISIONS_ = ['KEEP_SYSTEM_ONLY', 'ARCHIVE_EXCLUDE'];

function parseMasterStagingCsv_(csvText) {
  var text = String(csvText || '').replace(/^\uFEFF/, '');
  var values = Utilities.parseCsv(text);
  if (!values || values.length < 2) {
    throw appError_('STAGING_EMPTY', '照合CSVが空です', false);
  }
  var headers = values[0].map(function(value) { return String(value || '').trim(); });
  MASTER_IMPORT_STAGING_HEADERS.forEach(function(header) {
    if (headers.indexOf(header) < 0) {
      throw appError_('STAGING_SCHEMA_INVALID', '照合CSVの列が不足しています: ' + header, false);
    }
  });
  return values.slice(1).filter(function(row) {
    return row.some(function(value) { return String(value || '').trim() !== ''; });
  }).map(function(row) {
    var record = {};
    headers.forEach(function(header, index) {
      if (header) record[header] = row[index] === undefined ? '' : row[index];
    });
    return record;
  });
}

function validateMasterStagingRows_(rows) {
  if (!Array.isArray(rows) || rows.length !== MASTER_MIGRATION_EXPECTED_ROWS_) {
    throw appError_(
      'STAGING_COUNT_MISMATCH',
      '照合CSVは127社である必要があります',
      false
    );
  }
  var sourceHashes = {};
  var vendorNos = {};
  var sourceRows = {};
  rows.forEach(function(row) {
    var sourceRow = Number(row.source_row);
    var vendorNo = String(row.vendor_no || '').trim();
    var companyName = String(row.company_name_raw || '').trim();
    var classification = String(row.classification || '').trim().toUpperCase();
    var reviewStatus = String(row.review_status || '').trim().toUpperCase();
    var sourceHash = String(row.source_sha256 || '').trim().toLowerCase();
    if (!Number.isInteger(sourceRow) || sourceRow < 2 || sourceRows[sourceRow]) {
      throw appError_('STAGING_SOURCE_ROW_INVALID', 'source_rowが不正または重複しています', false);
    }
    if (!companyName || companyName.length > 300) {
      throw appError_('STAGING_COMPANY_INVALID', '会社名が不正です', false);
    }
    if ([
      'MATCHED', 'NEW_FROM_EXCEL', 'SYSTEM_ONLY', 'CONFLICT',
      'DUPLICATE', 'REVIEW_REQUIRED'
    ].indexOf(classification) < 0) {
      throw appError_('STAGING_CLASSIFICATION_INVALID', 'classificationが不正です', false);
    }
    if (MASTER_REVIEW_STATUSES_.indexOf(reviewStatus) < 0) {
      throw appError_('STAGING_REVIEW_INVALID', 'review_statusが不正です', false);
    }
    if (!/^[a-f0-9]{64}$/.test(sourceHash)) {
      throw appError_('STAGING_HASH_INVALID', 'source_sha256が不正です', false);
    }
    if (vendorNo) {
      if (vendorNos[vendorNo]) {
        throw appError_('DUPLICATE_VENDOR_NO', '業者番号が重複しています: ' + vendorNo, false);
      }
      vendorNos[vendorNo] = true;
    }
    sourceRows[sourceRow] = true;
    sourceHashes[sourceHash] = true;
  });
  if (Object.keys(sourceHashes).length !== 1) {
    throw appError_('STAGING_HASH_MISMATCH', '複数の原本hashが混在しています', false);
  }
  return {
    count: rows.length,
    sourceSha256: Object.keys(sourceHashes)[0],
    vendorNoCount: Object.keys(vendorNos).length
  };
}

function loadMasterImportStagingFromDrive_() {
  var fileId = String(getSecureSetting_('MASTER_STAGING_CSV_FILE_ID') || '').trim();
  if (!fileId || !/^[A-Za-z0-9_-]{10,200}$/.test(fileId)) {
    throw appError_('STAGING_FILE_NOT_CONFIGURED', '照合CSVのDrive file IDが未設定です', false);
  }
  var file;
  try {
    file = DriveApp.getFileById(fileId);
  } catch (error) {
    throw appError_('STAGING_FILE_UNAVAILABLE', '照合CSVを読み取れません', true);
  }
  var rows = parseMasterStagingCsv_(file.getBlob().getDataAsString('UTF-8'));
  var validation = validateMasterStagingRows_(rows);
  var confirmation = String(getSecureSetting_('MASTER_IMPORT_LOAD_CONFIRMATION') || '');
  var expected = 'LOAD_STAGING_' + validation.sourceSha256.substring(0, 12);
  if (confirmation !== expected) {
    throw appError_(
      'CONFIRMATION_REQUIRED',
      'Script PropertiesのMASTER_IMPORT_LOAD_CONFIRMATIONに ' + expected + ' が必要です',
      false
    );
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', '移行処理が実行中です', true);
  try {
    var sheet = getSheet_(SHEETS.MasterImportStaging);
    ensureHeaders_(sheet, MASTER_IMPORT_STAGING_HEADERS);
    var audit = appendAuditEvent_({
      user_email: 'TECHNICAL_ADMIN',
      action: 'LOAD_MASTER_STAGING',
      target_type: 'MasterImportStaging',
      target_id: validation.sourceSha256,
      details: JSON.stringify(validation),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    if (sheet.getLastRow() > 1) {
      sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).clearContent();
    }
    var values = rows.map(function(row) {
      return MASTER_IMPORT_STAGING_HEADERS.map(function(header) {
        return sanitizeForSheet_(row[header] === undefined ? '' : row[header]);
      });
    });
    sheet.getRange(2, 1, values.length, MASTER_IMPORT_STAGING_HEADERS.length).setValues(values);
    SpreadsheetApp.flush();
    appendAuditEvent_({
      log_id: audit.log_id,
      user_email: 'TECHNICAL_ADMIN',
      action: 'LOAD_MASTER_STAGING',
      target_type: 'MasterImportStaging',
      target_id: validation.sourceSha256,
      details: JSON.stringify(validation),
      status: 'COMMITTED'
    });
    return Object.assign({ auditId: audit.log_id }, validation);
  } catch (error) {
    try {
      appendAuditEvent_({
        user_email: 'TECHNICAL_ADMIN',
        action: 'LOAD_MASTER_STAGING',
        target_type: 'MasterImportStaging',
        target_id: validation && validation.sourceSha256,
        details: String(error.message || error).substring(0, 500),
        status: 'ABORTED',
        error_code: String(error.code || 'STAGING_LOAD_FAILED')
      });
    } catch (ignoredAuditFailure) {
      console.error('[STAGING_LOAD_AUDIT_FAILED]');
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function normalizeMigrationCompanyName_(value) {
  var text = String(value || '').trim().toLowerCase();
  text = text.replace(/[\s\u3000]/g, '');
  text = text.replace(/㈱/g, '株式会社').replace(/㈲/g, '有限会社');
  text = text.replace(/[（(]株[）)]/g, '株式会社');
  text = text.replace(/[（(]有[）)]/g, '有限会社');
  return text;
}

function buildSystemCompanySeedMap_() {
  var map = {};
  function add(record, source) {
    var id = String(record.company_id || '').trim();
    if (!id) return;
    if (!map[id]) {
      map[id] = {
        company_id: id,
        company_name_raw: '',
        company_name_normalized: '',
        representative_name: '',
        contact_person: '',
        contact_email: '',
        contact_email_cc: '',
        phone: '',
        status: 'ACTIVE',
        created_at: '',
        updated_at: '',
        vendor_no: '',
        internal_owner_email: '',
        contact_verified_at: '',
        contact_verified_by: '',
        notification_mode: 'MANUAL',
        data_version: 1,
        created_by: 'MASTER_MIGRATION',
        updated_by: 'MASTER_MIGRATION',
        _sources: {}
      };
    }
    map[id]._sources[source] = true;
    var rawName = String(
      record.company_name_raw ||
      record.company_name ||
      record.company_name_normalized ||
      ''
    ).trim();
    if (!map[id].company_name_raw && rawName) {
      map[id].company_name_raw = rawName;
      map[id].company_name_normalized = normalizeMigrationCompanyName_(rawName);
    }
  }
  readRecords_(SHEETS.Companies).forEach(function(record) {
    add(record, 'Companies');
    var id = String(record.company_id || '').trim();
    if (!id) return;
    SECURE_COMPANIES_HEADERS_.forEach(function(header) {
      if (header === 'company_id') return;
      if (record[header] !== undefined && record[header] !== '') {
        map[id][header] = record[header];
      }
    });
  });
  readRecords_(SHEETS.Permits).forEach(function(record) { add(record, 'Permits'); });
  readRecords_(SHEETS.MLITPermits).forEach(function(record) { add(record, 'MLITPermits'); });
  return map;
}

function nextMigrationCompanyId_(usedIds, cursor) {
  var next = cursor;
  var id = '';
  do {
    next++;
    id = 'C' + ('0000' + next).slice(-4);
  } while (usedIds[id]);
  usedIds[id] = true;
  return { id: id, cursor: next };
}

function parseSystemOnlyDecisions_() {
  var raw = String(getSecureSetting_('MASTER_SYSTEM_ONLY_DECISIONS') || '').trim();
  if (!raw) return {};
  var parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw appError_(
      'SYSTEM_ONLY_DECISIONS_INVALID',
      'MASTER_SYSTEM_ONLY_DECISIONSはJSON objectで指定してください',
      false
    );
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw appError_(
      'SYSTEM_ONLY_DECISIONS_INVALID',
      'MASTER_SYSTEM_ONLY_DECISIONSはJSON objectで指定してください',
      false
    );
  }
  var decisions = {};
  Object.keys(parsed).forEach(function(companyId) {
    var id = String(companyId || '').trim();
    var decision = String(parsed[companyId] || '').trim().toUpperCase();
    if (!/^C\d{4,8}$/.test(id) ||
        MASTER_SYSTEM_ONLY_DECISIONS_.indexOf(decision) < 0) {
      throw appError_(
        'SYSTEM_ONLY_DECISIONS_INVALID',
        'SYSTEM_ONLY判断が不正です: ' + id,
        false
      );
    }
    decisions[id] = decision;
  });
  return decisions;
}

function assertSystemOnlyDecisions_(systemOnlyIds, decisions) {
  var expected = (systemOnlyIds || []).slice().sort();
  var actual = Object.keys(decisions || {}).sort();
  if (JSON.stringify(expected) !== JSON.stringify(actual)) {
    throw appError_(
      'SYSTEM_ONLY_DECISION_REQUIRED',
      'SYSTEM_ONLY会社ごとの維持・除外判断が一致しません',
      false
    );
  }
}

function applySystemOnlyDecisions_(canonicalMap, systemOnlyIds, decisions) {
  var summary = {
    KEEP_SYSTEM_ONLY: 0,
    ARCHIVE_EXCLUDE: 0,
    PENDING: 0
  };
  (systemOnlyIds || []).forEach(function(companyId) {
    var decision = decisions[companyId];
    if (!decision) {
      summary.PENDING++;
      return;
    }
    if (decision === 'KEEP_SYSTEM_ONLY') {
      canonicalMap[companyId].status = 'ACTIVE';
      summary.KEEP_SYSTEM_ONLY++;
    } else if (decision === 'ARCHIVE_EXCLUDE') {
      // company_idとPermit/MLIT参照は保全し、通常画面・送信対象から外す。
      canonicalMap[companyId].status = 'INACTIVE';
      canonicalMap[companyId].contact_verified_at = '';
      canonicalMap[companyId].contact_verified_by = '';
      summary.ARCHIVE_EXCLUDE++;
    }
    canonicalMap[companyId].updated_at = getNowString_();
    canonicalMap[companyId].updated_by = 'MASTER_MIGRATION';
  });
  return summary;
}

function prepareCanonicalMasterMigration_() {
  var staging = readRecords_(SHEETS.MasterImportStaging);
  var validation = validateMasterStagingRows_(staging);
  var notApproved = staging.filter(function(row) {
    return String(row.review_status || '').trim().toUpperCase() !== 'APPROVED';
  });
  if (notApproved.length > 0) {
    throw appError_(
      'MIGRATION_REVIEW_INCOMPLETE',
      '未承認の照合行が' + notApproved.length + '件あります',
      false
    );
  }
  var forbidden = staging.filter(function(row) {
    return ['CONFLICT', 'DUPLICATE'].indexOf(
      String(row.classification || '').trim().toUpperCase()
    ) >= 0;
  });
  if (forbidden.length > 0) {
    throw appError_('MIGRATION_CONFLICT', '競合・重複が残っています', false);
  }

  var canonicalMap = buildSystemCompanySeedMap_();
  var initialSystemIds = Object.keys(canonicalMap);
  var matchedSystemIds = {};
  var usedIds = {};
  var maxNumeric = 0;
  Object.keys(canonicalMap).forEach(function(id) {
    usedIds[id] = true;
    var match = /^C(\d{1,8})$/.exec(id);
    if (match) maxNumeric = Math.max(maxNumeric, Number(match[1]));
  });
  var assignedNew = 0;
  staging.forEach(function(row) {
    var matchedId = String(row.matched_company_id || '').trim();
    var classification = String(row.classification || '').trim().toUpperCase();
    if (matchedId && !canonicalMap[matchedId]) {
      throw appError_(
        'MATCHED_COMPANY_NOT_FOUND',
        '照合先company_idが存在しません: ' + matchedId,
        false
      );
    }
    if (matchedId) matchedSystemIds[matchedId] = true;
    if (!matchedId) {
      if (classification !== 'NEW_FROM_EXCEL') {
        throw appError_(
          'MATCH_DECISION_REQUIRED',
          '既存候補のmatched_company_idが未確定です: source_row=' + row.source_row,
          false
        );
      }
      var allocated = nextMigrationCompanyId_(usedIds, maxNumeric);
      matchedId = allocated.id;
      maxNumeric = allocated.cursor;
      canonicalMap[matchedId] = {
        company_id: matchedId,
        status: 'ACTIVE',
        notification_mode: 'MANUAL',
        data_version: 1,
        created_at: getNowString_(),
        created_by: 'MASTER_MIGRATION',
        updated_at: getNowString_(),
        updated_by: 'MASTER_MIGRATION',
        _sources: { Excel: true }
      };
      assignedNew++;
    }
    var company = canonicalMap[matchedId];
    company.vendor_no = String(row.vendor_no || '').trim();
    company.company_name_raw = String(row.company_name_raw || '').trim();
    company.company_name_normalized = String(
      row.company_name_normalized ||
      normalizeMigrationCompanyName_(row.company_name_raw)
    ).trim();
    company.updated_at = getNowString_();
    company.updated_by = 'MASTER_MIGRATION';
  });

  var systemOnlyIds = initialSystemIds.filter(function(id) {
    return !matchedSystemIds[id];
  }).sort();
  var systemOnlyDecisions = parseSystemOnlyDecisions_();
  var systemOnlyDecisionSummary = applySystemOnlyDecisions_(
    canonicalMap,
    systemOnlyIds,
    systemOnlyDecisions
  );
  var pendingSystemOnlyDecisionIds = systemOnlyIds.filter(function(id) {
    return !systemOnlyDecisions[id];
  });

  var canonical = Object.keys(canonicalMap).sort().map(function(id) {
    var company = canonicalMap[id];
    var clean = {};
    SECURE_COMPANIES_HEADERS_.forEach(function(header) {
      clean[header] = company[header] === undefined ? '' : company[header];
    });
    clean.data_version = getCompanyVersion_(clean);
    return clean;
  });
  var vendorNos = {};
  canonical.forEach(function(company) {
    var vendorNo = String(company.vendor_no || '').trim();
    if (vendorNo && vendorNos[vendorNo]) {
      throw appError_('DUPLICATE_VENDOR_NO', '正本候補の業者番号が重複しています: ' + vendorNo, false);
    }
    if (vendorNo) vendorNos[vendorNo] = true;
  });
  var orphanIds = {};
  readRecords_(SHEETS.Permits).concat(readRecords_(SHEETS.MLITPermits)).forEach(function(record) {
    var id = String(record.company_id || '').trim();
    if (id && !canonicalMap[id]) orphanIds[id] = true;
  });
  if (Object.keys(orphanIds).length > 0) {
    throw appError_('ORPHAN_COMPANY_ID', 'permit参照の孤立company_idがあります', false);
  }
  var activeCount = canonical.filter(function(company) {
    return String(company.status || 'ACTIVE').toUpperCase() === 'ACTIVE';
  }).length;
  var inactiveCount = canonical.length - activeCount;
  return {
    sourceSha256: validation.sourceSha256,
    stagingCount: staging.length,
    canonicalCount: canonical.length,
    assignedNewCount: assignedNew,
    vendorNoCount: Object.keys(vendorNos).length,
    orphanCompanyIdCount: 0,
    systemOnlyIds: systemOnlyIds,
    systemOnlyDecisions: systemOnlyDecisions,
    systemOnlyDecisionSummary: systemOnlyDecisionSummary,
    pendingSystemOnlyDecisionIds: pendingSystemOnlyDecisionIds,
    activeCount: activeCount,
    inactiveCount: inactiveCount,
    canonical: canonical
  };
}

function dryRunCanonicalMasterMigration_() {
  var prepared = prepareCanonicalMasterMigration_();
  return {
    sourceSha256: prepared.sourceSha256,
    stagingCount: prepared.stagingCount,
    canonicalCount: prepared.canonicalCount,
    assignedNewCount: prepared.assignedNewCount,
    vendorNoCount: prepared.vendorNoCount,
    orphanCompanyIdCount: prepared.orphanCompanyIdCount,
    systemOnlyIds: prepared.systemOnlyIds,
    systemOnlyDecisions: prepared.systemOnlyDecisions,
    systemOnlyDecisionSummary: prepared.systemOnlyDecisionSummary,
    pendingSystemOnlyDecisionIds: prepared.pendingSystemOnlyDecisionIds,
    activeCount: prepared.activeCount,
    inactiveCount: prepared.inactiveCount,
    systemOnlyDecisionProperty: 'MASTER_SYSTEM_ONLY_DECISIONS',
    confirmationRequired: 'APPLY_CANONICAL_' + prepared.sourceSha256.substring(0, 12)
  };
}

function assertRecentBackupForMigration_() {
  if (getSecureSetting_('LAST_BACKUP_STATUS') !== 'COMMITTED') {
    throw appError_('RECENT_BACKUP_REQUIRED', '移行前バックアップが未完了です', false);
  }
  var lastAt = new Date(getSecureSetting_('LAST_BACKUP_AT'));
  if (isNaN(lastAt.getTime()) || Date.now() - lastAt.getTime() > 24 * 60 * 60 * 1000) {
    throw appError_('RECENT_BACKUP_REQUIRED', '24時間以内のバックアップが必要です', false);
  }
}

function applyCanonicalMasterMigration_() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw appError_('LOCK_TIMEOUT', '移行処理が実行中です', true);
  var prepared = null;
  var spreadsheet = null;
  var archiveName = '';
  var candidateName = '';
  try {
    prepared = prepareCanonicalMasterMigration_();
    assertRecentBackupForMigration_();
    assertSystemOnlyDecisions_(
      prepared.systemOnlyIds,
      prepared.systemOnlyDecisions
    );
    var expectedConfirmation = 'APPLY_CANONICAL_' + prepared.sourceSha256.substring(0, 12);
    if (String(getSecureSetting_('MASTER_IMPORT_APPLY_CONFIRMATION') || '') !== expectedConfirmation) {
      throw appError_(
        'CONFIRMATION_REQUIRED',
        'Script PropertiesのMASTER_IMPORT_APPLY_CONFIRMATIONに ' +
          expectedConfirmation + ' が必要です',
        false
      );
    }

    spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var current = spreadsheet.getSheetByName(SHEETS.Companies);
    if (!current) throw appError_('COMPANIES_NOT_FOUND', 'Companiesが存在しません', false);
    var timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyyMMdd_HHmmss');
    candidateName = 'Companies_Migration_' + timestamp;
    archiveName = 'Companies_Archive_' + timestamp;
    if (spreadsheet.getSheetByName(candidateName) || spreadsheet.getSheetByName(archiveName)) {
      throw appError_('MIGRATION_NAME_CONFLICT', '移行用シート名が重複しました', true);
    }
    var audit = appendAuditEvent_({
      user_email: 'TECHNICAL_ADMIN',
      action: 'APPLY_CANONICAL_MASTER',
      target_type: 'Companies',
      target_id: prepared.sourceSha256,
      before_json: JSON.stringify({ rowCount: Math.max(current.getLastRow() - 1, 0) }),
      after_json: JSON.stringify({
        rowCount: prepared.canonicalCount,
        assignedNewCount: prepared.assignedNewCount
      }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();

    var candidate = spreadsheet.insertSheet(candidateName);
    candidate.getRange(1, 1, 1, SECURE_COMPANIES_HEADERS_.length)
      .setValues([SECURE_COMPANIES_HEADERS_])
      .setFontWeight('bold')
      .setBackground('#e8f0fe');
    candidate.setFrozenRows(1);
    var values = prepared.canonical.map(function(company) {
      return SECURE_COMPANIES_HEADERS_.map(function(header) {
        return sanitizeForSheet_(company[header]);
      });
    });
    candidate.getRange(2, 1, values.length, SECURE_COMPANIES_HEADERS_.length).setValues(values);
    SpreadsheetApp.flush();

    current.setName(archiveName);
    candidate.setName(SHEETS.Companies);
    protectSheetForOwner_(SHEETS.Companies);
    protectSheetForOwner_(archiveName);
    setSecureSetting_('MASTER_MIGRATION_APPLIED_SHA256', prepared.sourceSha256);
    setSecureSetting_('MASTER_MIGRATION_APPLIED_AT', getNowString_());
    appendAuditEvent_({
      log_id: audit.log_id,
      user_email: 'TECHNICAL_ADMIN',
      action: 'APPLY_CANONICAL_MASTER',
      target_type: 'Companies',
      target_id: prepared.sourceSha256,
      details: JSON.stringify({
        archiveSheet: archiveName,
        canonicalCount: prepared.canonicalCount,
        assignedNewCount: prepared.assignedNewCount,
        vendorNoCount: prepared.vendorNoCount,
        systemOnlyDecisionSummary: prepared.systemOnlyDecisionSummary
      }),
      status: 'COMMITTED'
    });
    return {
      auditId: audit.log_id,
      archiveSheet: archiveName,
      sourceSha256: prepared.sourceSha256,
      canonicalCount: prepared.canonicalCount,
      assignedNewCount: prepared.assignedNewCount
    };
  } catch (error) {
    // Companiesへの改名途中で失敗した場合だけ、旧シート名を即時復旧する。
    try {
      if (spreadsheet &&
          !spreadsheet.getSheetByName(SHEETS.Companies) &&
          archiveName &&
          spreadsheet.getSheetByName(archiveName)) {
        spreadsheet.getSheetByName(archiveName).setName(SHEETS.Companies);
      }
    } catch (rollbackError) {
      console.error('[MIGRATION_NAME_ROLLBACK_FAILED]');
    }
    try {
      appendAuditEvent_({
        user_email: 'TECHNICAL_ADMIN',
        action: 'APPLY_CANONICAL_MASTER',
        target_type: 'Companies',
        target_id: prepared && prepared.sourceSha256,
        details: String(error.message || error).substring(0, 500),
        status: 'ABORTED',
        error_code: String(error.code || 'MIGRATION_FAILED')
      });
    } catch (ignoredAuditFailure) {
      console.error('[MIGRATION_AUDIT_FAILED]');
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}
