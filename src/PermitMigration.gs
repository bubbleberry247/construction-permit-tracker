/**
 * PermitMigration.gs — 旧MLIT観測から許可正本・監視対象を承認移行する。
 *
 * 通常APIへ公開しない。Drive上のreview済みCSVをstagingへロードし、
 * dry-run、直近backup、hash付き確認文字列を経て適用する。
 */

var PERMIT_MIGRATION_REVIEW_STATUSES_ = ['PENDING', 'APPROVED', 'REJECTED'];
var MONITORING_MIGRATION_REVIEW_STATUSES_ = [
  'PENDING', 'APPROVED', 'REJECTED', 'AUTO_APPROVED'
];

function parseMigrationCsv_(csvText, requiredHeaders) {
  var text = String(csvText || '').replace(/^\uFEFF/, '');
  var values = Utilities.parseCsv(text);
  if (!values || values.length < 2) {
    throw appError_('STAGING_EMPTY', '移行CSVが空です', false);
  }
  var headers = values[0].map(function(value) {
    return String(value || '').trim();
  });
  requiredHeaders.forEach(function(header) {
    if (headers.indexOf(header) < 0) {
      throw appError_(
        'STAGING_SCHEMA_INVALID',
        '移行CSVの列が不足しています: ' + header,
        false
      );
    }
  });
  return {
    text: text,
    rows: values.slice(1).filter(function(row) {
      return row.some(function(value) {
        return String(value || '').trim() !== '';
      });
    }).map(function(row) {
      var record = {};
      headers.forEach(function(header, index) {
        if (header) record[header] = row[index] === undefined ? '' : row[index];
      });
      return record;
    })
  };
}

function loadMigrationStagingFromDrive_(options) {
  var fileId = String(getSecureSetting_(options.fileProperty) || '').trim();
  if (!/^[A-Za-z0-9_-]{10,200}$/.test(fileId)) {
    throw appError_(
      'STAGING_FILE_NOT_CONFIGURED',
      options.fileProperty + 'が未設定です',
      false
    );
  }
  var file;
  try {
    file = DriveApp.getFileById(fileId);
  } catch (error) {
    throw appError_('STAGING_FILE_UNAVAILABLE', '移行CSVを読み取れません', true);
  }
  var parsed = parseMigrationCsv_(
    file.getBlob().getDataAsString('UTF-8'),
    options.headers
  );
  options.validate(parsed.rows, false);
  var csvHash = sha256Hex_(parsed.text);
  var expected = options.loadPrefix + csvHash.substring(0, 12);
  if (String(getSecureSetting_(options.loadConfirmationProperty) || '') !== expected) {
    throw appError_(
      'CONFIRMATION_REQUIRED',
      options.loadConfirmationProperty + 'に' + expected + 'が必要です',
      false
    );
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', '移行stagingのロード処理が実行中です', true);
  }
  var audit = null;
  var sheet = null;
  var previousValues = null;
  try {
    sheet = getSheet_(options.sheetName);
    ensureHeaders_(sheet, options.headers);
    previousValues = [];
    audit = appendAuditEvent_({
      user_email: 'TECHNICAL_ADMIN',
      action: options.loadAction,
      target_type: options.sheetName,
      target_id: csvHash,
      details: JSON.stringify({ rows: parsed.rows.length, fileId: fileId }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    if (sheet.getLastRow() > 1) {
      previousValues = sheet.getRange(
        2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
      ).getValues();
      sheet.getRange(
        2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
      ).clearContent();
    }
    var values = parsed.rows.map(function(row) {
      return options.headers.map(function(header) {
        return sanitizeForSheet_(row[header] === undefined ? '' : row[header]);
      });
    });
    sheet.getRange(2, 1, values.length, options.headers.length).setValues(values);
    SpreadsheetApp.flush();
    setSecureSetting_(options.loadedHashProperty, csvHash);
    if (!updateAuditEvent_(audit.log_id, {
      status: 'COMMITTED',
      details: JSON.stringify({
        rows: parsed.rows.length,
        fileId: fileId,
        csvSha256: csvHash
      })
    })) {
      throw appError_(
        'AUDIT_COMMIT_FAILED',
        'stagingロードの監査を確定できませんでした',
        true,
        audit.log_id
      );
    }
    return {
      rows: parsed.rows.length,
      csvSha256: csvHash,
      auditId: audit.log_id
    };
  } catch (error) {
    try {
      if (sheet && previousValues) {
        if (sheet.getLastRow() > 1) {
          sheet.getRange(
            2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
          ).clearContent();
        }
        if (previousValues.length > 0) {
          sheet.getRange(
            2, 1, previousValues.length, previousValues[0].length
          ).setValues(previousValues);
        }
        SpreadsheetApp.flush();
      }
    } catch (rollbackError) {
      console.error('[STAGING_LOAD_ROLLBACK_FAILED]');
    }
    if (audit) {
      try {
        updateAuditEvent_(audit.log_id, {
          status: 'ABORTED',
          error_code: String(error.code || 'STAGING_LOAD_FAILED'),
          details: String(error.message || error).substring(0, 500)
        });
        SpreadsheetApp.flush();
      } catch (auditError) {
        console.error('[STAGING_LOAD_ABORT_AUDIT_FAILED]');
      }
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function permitStagingStableKey_(row) {
  return [
    String(row.company_id || '').trim(),
    String(
      row.permit_authority_name_normalized ||
      row.permit_authority_name ||
      ''
    ).replace(/[\s\u3000]/g, ''),
    normalizePermitNumberForMlit_(row.contractor_number),
    String(row.permit_category || '').trim()
  ].join('|');
}

function validatePermitImportStaging_(rows, requireDecisions) {
  if (!Array.isArray(rows) || rows.length < 1 || rows.length > 500) {
    throw appError_('PERMIT_STAGING_COUNT_INVALID', '許可移行行数が不正です', false);
  }
  var rowIds = {};
  var approvedKeys = {};
  var counts = { PENDING: 0, APPROVED: 0, REJECTED: 0 };
  rows.forEach(function(row) {
    var rowId = String(row.migration_row_id || '').trim();
    var sourceHash = String(row.source_sha256 || '').trim().toLowerCase();
    var reviewStatus = String(row.review_status || '').trim().toUpperCase();
    if (!/^PM-\d{4}-\d{2}$/.test(rowId) || rowIds[rowId]) {
      throw appError_('PERMIT_STAGING_ROW_INVALID', 'migration_row_idが不正です', false);
    }
    if (!/^[a-f0-9]{64}$/.test(sourceHash)) {
      throw appError_('PERMIT_STAGING_HASH_INVALID', 'source_sha256が不正です', false);
    }
    if (PERMIT_MIGRATION_REVIEW_STATUSES_.indexOf(reviewStatus) < 0) {
      throw appError_('PERMIT_STAGING_REVIEW_INVALID', 'review_statusが不正です', false);
    }
    rowIds[rowId] = true;
    counts[reviewStatus]++;
    if (reviewStatus !== 'APPROVED') return;

    var company = findByKey_(SHEETS.Companies, 'company_id', row.company_id);
    if (!company ||
        String(company.status || '').trim().toUpperCase() !== 'ACTIVE') {
      throw appError_(
        'PERMIT_STAGING_COMPANY_INVALID',
        'ACTIVE会社に紐づかない許可候補があります: ' + rowId,
        false
      );
    }
    var authority = String(
      row.permit_authority_name_normalized ||
      row.permit_authority_name ||
      ''
    ).trim();
    var contractorNumber = normalizePermitNumberForMlit_(row.contractor_number);
    var category = String(row.permit_category || '').trim();
    var expiry = normalizeExpiryDateKey_(row.expiry_date);
    if (!authority || !contractorNumber || !category || !expiry) {
      throw appError_(
        'PERMIT_STAGING_FIELDS_REQUIRED',
        '承認行の行政庁・許可番号・区分・期限が不足しています: ' + rowId,
        false
      );
    }
    var stableKey = permitStagingStableKey_(row);
    if (approvedKeys[stableKey]) {
      throw appError_(
        'PERMIT_STAGING_DUPLICATE',
        '承認候補の許可キーが重複しています: ' + stableKey,
        false
      );
    }
    approvedKeys[stableKey] = true;
    if (PermitsModel.findByUpsertKey(
      row.company_id, authority, contractorNumber, category
    )) {
      throw appError_(
        'PERMIT_ALREADY_EXISTS',
        '承認候補と同じ許可が既にPermitsにあります: ' + stableKey,
        false
      );
    }
  });
  if (requireDecisions && counts.PENDING > 0) {
    throw appError_(
      'PERMIT_REVIEW_INCOMPLETE',
      '未判断の許可移行候補が' + counts.PENDING + '件あります',
      false
    );
  }
  return {
    total: rows.length,
    pending: counts.PENDING,
    approved: counts.APPROVED,
    rejected: counts.REJECTED
  };
}

function loadPermitImportStagingFromDrive_() {
  return loadMigrationStagingFromDrive_({
    fileProperty: 'PERMIT_STAGING_CSV_FILE_ID',
    loadConfirmationProperty: 'PERMIT_STAGING_LOAD_CONFIRMATION',
    loadedHashProperty: 'PERMIT_STAGING_LOADED_SHA256',
    loadPrefix: 'LOAD_PERMIT_STAGING_',
    sheetName: SHEETS.PermitImportStaging,
    headers: PERMIT_IMPORT_STAGING_HEADERS_,
    validate: validatePermitImportStaging_,
    loadAction: 'LOAD_PERMIT_STAGING'
  });
}

function buildPermitMigrationCandidate_() {
  var staging = readRecords_(SHEETS.PermitImportStaging);
  var validation = validatePermitImportStaging_(staging, true);
  var canonical = readRecords_(SHEETS.Permits).map(function(row) {
    var clean = {};
    PERMITS_HEADERS.forEach(function(header) {
      clean[header] = row[header] === undefined ? '' : row[header];
    });
    clean.permit_data_version = getPermitDataVersion_(clean);
    return clean;
  });
  var createdAt = getNowString_();
  staging.filter(function(row) {
    return String(row.review_status || '').trim().toUpperCase() === 'APPROVED';
  }).forEach(function(row) {
    var permit = {};
    PERMITS_HEADERS.forEach(function(header) {
      permit[header] = row[header] === undefined ? '' : row[header];
    });
    permit.permit_id = String(row.permit_id || '').trim() || generateUuid_();
    permit.permit_authority_name_normalized = String(
      row.permit_authority_name_normalized ||
      row.permit_authority_name ||
      ''
    ).trim();
    permit.contractor_number = normalizePermitNumberForMlit_(
      row.contractor_number
    );
    permit.expiry_date = normalizeExpiryDateKey_(row.expiry_date);
    permit.current_status = String(row.current_status || 'VALID').trim();
    permit.parse_status = 'OK_MIGRATED_FROM_REVIEWED_MLIT';
    permit.permit_file_version = Number(row.permit_file_version || 1);
    permit.permit_data_version = 1;
    permit.created_at = createdAt;
    permit.updated_at = createdAt;
    canonical.push(permit);
  });
  var ids = {};
  canonical.forEach(function(permit) {
    var id = String(permit.permit_id || '').trim();
    if (!id || ids[id]) {
      throw appError_('PERMIT_ID_DUPLICATE', 'permit_idが空または重複しています', false);
    }
    ids[id] = true;
  });
  return {
    validation: validation,
    existingCount: canonical.length - validation.approved,
    canonical: canonical,
    canonicalCount: canonical.length
  };
}

function dryRunPermitMasterMigration_() {
  var prepared = buildPermitMigrationCandidate_();
  var csvHash = String(
    getSecureSetting_('PERMIT_STAGING_LOADED_SHA256') || ''
  ).trim();
  return {
    total: prepared.validation.total,
    approved: prepared.validation.approved,
    rejected: prepared.validation.rejected,
    pending: prepared.validation.pending,
    existingCount: prepared.existingCount,
    canonicalCount: prepared.canonicalCount,
    csvSha256: csvHash,
    confirmationRequired: 'APPLY_PERMIT_STAGING_' + csvHash.substring(0, 12)
  };
}

function applyPermitMasterMigration_() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', '許可正本移行が実行中です', true);
  }
  var spreadsheet = null;
  var audit = null;
  var candidateName = '';
  var archiveName = '';
  var swapped = false;
  try {
    assertRecentBackupForMigration_();
    var prepared = buildPermitMigrationCandidate_();
    var csvHash = String(
      getSecureSetting_('PERMIT_STAGING_LOADED_SHA256') || ''
    ).trim();
    var expected = 'APPLY_PERMIT_STAGING_' + csvHash.substring(0, 12);
    if (!csvHash ||
        String(getSecureSetting_('PERMIT_STAGING_APPLY_CONFIRMATION') || '') !== expected) {
      throw appError_(
        'CONFIRMATION_REQUIRED',
        'PERMIT_STAGING_APPLY_CONFIRMATIONに' + expected + 'が必要です',
        false
      );
    }
    if (getSecureSetting_('PERMIT_MIGRATION_APPLIED_SHA256') === csvHash) {
      throw appError_('MIGRATION_ALREADY_APPLIED', '同じ許可移行は適用済みです', false);
    }

    spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var current = spreadsheet.getSheetByName(SHEETS.Permits);
    if (!current) throw appError_('PERMITS_NOT_FOUND', 'Permitsが存在しません', false);
    var timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyyMMdd_HHmmss');
    candidateName = 'Permits_Migration_' + timestamp;
    archiveName = 'Permits_Archive_' + timestamp;
    if (spreadsheet.getSheetByName(candidateName) ||
        spreadsheet.getSheetByName(archiveName)) {
      throw appError_('MIGRATION_NAME_CONFLICT', '許可移行シート名が重複しました', true);
    }
    audit = appendAuditEvent_({
      user_email: 'TECHNICAL_ADMIN',
      action: 'APPLY_PERMIT_MASTER',
      target_type: 'Permits',
      target_id: csvHash,
      before_json: JSON.stringify({ rows: prepared.existingCount }),
      after_json: JSON.stringify({ rows: prepared.canonicalCount }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();

    var candidate = spreadsheet.insertSheet(candidateName);
    candidate.getRange(1, 1, 1, PERMITS_HEADERS.length)
      .setValues([PERMITS_HEADERS])
      .setFontWeight('bold')
      .setBackground('#e8f0fe');
    candidate.setFrozenRows(1);
    var values = prepared.canonical.map(function(permit) {
      return PERMITS_HEADERS.map(function(header) {
        return sanitizeForSheet_(permit[header]);
      });
    });
    candidate.getRange(2, 1, values.length, PERMITS_HEADERS.length).setValues(values);
    SpreadsheetApp.flush();
    current.setName(archiveName);
    candidate.setName(SHEETS.Permits);
    swapped = true;
    protectSheetForOwner_(SHEETS.Permits);
    protectSheetForOwner_(archiveName);
    setSecureSetting_('PERMIT_MIGRATION_APPLIED_SHA256', csvHash);
    setSecureSetting_('PERMIT_MIGRATION_APPLIED_AT', getNowString_());
    if (!updateAuditEvent_(audit.log_id, {
      status: 'COMMITTED',
      details: JSON.stringify({
        archiveSheet: archiveName,
        existing: prepared.existingCount,
        added: prepared.validation.approved,
        rejected: prepared.validation.rejected,
        canonical: prepared.canonicalCount
      })
    })) {
      throw appError_(
        'AUDIT_COMMIT_FAILED',
        '許可正本移行の監査を確定できませんでした',
        true,
        audit.log_id
      );
    }
    return {
      auditId: audit.log_id,
      archiveSheet: archiveName,
      addedCount: prepared.validation.approved,
      canonicalCount: prepared.canonicalCount
    };
  } catch (error) {
    try {
      if (spreadsheet && archiveName &&
          spreadsheet.getSheetByName(archiveName)) {
        var activePermits = spreadsheet.getSheetByName(SHEETS.Permits);
        if (swapped && activePermits) {
          var failedName = candidateName + '_FAILED';
          if (spreadsheet.getSheetByName(failedName)) {
            failedName += '_' + generateUuid_().substring(0, 8);
          }
          activePermits.setName(failedName);
        }
        spreadsheet.getSheetByName(archiveName).setName(SHEETS.Permits);
        setSecureSetting_('PERMIT_MIGRATION_APPLIED_SHA256', '');
        setSecureSetting_('PERMIT_MIGRATION_APPLIED_AT', '');
        SpreadsheetApp.flush();
      }
    } catch (ignoredRollbackError) {
      console.error('[PERMIT_MIGRATION_NAME_ROLLBACK_FAILED]');
    }
    if (audit) {
      try {
        updateAuditEvent_(audit.log_id, {
          status: 'ABORTED',
          error_code: String(error.code || 'PERMIT_MIGRATION_FAILED'),
          details: String(error.message || error).substring(0, 500)
        });
        SpreadsheetApp.flush();
      } catch (auditError) {
        console.error('[PERMIT_MIGRATION_ABORT_AUDIT_FAILED]');
      }
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function validateMonitoringTargetStaging_(rows, requireDecisions) {
  var companies = readRecords_(SHEETS.Companies);
  if (!Array.isArray(rows) || rows.length !== companies.length) {
    throw appError_(
      'MONITORING_STAGING_COUNT_INVALID',
      '監視対象CSVはCompanies全社を含む必要があります',
      false
    );
  }
  var companyMap = {};
  companies.forEach(function(company) {
    companyMap[String(company.company_id || '').trim()] = company;
  });
  var seen = {};
  var counts = { PENDING: 0, APPROVED: 0, REJECTED: 0, AUTO_APPROVED: 0 };
  rows.forEach(function(row) {
    var companyId = String(row.company_id || '').trim();
    var reviewStatus = String(row.review_status || '').trim().toUpperCase();
    var action = String(row.proposed_action || '').trim().toUpperCase();
    if (!companyMap[companyId] || seen[companyId]) {
      throw appError_(
        'MONITORING_STAGING_COMPANY_INVALID',
        '監視対象のcompany_idが不正または重複しています',
        false
      );
    }
    if (MONITORING_MIGRATION_REVIEW_STATUSES_.indexOf(reviewStatus) < 0 ||
        ['MONITOR', 'DO_NOT_MONITOR'].indexOf(action) < 0) {
      throw appError_('MONITORING_STAGING_REVIEW_INVALID', '監視対象判断が不正です', false);
    }
    if (action === 'MONITOR' &&
        String(companyMap[companyId].status || '').toUpperCase() !== 'ACTIVE') {
      throw appError_(
        'MONITORING_INACTIVE_COMPANY',
        'INACTIVE会社を監視対象にはできません: ' + companyId,
        false
      );
    }
    seen[companyId] = true;
    counts[reviewStatus]++;
  });
  if (requireDecisions && counts.PENDING > 0) {
    throw appError_(
      'MONITORING_REVIEW_INCOMPLETE',
      '未判断の監視対象が' + counts.PENDING + '社あります',
      false
    );
  }
  return {
    total: rows.length,
    pending: counts.PENDING,
    approved: counts.APPROVED,
    rejected: counts.REJECTED,
    autoApproved: counts.AUTO_APPROVED,
    monitor: rows.filter(function(row) {
      return String(row.proposed_action || '').toUpperCase() === 'MONITOR' &&
        ['APPROVED', 'AUTO_APPROVED'].indexOf(
          String(row.review_status || '').toUpperCase()
        ) >= 0;
    }).length
  };
}

function loadMonitoringTargetStagingFromDrive_() {
  return loadMigrationStagingFromDrive_({
    fileProperty: 'MONITORING_STAGING_CSV_FILE_ID',
    loadConfirmationProperty: 'MONITORING_STAGING_LOAD_CONFIRMATION',
    loadedHashProperty: 'MONITORING_STAGING_LOADED_SHA256',
    loadPrefix: 'LOAD_MONITORING_STAGING_',
    sheetName: SHEETS.MonitoringTargetStaging,
    headers: MONITORING_TARGET_STAGING_HEADERS_,
    validate: validateMonitoringTargetStaging_,
    loadAction: 'LOAD_MONITORING_STAGING'
  });
}

function dryRunMonitoringTargetMigration_() {
  var validation = validateMonitoringTargetStaging_(
    readRecords_(SHEETS.MonitoringTargetStaging),
    true
  );
  var csvHash = String(
    getSecureSetting_('MONITORING_STAGING_LOADED_SHA256') || ''
  ).trim();
  return Object.assign({}, validation, {
    csvSha256: csvHash,
    confirmationRequired: 'APPLY_MONITORING_STAGING_' + csvHash.substring(0, 12)
  });
}

function applyMonitoringTargetMigration_() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', '監視対象移行が実行中です', true);
  }
  var changed = [];
  var audit = null;
  try {
    assertRecentBackupForMigration_();
    var rows = readRecords_(SHEETS.MonitoringTargetStaging);
    var validation = validateMonitoringTargetStaging_(rows, true);
    var csvHash = String(
      getSecureSetting_('MONITORING_STAGING_LOADED_SHA256') || ''
    ).trim();
    var expected = 'APPLY_MONITORING_STAGING_' + csvHash.substring(0, 12);
    if (!csvHash ||
        String(getSecureSetting_('MONITORING_STAGING_APPLY_CONFIRMATION') || '') !== expected) {
      throw appError_(
        'CONFIRMATION_REQUIRED',
        'MONITORING_STAGING_APPLY_CONFIRMATIONに' + expected + 'が必要です',
        false
      );
    }
    audit = appendAuditEvent_({
      user_email: 'TECHNICAL_ADMIN',
      action: 'APPLY_MONITORING_TARGETS',
      target_type: 'Companies',
      target_id: csvHash,
      details: JSON.stringify(validation),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();
    rows.forEach(function(row) {
      var reviewStatus = String(row.review_status || '').toUpperCase();
      var enabled = reviewStatus !== 'REJECTED' &&
        String(row.proposed_action || '').toUpperCase() === 'MONITOR';
      var company = findByKey_(SHEETS.Companies, 'company_id', row.company_id);
      var before = {
        permit_monitoring_enabled: company.permit_monitoring_enabled,
        data_version: company.data_version,
        updated_at: company.updated_at,
        updated_by: company.updated_by
      };
      changed.push({ row: company._row, before: before });
      if (!updateRecord_(SHEETS.Companies, company._row, {
        permit_monitoring_enabled: enabled,
        data_version: getCompanyVersion_(company) + 1,
        updated_at: getNowString_(),
        updated_by: 'MONITORING_MIGRATION'
      })) {
        throw appError_(
          'MONITORING_UPDATE_FAILED',
          '監視対象を更新できませんでした: ' + row.company_id,
          true
        );
      }
    });
    SpreadsheetApp.flush();
    if (!updateAuditEvent_(audit.log_id, {
      status: 'COMMITTED',
      details: JSON.stringify({
        validation: validation,
        changed: changed.length
      })
    })) {
      throw appError_(
        'AUDIT_COMMIT_FAILED',
        '監視対象移行の監査を確定できませんでした',
        true,
        audit.log_id
      );
    }
    return {
      auditId: audit.log_id,
      changedCount: changed.length,
      monitoringEnabledCount: validation.monitor
    };
  } catch (error) {
    changed.forEach(function(item) {
      try {
        updateRecord_(SHEETS.Companies, item.row, item.before);
      } catch (ignoredRollbackError) {
        console.error('[MONITORING_MIGRATION_ROLLBACK_FAILED]');
      }
    });
    try {
      SpreadsheetApp.flush();
    } catch (rollbackFlushError) {
      console.error('[MONITORING_MIGRATION_ROLLBACK_FLUSH_FAILED]');
    }
    if (audit) {
      try {
        updateAuditEvent_(audit.log_id, {
          status: 'ABORTED',
          error_code: String(error.code || 'MONITORING_MIGRATION_FAILED'),
          details: String(error.message || error).substring(0, 500)
        });
        SpreadsheetApp.flush();
      } catch (auditError) {
        console.error('[MONITORING_MIGRATION_ABORT_AUDIT_FAILED]');
      }
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}
