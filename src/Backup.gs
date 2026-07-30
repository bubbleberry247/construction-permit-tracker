/**
 * Backup.gs — スプレッドシート全体の日次・月次バックアップ
 *
 * BACKUP_FOLDER_ID はScript Propertiesへ設定する。指定フォルダ以外は触らない。
 * 日次35日、月次5年を超えたバックアップはゴミ箱へ移動する。
 */

var BACKUP_DAILY_PREFIX_ = 'PERMIT_BACKUP_DAILY_';
var BACKUP_MONTHLY_PREFIX_ = 'PERMIT_BACKUP_MONTHLY_';
var BACKUP_CHECKPOINT_PREFIX_ = 'PERMIT_BACKUP_CHECKPOINT_';

function getBackupFolder_() {
  var folderId = String(getSecureSetting_('BACKUP_FOLDER_ID') || '').trim();
  if (!folderId || !/^[A-Za-z0-9_-]{10,200}$/.test(folderId)) {
    throw appError_(
      'BACKUP_NOT_CONFIGURED',
      'BACKUP_FOLDER_IDがScript Propertiesに設定されていません',
      false
    );
  }
  try {
    return DriveApp.getFolderById(folderId);
  } catch (error) {
    throw appError_('BACKUP_FOLDER_UNAVAILABLE', 'バックアップ先を利用できません', true);
  }
}

function formatBackupDate_(date, pattern) {
  return Utilities.formatDate(date, 'Asia/Tokyo', pattern);
}

function findBackupForPeriod_(folder, prefix, periodKey) {
  var files = folder.getFiles();
  var expectedPrefix = prefix + periodKey + '_';
  while (files.hasNext()) {
    var file = files.next();
    if (String(file.getName() || '').indexOf(expectedPrefix) === 0 && !file.isTrashed()) {
      return file;
    }
  }
  return null;
}

function copySpreadsheetBackup_(folder, prefix, periodKey, now) {
  var existing = findBackupForPeriod_(folder, prefix, periodKey);
  if (existing) {
    return {
      created: false,
      fileId: existing.getId(),
      fileName: existing.getName()
    };
  }

  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var source = DriveApp.getFileById(spreadsheet.getId());
  var name = prefix + periodKey + '_' + formatBackupDate_(now, 'HHmmss');
  var copy = source.makeCopy(name, folder);
  return {
    created: true,
    fileId: copy.getId(),
    fileName: copy.getName()
  };
}

function moveExpiredBackupsToTrash_(folder, prefix, cutoffDate) {
  var files = folder.getFiles();
  var trashed = [];
  while (files.hasNext()) {
    var file = files.next();
    var name = String(file.getName() || '');
    if (name.indexOf(prefix) !== 0 || file.isTrashed()) continue;
    if (file.getDateCreated().getTime() >= cutoffDate.getTime()) continue;
    file.setTrashed(true);
    trashed.push({ fileId: file.getId(), fileName: name });
  }
  return trashed;
}

function subtractDays_(date, days) {
  var result = new Date(date.getTime());
  result.setDate(result.getDate() - days);
  return result;
}

function subtractYears_(date, years) {
  var result = new Date(date.getTime());
  result.setFullYear(result.getFullYear() - years);
  return result;
}

/**
 * 日次time-driven trigger用。バックアップ以外のDriveファイルは変更しない。
 */
function runDailyBackup_() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', 'バックアップ処理が実行中です', true);
  }
  try {
    var now = new Date();
    var folder = getBackupFolder_();
    var daily = copySpreadsheetBackup_(
      folder,
      BACKUP_DAILY_PREFIX_,
      formatBackupDate_(now, 'yyyyMMdd'),
      now
    );
    var monthly = null;
    if (Number(formatBackupDate_(now, 'd')) === 1) {
      monthly = copySpreadsheetBackup_(
        folder,
        BACKUP_MONTHLY_PREFIX_,
        formatBackupDate_(now, 'yyyyMM'),
        now
      );
    }

    var trashedDaily = moveExpiredBackupsToTrash_(
      folder,
      BACKUP_DAILY_PREFIX_,
      subtractDays_(now, 35)
    );
    var trashedMonthly = moveExpiredBackupsToTrash_(
      folder,
      BACKUP_MONTHLY_PREFIX_,
      subtractYears_(now, 5)
    );

    var completedAt = getNowString_();
    setSecureSetting_('LAST_BACKUP_AT', now.toISOString());
    setSecureSetting_('LAST_BACKUP_DISPLAY_AT', completedAt);
    setSecureSetting_('LAST_BACKUP_STATUS', 'COMMITTED');
    var audit = appendAuditEvent_({
      user_email: 'SYSTEM_BACKUP',
      action: 'DAILY_BACKUP',
      target_type: 'Spreadsheet',
      target_id: SpreadsheetApp.getActiveSpreadsheet().getId(),
      details: JSON.stringify({
        daily: daily,
        monthly: monthly,
        trashedDaily: trashedDaily,
        trashedMonthly: trashedMonthly
      }),
      status: 'COMMITTED'
    });
    return {
      daily: daily,
      monthly: monthly,
      trashedDailyCount: trashedDaily.length,
      trashedMonthlyCount: trashedMonthly.length,
      completedAt: completedAt,
      auditId: audit.log_id
    };
  } catch (error) {
    setSecureSetting_('LAST_BACKUP_AT', getNowString_());
    setSecureSetting_('LAST_BACKUP_STATUS', 'ABORTED');
    try {
      appendAuditEvent_({
        user_email: 'SYSTEM_BACKUP',
        action: 'DAILY_BACKUP',
        target_type: 'Spreadsheet',
        target_id: '',
        details: String(error.message || error).substring(0, 500),
        status: 'ABORTED',
        error_code: String(error.code || 'BACKUP_FAILED')
      });
    } catch (ignoredAuditFailure) {
      console.error('[BACKUP_AUDIT_FAILED]');
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

/**
 * 移行後・復旧演習前など、日次バックアップと別に明示的な復旧点を作る。
 * label: 英大文字・数字・_・- の1〜32文字
 * confirmation: CREATE_RECOVERY_CHECKPOINT_<label>
 */
function createRecoveryCheckpointBackup_(label, confirmation) {
  var normalizedLabel = String(label || '').trim().toUpperCase();
  if (!/^[A-Z0-9_-]{1,32}$/.test(normalizedLabel)) {
    throw appError_(
      'CHECKPOINT_LABEL_INVALID',
      '復旧チェックポイントlabelが不正です',
      false
    );
  }
  var expected = 'CREATE_RECOVERY_CHECKPOINT_' + normalizedLabel;
  if (String(confirmation || '') !== expected) {
    throw appError_(
      'CHECKPOINT_CONFIRMATION_REQUIRED',
      '復旧チェックポイント用の確認文字列が一致しません',
      false
    );
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw appError_('LOCK_TIMEOUT', 'バックアップ処理が実行中です', true);
  }
  var audit = null;
  try {
    var now = new Date();
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var folder = getBackupFolder_();
    var source = DriveApp.getFileById(spreadsheet.getId());
    var fileName = BACKUP_CHECKPOINT_PREFIX_ +
      formatBackupDate_(now, 'yyyyMMdd_HHmmss') + '_' + normalizedLabel;
    audit = appendAuditEvent_({
      user_email: 'TECHNICAL_ADMIN',
      action: 'CREATE_RECOVERY_CHECKPOINT',
      target_type: 'Spreadsheet',
      target_id: spreadsheet.getId(),
      details: JSON.stringify({ label: normalizedLabel, fileName: fileName }),
      status: 'PREPARED'
    });
    SpreadsheetApp.flush();

    var copy = source.makeCopy(fileName, folder);
    var sheetSummary = spreadsheet.getSheets().map(function(sheet) {
      return {
        name: sheet.getName(),
        rows: sheet.getLastRow(),
        columns: sheet.getLastColumn()
      };
    });
    var trashed = moveExpiredBackupsToTrash_(
      folder,
      BACKUP_CHECKPOINT_PREFIX_,
      subtractDays_(now, 35)
    );
    if (!updateAuditEvent_(audit.log_id, {
      status: 'COMMITTED',
      details: JSON.stringify({
        label: normalizedLabel,
        fileId: copy.getId(),
        fileName: copy.getName(),
        sheetSummary: sheetSummary,
        trashedCheckpoints: trashed
      })
    })) {
      throw appError_(
        'AUDIT_COMMIT_FAILED',
        '復旧チェックポイントの監査を確定できませんでした',
        true,
        audit.log_id
      );
    }
    SpreadsheetApp.flush();
    var completedAt = getNowString_();
    setSecureSetting_('LAST_BACKUP_AT', now.toISOString());
    setSecureSetting_('LAST_BACKUP_DISPLAY_AT', completedAt);
    setSecureSetting_('LAST_BACKUP_STATUS', 'COMMITTED');
    setSecureSetting_('LAST_RECOVERY_CHECKPOINT_FILE_ID', copy.getId());
    setSecureSetting_('LAST_RECOVERY_CHECKPOINT_AT', now.toISOString());
    return {
      auditId: audit.log_id,
      fileId: copy.getId(),
      fileName: copy.getName(),
      completedAt: completedAt,
      sheetSummary: sheetSummary
    };
  } catch (error) {
    try {
      setSecureSetting_('LAST_BACKUP_AT', getNowString_());
      setSecureSetting_('LAST_BACKUP_STATUS', 'ABORTED');
    } catch (ignoredPropertyFailure) {
      console.error('[CHECKPOINT_BACKUP_PROPERTY_FAILED]');
    }
    if (audit) {
      try {
        updateAuditEvent_(audit.log_id, {
          status: 'ABORTED',
          error_code: String(error.code || 'CHECKPOINT_BACKUP_FAILED'),
          details: String(error.message || error).substring(0, 500)
        });
        SpreadsheetApp.flush();
      } catch (ignoredAuditFailure) {
        console.error('[CHECKPOINT_BACKUP_AUDIT_FAILED]');
      }
    }
    throw error;
  } finally {
    lock.releaseLock();
  }
}

var MANAGED_TRIGGER_HOURS_ = {
  runDailyBackup_: 2,
  runDailyMlitRolling_: 3,
  runPreNotificationMlitRefresh_: 7,
  runDailyNotifications_: 8
};

var MANAGED_TRIGGER_PROFILES_ = {
  BACKUP_ONLY: ['runDailyBackup_'],
  MLIT_SHADOW: [
    'runDailyBackup_',
    'runDailyMlitRolling_',
    'runPreNotificationMlitRefresh_'
  ],
  FULL: [
    'runDailyBackup_',
    'runDailyMlitRolling_',
    'runPreNotificationMlitRefresh_',
    'runDailyNotifications_'
  ]
};

function getManagedTriggerStatus_() {
  var handlers = {};
  Object.keys(MANAGED_TRIGGER_HOURS_).forEach(function(handler) {
    handlers[handler] = { count: 0, hour: MANAGED_TRIGGER_HOURS_[handler] };
  });
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    var handler = trigger.getHandlerFunction();
    if (!handlers[handler]) return;
    handlers[handler].count++;
  });
  var total = Object.keys(handlers).reduce(function(sum, handler) {
    return sum + handlers[handler].count;
  }, 0);
  return { total: total, handlers: handlers };
}

function createManagedTimeTrigger_(handler) {
  return ScriptApp.newTrigger(handler)
    .timeBased()
    .atHour(MANAGED_TRIGGER_HOURS_[handler])
    .everyDays(1)
    .inTimezone('Asia/Tokyo')
    .create();
}

/**
 * Apps Scriptエディタから段階ごとに実行する。既存の無関係なトリガーは触らない。
 * profile: BACKUP_ONLY / MLIT_SHADOW / FULL
 * confirmation: INSTALL_MANAGED_TRIGGERS_<profile>
 */
function installManagedTriggers_(profile, confirmation) {
  var normalizedProfile = String(profile || '').trim().toUpperCase();
  var selectedHandlers = MANAGED_TRIGGER_PROFILES_[normalizedProfile];
  if (!selectedHandlers) {
    throw appError_(
      'INVALID_TRIGGER_PROFILE',
      'trigger profileはBACKUP_ONLY、MLIT_SHADOW、FULLから選択してください',
      false
    );
  }
  if (String(confirmation || '') !==
      'INSTALL_MANAGED_TRIGGERS_' + normalizedProfile) {
    throw appError_(
      'TRIGGER_CONFIRMATION_REQUIRED',
      'trigger設置用の確認文字列が一致しません',
      false
    );
  }

  var managedHandlers = {};
  Object.keys(MANAGED_TRIGGER_HOURS_).forEach(function(handler) {
    managedHandlers[handler] = true;
  });
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    if (managedHandlers[trigger.getHandlerFunction()]) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  var created = {};
  selectedHandlers.forEach(function(handler) {
    created[handler] = createManagedTimeTrigger_(handler).getUniqueId();
  });
  appendAuditEvent_({
    user_email: 'TECHNICAL_ADMIN',
    action: 'INSTALL_MANAGED_TRIGGERS',
    target_type: 'AppsScript',
    target_id: ScriptApp.getScriptId(),
    details: JSON.stringify({
      profile: normalizedProfile,
      handlers: selectedHandlers,
      triggerIds: created
    }),
    status: 'COMMITTED'
  });
  return {
    profile: normalizedProfile,
    triggerIds: created,
    status: getManagedTriggerStatus_()
  };
}
