/**
 * Backup.gs — スプレッドシート全体の日次・月次バックアップ
 *
 * BACKUP_FOLDER_ID はScript Propertiesへ設定する。指定フォルダ以外は触らない。
 * 日次35日、月次5年を超えたバックアップはゴミ箱へ移動する。
 */

var BACKUP_DAILY_PREFIX_ = 'PERMIT_BACKUP_DAILY_';
var BACKUP_MONTHLY_PREFIX_ = 'PERMIT_BACKUP_MONTHLY_';

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
 * Apps Scriptエディタから初回だけ実行する。既存の無関係なトリガーは触らない。
 * 公開前のUAT環境で動作確認し、証跡を残してから本番で実行する。
 */
function installManagedTriggers_() {
  var managedHandlers = {
    runDailyNotifications_: true,
    runDailyBackup_: true
  };
  ScriptApp.getProjectTriggers().forEach(function(trigger) {
    if (managedHandlers[trigger.getHandlerFunction()]) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  var notificationTrigger = ScriptApp.newTrigger('runDailyNotifications_')
    .timeBased()
    .atHour(8)
    .everyDays(1)
    .inTimezone('Asia/Tokyo')
    .create();
  var backupTrigger = ScriptApp.newTrigger('runDailyBackup_')
    .timeBased()
    .atHour(2)
    .everyDays(1)
    .inTimezone('Asia/Tokyo')
    .create();
  appendAuditEvent_({
    user_email: 'TECHNICAL_ADMIN',
    action: 'INSTALL_MANAGED_TRIGGERS',
    target_type: 'AppsScript',
    target_id: ScriptApp.getScriptId(),
    details: JSON.stringify({
      handlers: [
        notificationTrigger.getHandlerFunction(),
        backupTrigger.getHandlerFunction()
      ]
    }),
    status: 'COMMITTED'
  });
  return {
    notificationTriggerId: notificationTrigger.getUniqueId(),
    backupTriggerId: backupTrigger.getUniqueId()
  };
}
