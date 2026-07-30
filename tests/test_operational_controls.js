'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.resolve(__dirname, '..');
const source = fs.readFileSync(
  path.join(ROOT, 'src', 'Backup.gs'),
  'utf8'
);

function createContext() {
  let sequence = 0;
  const audits = [];
  const properties = { BACKUP_FOLDER_ID: 'UAT_BACKUP_FOLDER_12345' };
  const copies = [];
  const backupFiles = [];
  const folder = {
    getFiles() {
      let index = 0;
      return {
        hasNext: () => index < backupFiles.length,
        next: () => backupFiles[index++]
      };
    }
  };
  const sourceFile = {
    makeCopy(name) {
      const copy = {
        id: `COPY-${copies.length + 1}`,
        name,
        trashed: false,
        createdAt: new Date('2026-07-30T03:00:00Z'),
        getId() { return this.id; },
        getName() { return this.name; },
        isTrashed() { return this.trashed; },
        setTrashed(value) { this.trashed = value; },
        getDateCreated() { return this.createdAt; }
      };
      copies.push(copy);
      backupFiles.push(copy);
      return copy;
    }
  };
  const triggers = [{
    handler: 'unrelatedHandler_',
    id: 'UNRELATED-1',
    getHandlerFunction() { return this.handler; },
    getUniqueId() { return this.id; }
  }, {
    handler: 'runDailyNotifications_',
    id: 'OLD-MANAGED-1',
    getHandlerFunction() { return this.handler; },
    getUniqueId() { return this.id; }
  }];
  const context = {
    Date,
    JSON,
    Math,
    Number,
    Object,
    String,
    console,
    getSecureSetting_: key => properties[key] || '',
    setSecureSetting_: (key, value) => { properties[key] = value; },
    getNowString_: () => '2026-07-30 12:00:00',
    appendAuditEvent_: event => {
      const stored = { ...event, log_id: `A-${audits.length + 1}` };
      audits.push(stored);
      return { ...stored };
    },
    appError_: (code, message, retryable) => {
      const error = new Error(message);
      error.code = code;
      error.retryable = retryable;
      return error;
    },
    updateAuditEvent_: (logId, updates) => {
      const audit = audits.find(item => item.log_id === logId);
      if (!audit) return false;
      Object.assign(audit, updates);
      return true;
    },
    LockService: {
      getScriptLock: () => ({
        tryLock: () => true,
        releaseLock: () => {}
      })
    },
    SpreadsheetApp: {
      getActiveSpreadsheet: () => ({
        getId: () => 'SHEET-UAT',
        getSheets: () => [{
          getName: () => 'Companies',
          getLastRow: () => 132,
          getLastColumn: () => 20
        }, {
          getName: () => 'Permits',
          getLastRow: () => 20,
          getLastColumn: () => 34
        }]
      }),
      flush: () => {}
    },
    DriveApp: {
      getFolderById: () => folder,
      getFileById: () => sourceFile
    },
    Utilities: {
      formatDate: (date, timezone, pattern) => {
        if (pattern === 'yyyyMMdd_HHmmss') return '20260730_120000';
        if (pattern === 'yyyyMMdd') return '20260730';
        if (pattern === 'yyyyMM') return '202607';
        if (pattern === 'HHmmss') return '120000';
        if (pattern === 'd') return '30';
        throw new Error(`unexpected pattern: ${pattern}`);
      }
    },
    ScriptApp: {
      getProjectTriggers: () => triggers.slice(),
      deleteTrigger: trigger => {
        const index = triggers.indexOf(trigger);
        if (index >= 0) triggers.splice(index, 1);
      },
      getScriptId: () => 'SCRIPT-UAT',
      newTrigger: handler => {
        const pending = { handler };
        return {
          timeBased() { return this; },
          atHour(hour) { pending.hour = hour; return this; },
          everyDays(days) { pending.days = days; return this; },
          inTimezone(timezone) { pending.timezone = timezone; return this; },
          create() {
            const trigger = {
              ...pending,
              id: `TRIGGER-${++sequence}`,
              getHandlerFunction() { return this.handler; },
              getUniqueId() { return this.id; }
            };
            triggers.push(trigger);
            return trigger;
          }
        };
      }
    }
  };
  context.__triggers = triggers;
  context.__audits = audits;
  context.__properties = properties;
  context.__copies = copies;
  vm.createContext(context);
  vm.runInContext(source, context, { filename: 'Backup.gs' });
  return context;
}

const context = createContext();

assert.throws(
  () => context.installManagedTriggers_('BACKUP_ONLY', 'wrong'),
  error => error.code === 'TRIGGER_CONFIRMATION_REQUIRED'
);

let result = context.installManagedTriggers_(
  'BACKUP_ONLY',
  'INSTALL_MANAGED_TRIGGERS_BACKUP_ONLY'
);
assert.equal(result.status.total, 1);
assert.equal(result.status.handlers.runDailyBackup_.count, 1);
assert.equal(result.status.handlers.runDailyNotifications_.count, 0);
assert.equal(
  context.__triggers.filter(t => t.handler === 'unrelatedHandler_').length,
  1
);

result = context.installManagedTriggers_(
  'MLIT_SHADOW',
  'INSTALL_MANAGED_TRIGGERS_MLIT_SHADOW'
);
assert.equal(result.status.total, 3);
assert.equal(result.status.handlers.runDailyBackup_.count, 1);
assert.equal(result.status.handlers.runDailyMlitRolling_.count, 1);
assert.equal(result.status.handlers.runPreNotificationMlitRefresh_.count, 1);
assert.equal(result.status.handlers.runDailyNotifications_.count, 0);

result = context.installManagedTriggers_(
  'FULL',
  'INSTALL_MANAGED_TRIGGERS_FULL'
);
assert.equal(result.status.total, 4);
assert.deepEqual(
  context.__triggers
    .filter(t => t.handler !== 'unrelatedHandler_')
    .map(t => [t.handler, t.hour, t.days, t.timezone])
    .sort(),
  [
    ['runDailyBackup_', 2, 1, 'Asia/Tokyo'],
    ['runDailyMlitRolling_', 3, 1, 'Asia/Tokyo'],
    ['runDailyNotifications_', 8, 1, 'Asia/Tokyo'],
    ['runPreNotificationMlitRefresh_', 7, 1, 'Asia/Tokyo']
  ]
);
assert.equal(context.__audits.length, 3);
assert.equal(context.__audits[2].action, 'INSTALL_MANAGED_TRIGGERS');

assert.throws(
  () => context.createRecoveryCheckpointBackup_('POST_MIGRATION', 'wrong'),
  error => error.code === 'CHECKPOINT_CONFIRMATION_REQUIRED'
);
const checkpoint = context.createRecoveryCheckpointBackup_(
  'POST_MIGRATION',
  'CREATE_RECOVERY_CHECKPOINT_POST_MIGRATION'
);
assert.equal(context.__copies.length, 1);
assert.equal(
  checkpoint.fileName,
  'PERMIT_BACKUP_CHECKPOINT_20260730_120000_POST_MIGRATION'
);
assert.equal(checkpoint.sheetSummary[0].rows, 132);
assert.equal(context.__properties.LAST_BACKUP_STATUS, 'COMMITTED');
assert.equal(context.__properties.LAST_RECOVERY_CHECKPOINT_FILE_ID, 'COPY-1');
assert.equal(context.__audits[3].action, 'CREATE_RECOVERY_CHECKPOINT');
assert.equal(context.__audits[3].status, 'COMMITTED');

const failedCheckpointContext = createContext();
failedCheckpointContext.updateAuditEvent_ = () => false;
assert.throws(
  () => failedCheckpointContext.createRecoveryCheckpointBackup_(
    'AUDIT_FAILURE',
    'CREATE_RECOVERY_CHECKPOINT_AUDIT_FAILURE'
  ),
  error => error.code === 'AUDIT_COMMIT_FAILED'
);
assert.equal(
  failedCheckpointContext.__properties.LAST_BACKUP_STATUS,
  'ABORTED'
);
assert.equal(
  failedCheckpointContext.__properties.LAST_RECOVERY_CHECKPOINT_FILE_ID,
  undefined
);

console.log('operational controls tests passed');
