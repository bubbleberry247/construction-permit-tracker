'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.resolve(__dirname, '..');
const SRC = path.join(ROOT, 'src');

function readSource(name) {
  return fs.readFileSync(path.join(SRC, name), 'utf8');
}

function createUtilsContext(configOverrides) {
  const config = Object.assign({
    ENABLE_SEND: 'FALSE',
    GMAIL_DAILY_LIMIT: '150',
    ADMIN_EMAILS: 'admin@example.com',
    NOTIFY_STAGES_DAYS: '90,60,30,0'
  }, configOverrides || {});
  const notifications = [];
  const sentEmails = [];
  const consoleMessages = [];
  const operationEvents = [];
  let providerQuota = 100;
  let sendLockHeld = false;

  const sendLock = {
    tryLock() {
      if (sendLockHeld) return false;
      sendLockHeld = true;
      return true;
    },
    releaseLock() {
      operationEvents.push('release');
      sendLockHeld = false;
    }
  };

  const context = {
    Date,
    Error,
    JSON,
    Math,
    Number,
    Object,
    RegExp,
    String,
    console: {
      error: (...args) => consoleMessages.push(['error', ...args]),
      warn: (...args) => consoleMessages.push(['warn', ...args]),
      log: (...args) => consoleMessages.push(['log', ...args])
    },
    getConfig_: key => Object.prototype.hasOwnProperty.call(config, key) ? config[key] : '',
    reloadConfigAll_: () => Object.assign({}, config),
    NotificationsModel: {
      create(data) {
        operationEvents.push(`create:${data.result}`);
        const row = Object.assign({}, data, {
          notification_id: data.notification_id || `N-${notifications.length + 1}`,
          sent_at: data.sent_at || new Date()
        });
        notifications.push(row);
        return row;
      },
      updateById(id, updates) {
        operationEvents.push(`update:${updates.result || ''}`);
        const row = notifications.find(item => item.notification_id === id);
        if (!row) return false;
        Object.assign(row, updates);
        return true;
      },
      countReservedOrSentToday() {
        return notifications.filter(item => item.result === 'PENDING' || item.result === 'SENT').length;
      },
      hasBeenReservedOrSent(permitId, stage) {
        return notifications.some(item =>
          String(item.permit_id || '') === String(permitId) &&
          String(item.stage || '') === String(stage) &&
          (item.result === 'PENDING' || item.result === 'SENT')
        );
      }
    },
    LockService: {
      getScriptLock: () => sendLock
    },
    MailApp: {
      getRemainingDailyQuota: () => providerQuota
    },
    GmailApp: {
      sendEmail(to, subject, body, options) {
        operationEvents.push('gmail');
        sentEmails.push({ to, subject, body, options });
      }
    },
    SpreadsheetApp: {
      flush() {
        operationEvents.push('flush');
      }
    },
    Utilities: {
      getUuid: () => 'UUID',
      formatDate: (_date, _timezone, format) =>
        format === 'yyyy-MM' ? '2026-07' : '2026/07/30'
    }
  };
  context.logError_ = (message, error) => {
    consoleMessages.push(['logError', message, error && error.message]);
  };
  context.__state = {
    config,
    notifications,
    sentEmails,
    consoleMessages,
    operationEvents,
    isSendLockHeld() {
      return sendLockHeld;
    },
    setProviderQuota(value) {
      providerQuota = value;
    }
  };

  vm.createContext(context);
  vm.runInContext(readSource('Utils.gs'), context, { filename: 'Utils.gs' });
  return context;
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test('ENABLE_SENDは明示TRUEだけ許可する', () => {
  const matrix = new Map([
    ['TRUE', true],
    ['true', true],
    [' TrUe ', true],
    ['FALSE', false],
    ['false', false],
    ['', false],
    ['1', false],
    ['yes', false],
    [undefined, false]
  ]);
  for (const [value, expected] of matrix.entries()) {
    const context = createUtilsContext({ ENABLE_SEND: value });
    assert.equal(context.isSendEnabled_(), expected, `ENABLE_SEND=${String(value)}`);
  }
});

test('NOTIFY_STAGES_DAYSを厳密検証・重複除去・降順化する', () => {
  const context = createUtilsContext();
  assert.deepEqual(
    Array.from(context.parseNotifyStages_('30,90,60,30,0')),
    [90, 60, 30, 0]
  );
  for (const invalid of ['', '9060300', '90, 60,30,0', '90,,30', '-1,0', '366,30', '1.5,0']) {
    assert.throws(
      () => context.parseNotifyStages_(invalid),
      error => error && error.code === 'INVALID_NOTIFY_STAGES',
      `invalid=${invalid}`
    );
  }
});

test('ENABLE_SEND=FALSEでは送信せずBLOCKEDを記録する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'FALSE' });
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.sent, false);
  assert.equal(result.result, 'BLOCKED_SEND_DISABLED');
  assert.equal(context.__state.sentEmails.length, 0);
  assert.equal(context.__state.notifications[0].result, 'BLOCKED_SEND_DISABLED');
});

test('ロック待機中にENABLE_SENDがTRUEからFALSEへ変わった場合はfresh Configで送信停止する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const sheetConfig = {
    ENABLE_SEND: 'TRUE',
    GMAIL_DAILY_LIMIT: '150',
    ADMIN_EMAILS: 'admin@example.com',
    NOTIFY_STAGES_DAYS: '90,60,30,0'
  };
  context.SpreadsheetApp = {
    flush() {
      context.__state.operationEvents.push('flush');
    },
    getActiveSpreadsheet: () => ({
      getSheetByName: name => name === 'Config'
        ? {
            getDataRange: () => ({
              getValues: () => [
                ['key', 'value'],
                ...Object.entries(sheetConfig)
              ]
            })
          }
        : null
    })
  };
  vm.runInContext(readSource('Config.gs'), context, { filename: 'Config.gs' });

  // 最初の判定が読む実行内キャッシュにはTRUEを保持させる。
  assert.equal(context.getConfig_('ENABLE_SEND'), 'TRUE');
  context.LockService = {
    getScriptLock: () => ({
      tryLock() {
        // ロック待機中に運用者が緊急停止した状況を再現。
        sheetConfig.ENABLE_SEND = 'FALSE';
        return true;
      },
      releaseLock() {}
    })
  };

  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-CACHE', stage: '90' }
  });

  assert.equal(result.result, 'BLOCKED_SEND_DISABLED');
  assert.equal(context.__state.sentEmails.length, 0);
  assert.equal(context.CONFIG_CACHE_.ENABLE_SEND, 'FALSE');
});

test('日次上限の不正値は安全側に倒して送信しない', () => {
  const context = createUtilsContext({
    ENABLE_SEND: 'TRUE',
    GMAIL_DAILY_LIMIT: 'invalid'
  });
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.result, 'BLOCKED_CONFIG');
  assert.equal(context.__state.sentEmails.length, 0);
});

test('Gmail実残量が受信者数未満なら送信しない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.__state.setProviderQuota(1);
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: { bcc: 'admin@example.com' },
    notification: { stage: '90' }
  });
  assert.equal(result.result, 'BLOCKED_PROVIDER_QUOTA');
  assert.equal(context.__state.sentEmails.length, 0);
});

test('Gmail実残量を確認できない場合は送信しない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.MailApp.getRemainingDailyQuota = () => {
    throw new Error('quota unavailable');
  };
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.result, 'BLOCKED_QUOTA_CHECK');
  assert.equal(context.__state.sentEmails.length, 0);
});

test('内部エラー通知もENABLE_SEND共通ゲートを迂回しない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'FALSE' });
  const result = context.sendErrorAlert_('internal', 'detail');
  assert.equal(result.result, 'BLOCKED_SEND_DISABLED');
  assert.equal(context.__state.sentEmails.length, 0);
  assert.equal(context.__state.notifications[0].stage, 'ERROR_ALERT');
});

test('送信前PENDING・送信後SENTを記録する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.sent, true);
  assert.equal(context.__state.sentEmails.length, 1);
  assert.equal(context.__state.notifications.length, 1);
  assert.equal(context.__state.notifications[0].result, 'SENT');
});

test('PENDING記録失敗時はメールを送らない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.NotificationsModel.create = () => {
    throw new Error('log unavailable');
  };
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.result, 'BLOCKED_LOG_FAILURE');
  assert.equal(context.__state.sentEmails.length, 0);
});

test('PENDINGをflushできない場合はGmailを呼ばない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.SpreadsheetApp.flush = () => {
    context.__state.operationEvents.push('flush:error');
    throw new Error('flush unavailable');
  };
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-FLUSH', stage: '90' }
  });

  assert.equal(result.result, 'BLOCKED_LOG_FAILURE');
  assert.equal(result.sent, false);
  assert.equal(context.__state.sentEmails.length, 0);
  assert.equal(context.__state.notifications[0].result, 'PENDING');
});

test('PENDING flushはGmail前、結果flushはScriptLock解放前に実行する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-ORDER', stage: '90' }
  });

  assert.equal(result.result, 'SENT');
  assert.deepEqual(
    context.__state.operationEvents,
    ['create:PENDING', 'flush', 'gmail', 'update:SENT', 'flush', 'release']
  );
});

test('遅延commitのNotificationsでもpre-send flush後のPENDINGが次回送信を止める', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const persisted = [];
  let staged = null;
  context.NotificationsModel = {
    create(data) {
      staged = Object.assign({}, data, {
        notification_id: 'N-DELAYED',
        sent_at: new Date()
      });
      context.__state.operationEvents.push('create:PENDING');
      return staged;
    },
    updateById() {
      throw new Error('SENT update unavailable');
    },
    countReservedOrSentToday() {
      return persisted.filter(row => row.result === 'PENDING' || row.result === 'SENT').length;
    },
    hasBeenReservedOrSent(permitId, stage) {
      return persisted.some(row =>
        row.permit_id === permitId &&
        row.stage === stage &&
        (row.result === 'PENDING' || row.result === 'SENT')
      );
    }
  };
  context.SpreadsheetApp.flush = () => {
    context.__state.operationEvents.push('flush');
    if (staged) {
      persisted.push(staged);
      staged = null;
    }
  };
  const params = {
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-DELAYED', stage: '90' }
  };

  const first = context.sendSystemEmail_(params);
  const second = context.sendSystemEmail_(params);

  assert.equal(first.sent, true);
  assert.equal(persisted[0].result, 'PENDING');
  assert.equal(second.result, 'BLOCKED_DUPLICATE_RESERVATION');
  assert.equal(context.__state.sentEmails.length, 1);
});

test('メール送信成功後のSENTログ更新失敗を送信失敗と誤判定しない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.NotificationsModel.updateById = () => {
    throw new Error('update unavailable');
  };
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.sent, true);
  assert.equal(result.result, 'SENT');
  assert.equal(result.logUpdated, false);
  assert.equal(context.__state.sentEmails.length, 1);
});

test('送信成功後にSENT更新が失敗してPENDINGが残っても次回は再送しない', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.NotificationsModel.updateById = () => {
    throw new Error('update unavailable');
  };
  const params = {
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-001', stage: '90' }
  };

  const first = context.sendSystemEmail_(params);
  const second = context.sendSystemEmail_(params);

  assert.equal(first.sent, true);
  assert.equal(context.__state.notifications[0].result, 'PENDING');
  assert.equal(second.sent, false);
  assert.equal(second.result, 'BLOCKED_DUPLICATE_RESERVATION');
  assert.equal(context.__state.sentEmails.length, 1);
});

test('月次サマリーは月別冪等キーを持ち、PENDING残留時も同月に再送しない', () => {
  const context = createUtilsContext({
    ENABLE_SEND: 'TRUE',
    ADMIN_EMAILS: 'admin@example.com'
  });
  context.PermitsModel = { getAllActive: () => [] };
  context.CompaniesModel = { findById: () => null };
  vm.runInContext(readSource('Mailer.gs'), context, { filename: 'Mailer.gs' });
  context.NotificationsModel.updateById = () => {
    throw new Error('update unavailable');
  };

  const first = context.Mailer.sendMonthlySummary();
  const second = context.Mailer.sendMonthlySummary();

  assert.equal(first.sent, true);
  assert.equal(context.__state.notifications[0].permit_id, 'MONTHLY:2026-07');
  assert.equal(context.__state.notifications[0].result, 'PENDING');
  assert.equal(second.result, 'BLOCKED_DUPLICATE_RESERVATION');
  assert.equal(context.__state.sentEmails.length, 1);
});

test('月次サマリーは月が変われば別冪等キーで送信できる', () => {
  const context = createUtilsContext({
    ENABLE_SEND: 'TRUE',
    ADMIN_EMAILS: 'admin@example.com'
  });
  context.PermitsModel = { getAllActive: () => [] };
  context.CompaniesModel = { findById: () => null };
  vm.runInContext(readSource('Mailer.gs'), context, { filename: 'Mailer.gs' });
  const originalFormatDate = context.Utilities.formatDate;

  const first = context.Mailer.sendMonthlySummary();
  context.Utilities.formatDate = (date, timezone, format) =>
    format === 'yyyy-MM' ? '2026-08' : originalFormatDate(date, timezone, format);
  const second = context.Mailer.sendMonthlySummary();

  assert.equal(first.result, 'SENT');
  assert.equal(second.result, 'SENT');
  assert.equal(context.__state.notifications[0].permit_id, 'MONTHLY:2026-07');
  assert.equal(context.__state.notifications[1].permit_id, 'MONTHLY:2026-08');
  assert.equal(context.__state.sentEmails.length, 2);
});

test('permitを持たないERROR_ALERTは繰り返し可能イベントとして重複許可する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const first = context.sendErrorAlert_('one', 'detail');
  const second = context.sendErrorAlert_('two', 'detail');

  assert.equal(first.result, 'SENT');
  assert.equal(second.result, 'SENT');
  assert.equal(context.__state.sentEmails.length, 2);
});

test('FAILEDは再試行可能で、PENDINGとSENTだけが重複送信を止める', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const originalSend = context.GmailApp.sendEmail;
  context.GmailApp.sendEmail = () => {
    throw new Error('simulated failure');
  };
  const params = {
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-002', stage: '60' }
  };
  const first = context.sendSystemEmail_(params);
  context.GmailApp.sendEmail = originalSend;
  const second = context.sendSystemEmail_(params);

  assert.equal(first.result, 'FAILED');
  assert.equal(second.result, 'SENT');
  assert.equal(context.__state.sentEmails.length, 1);
});

test('上限確認から結果更新まで共通ScriptLockを保持する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  const originalCount = context.NotificationsModel.countReservedOrSentToday;
  const originalQuota = context.MailApp.getRemainingDailyQuota;
  const originalCreate = context.NotificationsModel.create;
  const originalSend = context.GmailApp.sendEmail;
  const originalUpdate = context.NotificationsModel.updateById;

  context.NotificationsModel.countReservedOrSentToday = () => {
    assert.equal(context.__state.isSendLockHeld(), true);
    return originalCount();
  };
  context.MailApp.getRemainingDailyQuota = () => {
    assert.equal(context.__state.isSendLockHeld(), true);
    return originalQuota();
  };
  context.NotificationsModel.create = data => {
    assert.equal(context.__state.isSendLockHeld(), true);
    return originalCreate(data);
  };
  context.GmailApp.sendEmail = (...args) => {
    assert.equal(context.__state.isSendLockHeld(), true);
    return originalSend(...args);
  };
  context.NotificationsModel.updateById = (id, updates) => {
    assert.equal(context.__state.isSendLockHeld(), true);
    return originalUpdate(id, updates);
  };

  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-003', stage: '30' }
  });
  assert.equal(result.result, 'SENT');
  assert.equal(context.__state.isSendLockHeld(), false);
});

test('競合実行は共通ScriptLockで止まり、Gmail送信は1回だけ', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  let competingResult = null;
  const originalSend = context.GmailApp.sendEmail;
  context.GmailApp.sendEmail = (...args) => {
    competingResult = context.sendSystemEmail_({
      to: 'other@example.com',
      subject: 'competing',
      body: 'body',
      options: {},
      notification: { permit_id: 'P-004', stage: '90' }
    });
    return originalSend(...args);
  };

  const first = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'first',
    body: 'body',
    options: {},
    notification: { permit_id: 'P-004', stage: '90' }
  });

  assert.equal(first.result, 'SENT');
  assert.equal(competingResult.result, 'BLOCKED_SEND_LOCK');
  assert.equal(context.__state.sentEmails.length, 1);
});

test('設定上限到達後はループ途中でも次の送信を止める', () => {
  const context = createUtilsContext({
    ENABLE_SEND: 'TRUE',
    GMAIL_DAILY_LIMIT: '1'
  });
  const first = context.sendSystemEmail_({
    to: 'one@example.com',
    subject: 'one',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  const second = context.sendSystemEmail_({
    to: 'two@example.com',
    subject: 'two',
    body: 'body',
    options: {},
    notification: { stage: '60' }
  });
  assert.equal(first.result, 'SENT');
  assert.equal(second.result, 'BLOCKED_CONFIG_LIMIT');
  assert.equal(context.__state.sentEmails.length, 1);
});

test('Gmail送信例外はFAILEDへ更新する', () => {
  const context = createUtilsContext({ ENABLE_SEND: 'TRUE' });
  context.GmailApp.sendEmail = () => {
    throw new Error('simulated failure');
  };
  const result = context.sendSystemEmail_({
    to: 'vendor@example.com',
    subject: 'subject',
    body: 'body',
    options: {},
    notification: { stage: '90' }
  });
  assert.equal(result.result, 'FAILED');
  assert.equal(context.__state.notifications[0].result, 'FAILED');
  assert.match(context.__state.notifications[0].error_message, /simulated failure/);
});

test('不正な通知日数では日次処理を1件も開始しない', () => {
  const context = createUtilsContext({
    ENABLE_SEND: 'TRUE',
    NOTIFY_STAGES_DAYS: '9060300'
  });
  let permitReads = 0;
  let alertCalls = 0;
  context.LockService = {
    getDocumentLock: () => ({
      waitLock() {},
      releaseLock() {}
    })
  };
  context.PermitsModel = {
    getAllActive() {
      permitReads++;
      return [];
    }
  };
  context.Mailer = { sendMonthlySummary() {} };
  context.refreshCompanyView_ = () => {};
  context.sendErrorAlert_ = () => {
    alertCalls++;
  };
  vm.runInContext(readSource('Scheduler.gs'), context, { filename: 'Scheduler.gs' });

  const result = context.runDailyNotifications_();
  assert.equal(result.success, false);
  assert.equal(permitReads, 0);
  assert.equal(alertCalls, 0, '不正通知日数では内部エラーメールも送らない');
});

test('Gmail送信は非公開共通ゲートの1箇所だけに存在する', () => {
  const gasFiles = fs.readdirSync(SRC).filter(name => name.endsWith('.gs'));
  const occurrences = [];
  for (const file of gasFiles) {
    const source = readSource(file);
    const matches = source.match(/GmailApp\.sendEmail\s*\(/g) || [];
    for (let i = 0; i < matches.length; i++) occurrences.push(file);
  }
  assert.deepEqual(occurrences, ['Utils.gs']);
});

test('危険な通知入口は公開トップレベル関数として残さない', () => {
  const utils = readSource('Utils.gs');
  const scheduler = readSource('Scheduler.gs');
  const ui = readSource('Ui.gs');
  const api = readSource('api.gs');
  const index = readSource('index.html');
  const logic = readSource('logic.gs');
  assert.doesNotMatch(utils, /function\s+sendErrorAlert\s*\(/);
  assert.match(utils, /function\s+sendErrorAlert_\s*\(/);
  assert.doesNotMatch(scheduler, /function\s+runDailyNotifications\s*\(/);
  assert.match(scheduler, /function\s+runDailyNotifications_\s*\(/);
  assert.doesNotMatch(scheduler, /function\s+runNow\s*\(/);
  assert.match(scheduler, /function\s+runNow_\s*\(/);
  assert.doesNotMatch(ui, /function\s+setupDailyTrigger\s*\(/);
  assert.match(ui, /function\s+setupDailyTrigger_\s*\(/);
  assert.match(ui, /ScriptApp\.newTrigger\(FUNCTION_NAME\)/);
  assert.doesNotMatch(api, /function\s+apiSendNotification\s*\(/);
  assert.doesNotMatch(index, /今すぐ通知送信/);
  assert.doesNotMatch(index, /function\s+sendNotification\s*\(/);
  assert.doesNotMatch(logic, /function\s+sendManualNotification_\s*\(/);
});

test('Phase 0中の危険メニューを表示しない', () => {
  const ui = readSource('Ui.gs');
  const onOpenStart = ui.indexOf('function onOpen()');
  const nextComment = ui.indexOf('/**', onOpenStart + 1);
  const onOpenSource = ui.slice(onOpenStart, nextComment);
  assert.doesNotMatch(onOpenSource, /テストメール送信/);
  assert.doesNotMatch(onOpenSource, /シートヘッダ初期化/);
  assert.doesNotMatch(onOpenSource, /日次トリガー設定/);
});

test('Schedulerの外側ロックは送信ゲートと別種で自己デッドロックを避ける', () => {
  const scheduler = readSource('Scheduler.gs');
  assert.match(scheduler, /function\s+runDailyNotifications_\s*\(\)[\s\S]*?getDocumentLock\(\)/);
  assert.doesNotMatch(
    scheduler,
    /function\s+runDailyNotifications_\s*\(\)[\s\S]*?getScriptLock\(\)/
  );
});

test('Webアプリ公開範囲をMYSELFに固定する', () => {
  const manifest = JSON.parse(readSource('appsscript.json'));
  assert.equal(manifest.webapp.access, 'MYSELF');
  assert.equal(manifest.webapp.executeAs, 'USER_DEPLOYING');
  assert.ok(
    manifest.oauthScopes.includes('https://www.googleapis.com/auth/script.send_mail'),
    'MailApp.getRemainingDailyQuota()に必要なscope'
  );
});

test('GMAIL_DAILY_LIMITを必須設定として検査する', () => {
  const config = readSource('Config.gs');
  assert.match(config, /REQUIRED_KEYS[\s\S]*?'GMAIL_DAILY_LIMIT'/);
});

let failures = 0;
for (const item of tests) {
  try {
    item.fn();
    process.stdout.write(`PASS ${item.name}\n`);
  } catch (error) {
    failures++;
    process.stderr.write(`FAIL ${item.name}\n${error.stack}\n`);
  }
}

if (failures > 0) {
  process.stderr.write(`${failures} test(s) failed\n`);
  process.exit(1);
}

process.stdout.write(`${tests.length} test(s) passed\n`);
