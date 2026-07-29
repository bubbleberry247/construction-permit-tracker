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
  let providerQuota = 100;

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
    getConfig: key => Object.prototype.hasOwnProperty.call(config, key) ? config[key] : '',
    NotificationsModel: {
      create(data) {
        const row = Object.assign({}, data, {
          notification_id: data.notification_id || `N-${notifications.length + 1}`,
          sent_at: data.sent_at || new Date()
        });
        notifications.push(row);
        return row;
      },
      updateById(id, updates) {
        const row = notifications.find(item => item.notification_id === id);
        if (!row) return false;
        Object.assign(row, updates);
        return true;
      },
      countReservedOrSentToday() {
        return notifications.filter(item => item.result === 'PENDING' || item.result === 'SENT').length;
      }
    },
    MailApp: {
      getRemainingDailyQuota: () => providerQuota
    },
    GmailApp: {
      sendEmail(to, subject, body, options) {
        sentEmails.push({ to, subject, body, options });
      }
    },
    Utilities: {
      getUuid: () => 'UUID',
      formatDate: () => '2026/07/30'
    }
  };
  context.logError = (message, error) => {
    consoleMessages.push(['logError', message, error && error.message]);
  };
  context.__state = {
    config,
    notifications,
    sentEmails,
    consoleMessages,
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
    getScriptLock: () => ({
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
  context.refreshCompanyView = () => {};
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
  assert.doesNotMatch(utils, /function\s+sendErrorAlert\s*\(/);
  assert.match(utils, /function\s+sendErrorAlert_\s*\(/);
  assert.doesNotMatch(scheduler, /function\s+runDailyNotifications\s*\(/);
  assert.match(scheduler, /function\s+runDailyNotifications_\s*\(/);
  assert.doesNotMatch(scheduler, /function\s+runNow\s*\(/);
  assert.match(scheduler, /function\s+runNow_\s*\(/);
  assert.doesNotMatch(ui, /function\s+setupDailyTrigger\s*\(/);
  assert.match(ui, /function\s+setupDailyTrigger_\s*\(/);
  assert.match(ui, /ScriptApp\.newTrigger\(FUNCTION_NAME\)/);
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

test('手動通知UIはsent=falseを成功表示しない', () => {
  const index = readSource('index.html');
  assert.match(index, /if \(r && r\.sent\)/);
  assert.match(index, /送信されませんでした/);
  assert.doesNotMatch(index, /if \(r\.dryRun\)/);
});

test('Webアプリ公開範囲をMYSELFに固定する', () => {
  const manifest = JSON.parse(readSource('appsscript.json'));
  assert.equal(manifest.webapp.access, 'MYSELF');
  assert.equal(manifest.webapp.executeAs, 'USER_DEPLOYING');
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
