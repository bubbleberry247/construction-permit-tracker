'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.resolve(__dirname, '..');
const SRC = path.join(ROOT, 'src');
const source = name => fs.readFileSync(path.join(SRC, name), 'utf8');

const tests = [];
function test(name, fn) { tests.push({ name, fn }); }

function baseContext() {
  const rows = {
    Companies: [{
      _row: 2,
      company_id: 'C0001',
      vendor_no: '101',
      company_name_raw: '株式会社テスト',
      company_name_normalized: '株式会社テスト',
      representative_name: '',
      contact_person: '旧担当',
      contact_email: 'old@example.com',
      contact_email_cc: '',
      phone: '',
      internal_owner_email: '',
      contact_verified_at: '2026-07-01 10:00:00',
      contact_verified_by: 'm-fujita@tokai-ic.co.jp',
      notification_mode: 'MANUAL',
      status: 'ACTIVE',
      permit_monitoring_enabled: true,
      data_version: 1,
      updated_at: '2026-07-01 10:00:00',
      updated_by: 'm-fujita@tokai-ic.co.jp'
    }],
    UserAccess: [
      {
        _row: 2, email: 'm-fujita@tokai-ic.co.jp', role: 'master_editor',
        active: true, displayName: '藤田', managerEmail: 'kanri.tic@tokai-ic.co.jp',
        canSendExternal: true
      },
      {
        _row: 3, email: 'kanri.tic@tokai-ic.co.jp', role: 'operations_admin',
        active: true, displayName: '管理', canSendExternal: true
      }
    ],
    AuditLog: [],
    NotificationQueue: [{
      _row: 2, queue_id: 'Q1', company_id: 'C0001', permit_id: 'P1',
      stage: '90', source_data_version: 1, status: 'READY',
      source_company_version: 1, source_permit_version: 1,
      source_expiry_date: '2026-08-31',
      to_email: 'old@example.com', subject: 'subject', body_template: 'body',
      idempotency_key: 'PERMIT:P1:EXPIRY:2026-08-31:STAGE:90'
    }],
    Permits: [{
      _row: 2, permit_id: 'P1', company_id: 'C0001',
      permit_authority_name: '愛知県知事', contractor_number: '12345',
      permit_number_full: '愛知県知事許可 第12345号',
      permit_category: '一般', expiry_date: '2026-08-31',
      current_status: 'VALID', permit_data_version: 1
    }],
    MLITPermits: [{
      _row: 2, permit_id: 'P1', company_id: 'C0001',
      authority: '愛知県知事', permit_number: '12345',
      observed_expiry_date: '2026-08-31', expiry_date: '2026-08-31',
      fetch_status: 'OK', diff_status: 'NONE',
      last_attempted_at: new Date().toISOString(),
      last_success_at: new Date().toISOString()
    }],
    Notifications: []
  };
  let uuid = 0;
  const properties = {
    GOOGLE_CLIENT_ID: 'client.apps.googleusercontent.com',
    NOTIFICATION_MODE: 'OFF',
    PILOT_COMPANY_IDS: '',
    INTERNAL_TEST_RECIPIENTS: 'internal@example.com'
  };
  const context = {
    Date, Error, JSON, Math, Number, Object, RegExp, String,
    Array, Boolean, console,
    SHEETS: {
      Companies: 'Companies', UserAccess: 'UserAccess', AuditLog: 'AuditLog',
      NotificationQueue: 'NotificationQueue', Permits: 'Permits',
      MLITPermits: 'MLITPermits', Notifications: 'Notifications'
    },
    SECURE_COMPANIES_HEADERS_: [],
    SECURE_NOTIFICATIONS_HEADERS_: [],
    NOTIFICATION_QUEUE_HEADERS: [],
    COMPANY_CONTACT_FIELDS_: undefined,
    ROLE_CAPABILITIES_: undefined,
    getConfig_: () => '',
    PropertiesService: {
      getScriptProperties: () => ({
        getProperty: key => Object.prototype.hasOwnProperty.call(properties, key) ? properties[key] : null,
        setProperty: (key, value) => { properties[key] = String(value); },
        deleteProperty: key => { delete properties[key]; }
      })
    },
    CacheService: { getScriptCache: () => ({ get: () => null, put: () => {} }) },
    UrlFetchApp: {
      fetch: () => ({
        getResponseCode: () => 200,
        getContentText: () => JSON.stringify({
          sub: '123', email: 'm-fujita@tokai-ic.co.jp', email_verified: 'true',
          aud: properties.GOOGLE_CLIENT_ID, iss: 'https://accounts.google.com',
          exp: Math.floor(Date.now() / 1000) + 3600, name: '藤田'
        })
      })
    },
    Utilities: {
      DigestAlgorithm: { SHA_256: 'SHA_256' },
      Charset: { UTF_8: 'UTF_8' },
      computeDigest: () => Array(32).fill(1),
      formatDate: () => '2026-07-30 12:00:00',
      getUuid: () => `UUID-${++uuid}`
    },
    SpreadsheetApp: { flush: () => {} },
    LockService: {
      getScriptLock: () => ({ tryLock: () => true, releaseLock: () => {} })
    },
    getSheet_: name => ({ name }),
    ensureHeaders_: () => ({ added: [], headers: [] }),
    readRecords_: name => (rows[name] || []).map(item => ({ ...item })),
    findByKey_: (name, key, value) => {
      const found = (rows[name] || []).find(item => String(item[key]) === String(value));
      return found ? { ...found } : null;
    },
    appendRecord_: (name, record) => {
      const row = { ...record, _row: (rows[name] || []).length + 2 };
      rows[name].push(row);
      return { ...row };
    },
    updateRecord_: (name, rowNumber, updates) => {
      const row = (rows[name] || []).find(item => item._row === rowNumber);
      if (!row) return false;
      Object.assign(row, updates);
      return true;
    },
    getCompanyDetail_: companyId => ({
      company: rows.Companies.find(row => row.company_id === companyId),
      permits: [], notifications: [], auditLog: []
    }),
    getNowString_: () => '2026-07-30 12:00:00',
    generateUuid_: () => `UUID-${++uuid}`,
    normalizeEmailRecipients_: value => String(value || '').split(',').map(v => v.trim()).filter(Boolean),
    normalizePermitNumberForMlit_: value => String(value || '').replace(/^0+/, '') || '0',
    parseNotifyStages_: () => [90, 60, 30, 0],
    getSecureSetting_: key => properties[key] || '',
    setSecureSetting_: (key, value) => { properties[key] = String(value); },
    MailApp: { getRemainingDailyQuota: () => 100 },
    isSendEnabled_: () => false,
    getCompanyReadiness_: undefined
  };
  context.__rows = rows;
  context.__properties = properties;
  vm.createContext(context);
  vm.runInContext(source('Security.gs'), context, { filename: 'Security.gs' });
  vm.runInContext(source('api.gs'), context, { filename: 'api.gs' });
  vm.runInContext(source('MasterService.gs'), context, { filename: 'MasterService.gs' });
  vm.runInContext(source('PermitSyncService.gs'), context, { filename: 'PermitSyncService.gs' });
  vm.runInContext(source('NotificationQueue.gs'), context, { filename: 'NotificationQueue.gs' });
  context.appendAuditEvent_ = event => {
    const record = {
      ...event,
      log_id: event.log_id || `A-${rows.AuditLog.length + 1}`,
      _row: rows.AuditLog.length + 2
    };
    rows.AuditLog.push(record);
    return { ...record };
  };
  return context;
}

test('公開トップレベル関数はdoGetとapiDispatchだけ', () => {
  const functions = [];
  for (const file of fs.readdirSync(SRC).filter(name => name.endsWith('.gs'))) {
    const text = source(file);
    const pattern = /^function\s+([A-Za-z0-9_$]+)\s*\(/gm;
    let match;
    while ((match = pattern.exec(text))) {
      if (!match[1].endsWith('_')) functions.push(`${file}:${match[1]}`);
    }
  }
  assert.deepEqual(functions.sort(), ['Code2.gs:doGet', 'api.gs:apiDispatch']);
});

test('全Apps ScriptとHTML内JavaScriptが構文解析できる', () => {
  for (const file of fs.readdirSync(SRC).filter(name => name.endsWith('.gs'))) {
    assert.doesNotThrow(
      () => new vm.Script(source(file), { filename: file }),
      `${file} must parse`
    );
  }
  const html = source('index.html')
    .replace(/<\?!=[\s\S]*?\?>/g, '"TEMPLATE_VALUE"')
    .replace(/<\?[\s\S]*?\?>/g, '');
  const inlineScripts = [...html.matchAll(
    /<script(?![^>]*\bsrc\s*=)[^>]*>([\s\S]*?)<\/script>/gi
  )].map(match => match[1]);
  assert.ok(inlineScripts.length > 0);
  inlineScripts.forEach((script, index) => {
    assert.doesNotThrow(
      () => new vm.Script(script, { filename: `index-inline-${index}.js` })
    );
  });
});

test('旧クライアント申告認証と固定キーを残さない', () => {
  const combined = ['api.gs', 'auth.gs', 'Code2.gs', 'index.html']
    .map(source).join('\n');
  assert.doesNotMatch(combined, /clientUserKey|manualLogin|tscg2026|Session\.getEffectiveUser/);
  assert.doesNotMatch(source('index.html'), /localStorage|sessionStorage/);
});

test('検証済みtokenとUserAccessからのみ利用者を構築する', () => {
  const context = baseContext();
  const user = context.buildAuthenticatedUser_('a'.repeat(40) + '.' + 'b'.repeat(40) + '.' + 'c'.repeat(40));
  assert.equal(user.email, 'm-fujita@tokai-ic.co.jp');
  assert.equal(user.role, 'master_editor');
  assert.equal(user.canSendExternal, true);
  assert.equal(user.capabilities.includes('companies.edit_contacts'), true);
  assert.equal(user.capabilities.includes('users.write'), false);
});

test('別client ID・期限切れtokenは拒否する', () => {
  const context = baseContext();
  context.UrlFetchApp.fetch = () => ({
    getResponseCode: () => 200,
    getContentText: () => JSON.stringify({
      sub: '1', email: 'm-fujita@tokai-ic.co.jp', email_verified: true,
      aud: 'attacker-client', iss: 'accounts.google.com',
      exp: Math.floor(Date.now() / 1000) - 1
    })
  });
  assert.throws(
    () => context.verifyGoogleIdToken_('a'.repeat(40) + '.' + 'b'.repeat(40) + '.' + 'c'.repeat(40)),
    error => error.code === 'UNAUTHORIZED'
  );
});

test('UserAccess未登録・inactiveは認証済みGoogle利用者でも拒否する', () => {
  const unregistered = baseContext();
  unregistered.__rows.UserAccess.length = 0;
  assert.throws(
    () => unregistered.buildAuthenticatedUser_(
      'a'.repeat(40) + '.' + 'b'.repeat(40) + '.' + 'c'.repeat(40)
    ),
    error => error.code === 'FORBIDDEN'
  );
  const inactive = baseContext();
  inactive.__rows.UserAccess[0].active = false;
  assert.throws(
    () => inactive.buildAuthenticatedUser_(
      'a'.repeat(40) + '.' + 'b'.repeat(40) + '.' + 'c'.repeat(40)
    ),
    error => error.code === 'FORBIDDEN'
  );
});

test('未定義action・余分なrequest項目を拒否する', () => {
  const context = baseContext();
  assert.throws(
    () => context.normalizeApiRequest_({
      version: '1', requestId: 'request_12345678',
      action: 'admin.become', payload: {}
    }),
    error => error.code === 'UNKNOWN_ACTION'
  );
  assert.throws(
    () => context.normalizeApiRequest_({
      version: '1', requestId: 'request_12345678',
      action: 'session.get', payload: {}, role: 'operations_admin'
    }),
    error => error.code === 'INVALID_PAYLOAD'
  );
});

test('会社名などallowlist外の更新項目を拒否する', () => {
  const context = baseContext();
  assert.throws(
    () => context.normalizeCompanyContactChanges_({ company_name_raw: '攻撃' }),
    error => error.code === 'INVALID_PAYLOAD'
  );
});

test('CCは検証・重複排除し最大5件', () => {
  const context = baseContext();
  assert.equal(
    context.normalizeCcEmails_('A@example.com,a@example.com,b@example.com'),
    'a@example.com,b@example.com'
  );
  assert.throws(
    () => context.normalizeCcEmails_('a@x.jp,b@x.jp,c@x.jp,d@x.jp,e@x.jp,f@x.jp'),
    error => error.code === 'INVALID_EMAIL'
  );
});

test('連絡先更新はversionを進め、監査をCOMMITTEDにし、古い通知候補をSTALEにする', () => {
  const context = baseContext();
  const user = {
    email: 'm-fujita@tokai-ic.co.jp', role: 'master_editor',
    capabilities: ['companies.edit_contacts'], canSendExternal: true
  };
  const result = context.updateCompanyContactsSecure_({
    companyId: 'C0001',
    dataVersion: 1,
    changes: { contact_person: '新担当', contact_email: 'new@example.com' },
    reasonCode: 'CUSTOMER_REQUEST',
    reasonNote: '',
    verifyContact: true
  }, user, 'request_12345678');
  assert.equal(result.company.data_version, 2);
  assert.equal(result.company.contact_person, '新担当');
  assert.equal(result.company.contact_email, 'new@example.com');
  assert.equal(result.company.contact_verified_by, user.email);
  assert.equal(context.__rows.AuditLog[0].status, 'COMMITTED');
  assert.equal(context.__rows.NotificationQueue[0].status, 'STALE');
});

test('古いdataVersionは上書きしない', () => {
  const context = baseContext();
  assert.throws(
    () => context.updateCompanyContactsSecure_({
      companyId: 'C0001', dataVersion: 9,
      changes: { phone: '000' }, reasonCode: 'CORRECTION',
      reasonNote: '', verifyContact: true
    }, { email: 'm-fujita@tokai-ic.co.jp', role: 'master_editor' }, 'request_87654321'),
    error => error.code === 'STALE_VERSION'
  );
  assert.equal(context.__rows.Companies[0].phone, '');
});

test('監査PREPAREDを書けない場合は会社を更新しない', () => {
  const context = baseContext();
  context.appendAuditEvent_ = () => { throw new Error('audit unavailable'); };
  assert.throws(() => context.updateCompanyContactsSecure_({
    companyId: 'C0001', dataVersion: 1,
    changes: { phone: '123' }, reasonCode: 'CORRECTION',
    reasonNote: '', verifyContact: true
  }, { email: 'm-fujita@tokai-ic.co.jp', role: 'master_editor' }, 'request_audit123'));
  assert.equal(context.__rows.Companies[0].phone, '');
  assert.equal(context.__rows.Companies[0].data_version, 1);
});

test('ScriptLockを取得できない場合は会社を変更しない', () => {
  const context = baseContext();
  context.LockService.getScriptLock = () => ({
    tryLock: () => false,
    releaseLock: () => {}
  });
  assert.throws(
    () => context.updateCompanyContactsSecure_({
      companyId: 'C0001', dataVersion: 1,
      changes: { phone: '123' }, reasonCode: 'CORRECTION',
      reasonNote: '', verifyContact: true
    }, { email: 'm-fujita@tokai-ic.co.jp', role: 'master_editor' }, 'request_lock1234'),
    error => error.code === 'LOCK_TIMEOUT'
  );
  assert.equal(context.__rows.Companies[0].phone, '');
  assert.equal(context.__rows.Companies[0].data_version, 1);
});

test('contact_email空欄・不正形式は送信準備未完了にする', () => {
  const context = baseContext();
  assert.equal(context.getCompanyReadiness_({
    contact_email: '', contact_verified_at: '', contact_verified_by: '', status: 'ACTIVE'
  }).code, 'EMAIL_MISSING');
  assert.equal(context.getCompanyReadiness_({
    contact_email: 'bad-email', contact_verified_at: 'x',
    contact_verified_by: 'y@example.com', status: 'ACTIVE'
  }).code, 'EMAIL_INVALID');
});

test('同じrequestIdの再実行は更新を重ねない', () => {
  const context = baseContext();
  const user = { email: 'm-fujita@tokai-ic.co.jp', role: 'master_editor' };
  const payload = {
    companyId: 'C0001', dataVersion: 1,
    changes: { phone: '123' }, reasonCode: 'CORRECTION',
    reasonNote: '', verifyContact: true
  };
  const first = context.updateCompanyContactsSecure_(payload, user, 'request_same123');
  const second = context.updateCompanyContactsSecure_(payload, user, 'request_same123');
  assert.equal(first.company.data_version, 2);
  assert.equal(second.company.data_version, 2);
  assert.equal(second.idempotentReplay, true);
});

test('OFFでは手動送信policyを拒否する', () => {
  const context = baseContext();
  context.__rows.NotificationQueue[0].status = 'APPROVED';
  assert.throws(
    () => context.validateQueuedSendPolicy_({
      queueId: 'Q1', origin: 'MANUAL', actorEmail: 'm-fujita@tokai-ic.co.jp'
    }),
    error => error.code === 'MODE_BLOCKED'
  );
});

test('MANUAL_PILOTではpilot外会社を拒否する', () => {
  const context = baseContext();
  context.__properties.NOTIFICATION_MODE = 'MANUAL_PILOT';
  context.__properties.PILOT_COMPANY_IDS = 'C9999';
  context.__rows.NotificationQueue[0].status = 'APPROVED';
  assert.throws(
    () => context.validateQueuedSendPolicy_({
      queueId: 'Q1', origin: 'MANUAL', actorEmail: 'm-fujita@tokai-ic.co.jp'
    }),
    error => error.code === 'PILOT_ONLY'
  );
});

test('マスタ更新後のSTALE候補と手動modeのAUTO originを拒否する', () => {
  const stale = baseContext();
  stale.__properties.NOTIFICATION_MODE = 'MANUAL_ALL';
  stale.__rows.NotificationQueue[0].status = 'APPROVED';
  stale.__rows.Companies[0].data_version = 2;
  assert.throws(
    () => stale.validateQueuedSendPolicy_({
      queueId: 'Q1', origin: 'MANUAL', actorEmail: 'm-fujita@tokai-ic.co.jp'
    }),
    error => error.code === 'STALE_QUEUE'
  );

  const autoBlocked = baseContext();
  autoBlocked.__properties.NOTIFICATION_MODE = 'MANUAL_ALL';
  autoBlocked.__rows.NotificationQueue[0].status = 'READY';
  assert.throws(
    () => autoBlocked.validateQueuedSendPolicy_({
      queueId: 'Q1', origin: 'AUTO', actorEmail: 'SYSTEM'
    }),
    error => error.code === 'MODE_BLOCKED'
  );
});

test('許可versionまたは期限が候補生成後に変わった場合は送信を拒否する', () => {
  const versionChanged = baseContext();
  versionChanged.__properties.NOTIFICATION_MODE = 'MANUAL_ALL';
  versionChanged.__rows.NotificationQueue[0].status = 'APPROVED';
  versionChanged.__rows.Permits[0].permit_data_version = 2;
  assert.throws(
    () => versionChanged.validateQueuedSendPolicy_({
      queueId: 'Q1', origin: 'MANUAL', actorEmail: 'm-fujita@tokai-ic.co.jp'
    }),
    error => error.code === 'STALE_QUEUE'
  );

  const expiryChanged = baseContext();
  expiryChanged.__properties.NOTIFICATION_MODE = 'MANUAL_ALL';
  expiryChanged.__rows.NotificationQueue[0].status = 'APPROVED';
  expiryChanged.__rows.Permits[0].expiry_date = '2031-08-31';
  assert.throws(
    () => expiryChanged.validateQueuedSendPolicy_({
      queueId: 'Q1', origin: 'MANUAL', actorEmail: 'm-fujita@tokai-ic.co.jp'
    }),
    error => error.code === 'STALE_QUEUE'
  );
});

test('通知冪等キーは許可ID・期限・stageをすべて含む', () => {
  const context = baseContext();
  const currentCycle = context.buildNotificationIdempotencyKey_(
    'P1', '2026-08-31', '90'
  );
  const renewedCycle = context.buildNotificationIdempotencyKey_(
    'P1', '2031-08-31', '90'
  );
  assert.equal(currentCycle, 'PERMIT:P1:EXPIRY:2026-08-31:STAGE:90');
  assert.notEqual(currentCycle, renewedCycle);
});

test('期限切れ許可もEXPIRED通知候補を生成できる', () => {
  const context = baseContext();
  context.__rows.Permits[0].current_status = 'EXPIRED';
  context.PermitsModel = {
    getAllActive: () => [{ ...context.__rows.Permits[0] }]
  };
  context.daysUntil_ = () => -1;
  context.Mailer = {
    buildExpiryNotification: () => ({
      to_email: 'old@example.com',
      cc_email: '',
      bcc_email: '',
      subject: '期限切れ',
      body: '本文'
    })
  };
  const result = context.generateNotificationCandidates_('SYSTEM_TEST');
  assert.equal(result.created, 1);
  const created = context.__rows.NotificationQueue.find(
    row => row.stage === 'EXPIRED'
  );
  assert.ok(created);
  assert.match(
    created.idempotency_key,
    /:EXPIRY:2026-08-31:STAGE:EXPIRED$/
  );
});

test('送信済みの最緊急stageから過去の緩いstageへ逆戻りしない', () => {
  const context = baseContext();
  context.__rows.Notifications.push({
    _row: 2,
    permit_id: 'P1',
    stage: '30',
    result: 'SENT',
    idempotency_key: 'PERMIT:P1:EXPIRY:2026-08-31:STAGE:30'
  });
  assert.equal(
    context.determineCandidateStage_(
      20, [90, 60, 30, 0], 'P1', '2026-08-31'
    ),
    null
  );
  context.__rows.Notifications.push({
    _row: 3,
    permit_id: 'P1',
    stage: 'EXPIRED',
    result: 'SENT',
    idempotency_key: 'PERMIT:P1:EXPIRY:2026-08-31:STAGE:EXPIRED'
  });
  assert.equal(
    context.determineCandidateStage_(
      -1, [90, 60, 30, 0], 'P1', '2026-08-31'
    ),
    null
  );
});

test('INTERNAL_TESTは実宛先を内部許可宛先へ置換する', () => {
  const context = baseContext();
  context.__properties.NOTIFICATION_MODE = 'INTERNAL_TEST';
  context.__rows.NotificationQueue[0].status = 'APPROVED';
  const prepared = context.validateQueuedSendPolicy_({
    queueId: 'Q1', origin: 'MANUAL', actorEmail: 'm-fujita@tokai-ic.co.jp'
  });
  assert.equal(prepared.to, 'internal@example.com');
  assert.match(prepared.subject, /内部テスト/);
  assert.notEqual(prepared.to, 'old@example.com');
});

test('マスタ移行は127行・業者番号一意・SYSTEM_ONLY承認を必須にする', () => {
  const context = baseContext();
  vm.runInContext(source('MasterMigration.gs'), context, { filename: 'MasterMigration.gs' });
  const rows = Array.from({ length: 127 }, (_, index) => ({
    source_row: index + 2,
    vendor_no: index < 125 ? String(index + 1) : '',
    company_name_raw: `会社${index + 1}`,
    company_name_normalized: `会社${index + 1}`,
    matched_company_id: '',
    classification: 'NEW_FROM_EXCEL',
    match_method: 'NONE',
    review_status: 'APPROVED',
    reviewed_by: 'kanri.tic@tokai-ic.co.jp',
    reviewed_at: '2026-07-30 12:00:00',
    notes: '',
    source_sha256: 'a'.repeat(64),
    imported_at: '2026-07-30 12:00:00'
  }));
  assert.equal(context.validateMasterStagingRows_(rows).vendorNoCount, 125);
  rows[126].vendor_no = '1';
  assert.throws(
    () => context.validateMasterStagingRows_(rows),
    error => error.code === 'DUPLICATE_VENDOR_NO'
  );
  assert.throws(
    () => context.assertSystemOnlyApproval_(['C0002', 'C0001']),
    error => error.code === 'SYSTEM_ONLY_APPROVAL_REQUIRED'
  );
  context.__properties.MASTER_SYSTEM_ONLY_APPROVED_IDS = 'C0001,C0002';
  assert.doesNotThrow(() => context.assertSystemOnlyApproval_(['C0002', 'C0001']));
});

test('式インジェクションとメールヘッダー注入を無害化する', () => {
  const context = baseContext();
  vm.runInContext(source('Models.gs'), context, { filename: 'Models.gs' });
  vm.runInContext(source('Utils.gs'), context, { filename: 'Utils.gs' });
  assert.equal(context.sanitizeForSheet_('=IMPORTXML("x")'), '\'=IMPORTXML("x")');
  assert.throws(
    () => context.validateOutboundEnvelope_(
      'safe@example.com\r\nBcc: attacker@example.com',
      '件名',
      {}
    )
  );
  assert.throws(
    () => context.validateOutboundEnvelope_(
      'safe@example.com',
      '正常\r\nBcc: attacker@example.com',
      {}
    )
  );
  assert.doesNotMatch(source('index.html'), /\binnerHTML\b|insertAdjacentHTML|document\.write/);
});

test('現行GASソースはGoogleフォーム受付に依存しない', () => {
  assert.equal(fs.existsSync(path.join(SRC, 'FormHandler.gs')), false);
  const currentSource = fs.readdirSync(SRC)
    .filter(name => /\.(gs|html)$/.test(name))
    .map(name => source(name))
    .join('\n');
  assert.doesNotMatch(
    currentSource,
    /FORM_ID|onFormSubmit|sendReceiptConfirmation|Googleフォーム|setupDailyTrigger_|initSheetHeaders_/
  );
});

let failed = 0;
for (const item of tests) {
  try {
    item.fn();
    console.log(`PASS ${item.name}`);
  } catch (error) {
    failed += 1;
    console.error(`FAIL ${item.name}`);
    console.error(error && error.stack ? error.stack : error);
  }
}
if (failed) {
  console.error(`${failed} test(s) failed`);
  process.exit(1);
}
console.log(`${tests.length} test(s) passed`);
