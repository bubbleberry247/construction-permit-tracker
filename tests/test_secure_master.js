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
  const fakeIdToken =
    'a'.repeat(40) + '.' + 'b'.repeat(40) + '.' + 'c'.repeat(40);
  const cacheStore = new Map();
  const scriptCache = {
    get: key => cacheStore.has(key) ? cacheStore.get(key) : null,
    put: (key, value) => { cacheStore.set(key, String(value)); },
    remove: key => { cacheStore.delete(key); }
  };
  const properties = {
    GOOGLE_CLIENT_ID: 'client.apps.googleusercontent.com',
    GOOGLE_CLIENT_SECRET: 'test-client-secret-that-is-long-enough',
    GOOGLE_OAUTH_REDIRECT_URI:
      'https://script.google.com/macros/s/TEST_DEPLOYMENT_IDENTIFIER_1234567890/exec',
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
    CacheService: { getScriptCache: () => scriptCache },
    UrlFetchApp: {
      fetch: url => {
        if (String(url) === 'https://oauth2.googleapis.com/token') {
          return {
            getResponseCode: () => 200,
            getContentText: () => JSON.stringify({ id_token: fakeIdToken })
          };
        }
        return {
          getResponseCode: () => 200,
          getContentText: () => JSON.stringify({
            sub: '123', email: 'm-fujita@tokai-ic.co.jp', email_verified: 'true',
            aud: properties.GOOGLE_CLIENT_ID, iss: 'https://accounts.google.com',
            exp: Math.floor(Date.now() / 1000) + 3600, name: '藤田'
          })
        };
      }
    },
    Utilities: {
      DigestAlgorithm: { SHA_256: 'SHA_256' },
      Charset: { UTF_8: 'UTF_8' },
      computeDigest: () => Array(32).fill(1),
      base64EncodeWebSafe: bytes => Buffer.from(
        bytes.map(byte => byte < 0 ? byte + 256 : byte)
      ).toString('base64url'),
      formatDate: () => '2026-07-30 12:00:00',
      getUuid: () => `00000000-0000-4000-8000-${String(++uuid).padStart(12, '0')}`
    },
    HtmlService: {
      createHtmlOutput: content => ({
        content,
        setTitle() { return this; },
        addMetaTag() { return this; }
      })
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
    updateAuditEvent_: (logId, updates) => {
      const row = rows.AuditLog.find(item => item.log_id === logId);
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
    toSerializable_: value => value,
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
  context.__cacheStore = cacheStore;
  context.__fakeIdToken = fakeIdToken;
  vm.createContext(context);
  vm.runInContext(source('Security.gs'), context, { filename: 'Security.gs' });
  vm.runInContext(source('OAuthLogin.gs'), context, { filename: 'OAuthLogin.gs' });
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

test('認証期限切れは一覧エラーにせず共通ログイン画面へ戻す', () => {
  const html = source('index.html');
  const inlineScript = html.match(
    /<script(?![^>]*\bsrc\s*=)[^>]*>([\s\S]*?)<\/script>/i
  )[1];
  const authClient = inlineScript.slice(
    inlineScript.indexOf("'use strict';"),
    inlineScript.indexOf('function invokeApi')
  );
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {
        id,
        textContent: '',
        disabled: false,
        dataset: {},
        style: {},
        classList: {
          add: name => classes.add(name),
          remove: name => classes.delete(name),
          toggle: (name, force) => force ? classes.add(name) : classes.delete(name),
          contains: name => classes.has(name)
        }
      });
    }
    return elements.get(id);
  }
  const context = vm.createContext({
    Date, Error, JSON, Math, Number, Object, Promise, String,
    Array, Boolean, Map,
    crypto: { randomUUID: () => '00000000-0000-4000-8000-000000000001' },
    document: { getElementById: element, querySelectorAll: () => [] },
    window: { clearTimeout: () => {}, setTimeout: () => 1 }
  });
  vm.runInContext(authClient, context, { filename: 'index-auth-client.js' });
  context.stopAuthPolling = () => { context.AUTH_POLL_TIMER = null; };

  context.AUTH_TOKEN = 'expired-token';
  context.AUTH_FLOW_STATE = 'state';
  context.CURRENT_USER = {email: 'user@example.com'};
  element('app-view').classList.remove('hidden');
  element('login-view').classList.add('hidden');

  assert.equal(context.handleAuthenticationError_({code: 'UNAUTHORIZED'}), true);
  assert.equal(context.AUTH_TOKEN, '');
  assert.equal(context.AUTH_FLOW_STATE, '');
  assert.equal(context.CURRENT_USER, null);
  assert.equal(element('app-view').classList.contains('hidden'), true);
  assert.equal(element('login-view').classList.contains('hidden'), false);
  assert.match(element('login-status').textContent, /有効期限.*再ログイン/);
  assert.equal(element('login-status').classList.contains('error'), true);

  element('toast').classList.add('hidden');
  assert.equal(context.showApiError_({code: 'UNAUTHORIZED', message: 'expired'}), false);
  assert.equal(element('toast').classList.contains('hidden'), true);

  context.AUTH_TOKEN = 'valid-token';
  assert.equal(context.handleAuthenticationError_({code: 'VALIDATION_ERROR'}), false);
  assert.equal(context.AUTH_TOKEN, 'valid-token');
  assert.equal(context.showApiError_({code: 'VALIDATION_ERROR', message: '入力エラー'}), true);
  assert.equal(element('toast').textContent, '入力エラー');
  assert.equal(element('toast').classList.contains('hidden'), false);

  assert.match(
    inlineScript,
    /api\('companies\.list'[\s\S]*?\.catch\(function\(error\) \{\s*if \(isAuthenticationError_\(error\)\) return;/
  );
  assert.match(
    inlineScript,
    /api\('mlit\.listDiffs'[\s\S]*?\.catch\(function\(error\) \{\s*if \(isAuthenticationError_\(error\)\) return;/
  );
  assert.match(inlineScript, /showScreen\('companies'\);/);
});

test('旧クライアント申告認証と固定キーを残さない', () => {
  const combined = ['api.gs', 'auth.gs', 'OAuthLogin.gs', 'Code2.gs', 'index.html']
    .map(source).join('\n');
  assert.doesNotMatch(combined, /clientUserKey|manualLogin|tscg2026|Session\.getEffectiveUser/);
  assert.doesNotMatch(source('index.html'), /localStorage|sessionStorage/);
  assert.doesNotMatch(source('index.html'), /accounts\.google\.com\/gsi|google\.accounts\.id/);
});

test('OAuth開始は匿名で許可するがsecretとPKCE verifierをブラウザへ返さない', () => {
  const context = baseContext();
  const response = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_start_123',
    action: 'auth.start',
    payload: {}
  }, '');
  assert.equal(response.ok, true);
  assert.match(response.data.state, /^[A-Za-z0-9_-]{48,160}$/);
  assert.match(response.data.authorizationUrl, /^https:\/\/accounts\.google\.com\/o\/oauth2\/v2\/auth\?/);
  assert.match(response.data.authorizationUrl, /code_challenge_method=S256/);
  assert.match(response.data.authorizationUrl, /response_type=code/);
  assert.doesNotMatch(
    JSON.stringify(response.data),
    /test-client-secret|verifier/i
  );
});

test('OAuth callbackはUserAccess確認後にID tokenを一度だけ受け渡す', () => {
  const context = baseContext();
  const started = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_start_456',
    action: 'auth.start',
    payload: {}
  }, '');
  assert.equal(started.ok, true);

  const completed = context.completeOAuthCallback_({
    state: started.data.state,
    code: 'valid-authorization-code'
  });
  assert.equal(completed.ok, true);
  assert.equal(completed.email, 'm-fujita@tokai-ic.co.jp');

  const firstPoll = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_poll_456',
    action: 'auth.poll',
    payload: { state: started.data.state }
  }, '');
  assert.equal(firstPoll.ok, true);
  assert.equal(firstPoll.data.status, 'COMPLETED');
  assert.equal(firstPoll.data.credential, context.__fakeIdToken);

  const replayPoll = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_poll_457',
    action: 'auth.poll',
    payload: { state: started.data.state }
  }, '');
  assert.equal(replayPoll.ok, false);
  assert.equal(replayPoll.error.code, 'AUTH_FLOW_EXPIRED');
});

test('OAuth state再利用・キャンセル・余分なpayloadをfail-closedで拒否する', () => {
  const context = baseContext();
  const started = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_start_789',
    action: 'auth.start',
    payload: {}
  }, '');
  context.completeOAuthCallback_({
    state: started.data.state,
    code: 'valid-authorization-code'
  });
  assert.throws(
    () => context.completeOAuthCallback_({
      state: started.data.state,
      code: 'second-authorization-code'
    }),
    error => error.code === 'AUTH_FLOW_REUSED'
  );

  const cancelled = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_start_cancel',
    action: 'auth.start',
    payload: {}
  }, '');
  assert.throws(
    () => context.completeOAuthCallback_({
      state: cancelled.data.state,
      error: 'access_denied',
      error_description: '<script>alert(1)</script>'
    }),
    error => error.code === 'AUTH_CANCELLED'
  );
  const cancelledPoll = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_poll_cancel',
    action: 'auth.poll',
    payload: { state: cancelled.data.state }
  }, '');
  assert.equal(cancelledPoll.ok, false);
  assert.equal(cancelledPoll.error.code, 'AUTH_CANCELLED');
  assert.doesNotMatch(cancelledPoll.error.message, /script|alert/i);

  const extraPayload = context.apiDispatch({
    version: '1',
    requestId: 'request_auth_extra_123',
    action: 'auth.start',
    payload: { role: 'operations_admin' }
  }, '');
  assert.equal(extraPayload.ok, false);
  assert.equal(extraPayload.error.code, 'INVALID_PAYLOAD');
});

test('OAuth secret未設定とlock失敗は認証開始前にfail-closedで停止する', () => {
  const missingSecret = baseContext();
  delete missingSecret.__properties.GOOGLE_CLIENT_SECRET;
  const notConfigured = missingSecret.apiDispatch({
    version: '1',
    requestId: 'request_auth_missing_secret',
    action: 'auth.start',
    payload: {}
  }, '');
  assert.equal(notConfigured.ok, false);
  assert.equal(notConfigured.error.code, 'AUTH_NOT_CONFIGURED');

  const lockFailure = baseContext();
  lockFailure.LockService = {
    getScriptLock: () => ({ tryLock: () => false, releaseLock: () => {} })
  };
  const busy = lockFailure.apiDispatch({
    version: '1',
    requestId: 'request_auth_lock_failure',
    action: 'auth.start',
    payload: {}
  }, '');
  assert.equal(busy.ok, false);
  assert.equal(busy.error.code, 'AUTH_FLOW_BUSY');
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

test('期限切れの自動送信はAUTO_ALLだけに限定する', () => {
  const context = baseContext();
  assert.deepEqual(
    Array.from(context.getModePolicy_('AUTO_STANDARD_ALL').autoStages),
    ['90', '60', '30']
  );
  assert.deepEqual(
    Array.from(context.getModePolicy_('AUTO_ALL').autoStages),
    ['90', '60', '30', '0', 'EXPIRED']
  );
});

test('送信結果の要照合は運用管理者だけが証跡付きで確定できる', () => {
  const context = baseContext();
  assert.equal(
    context.ROLE_CAPABILITIES_.operations_admin.includes('notifications.reconcile'),
    true
  );
  assert.equal(
    context.ROLE_CAPABILITIES_.master_editor.includes('notifications.reconcile'),
    false
  );
  context.__rows.NotificationQueue[0].status = 'PENDING_RECONCILIATION';
  context.__rows.NotificationQueue[0].notification_id = 'N1';
  context.__rows.Notifications.push({
    _row: 2,
    notification_id: 'N1',
    result: 'PENDING_RECONCILIATION'
  });
  context.NotificationsModel = {
    updateById: (id, updates) => {
      const notification = context.__rows.Notifications.find(
        row => row.notification_id === id
      );
      if (!notification) return false;
      Object.assign(notification, updates);
      return true;
    }
  };
  const result = context.reconcileNotificationCandidate_({
    queueId: 'Q1',
    outcome: 'CONFIRMED_SENT',
    evidenceNote: 'Gmail送信済みでMessage-IDを確認'
  }, {
    email: 'kanri.tic@tokai-ic.co.jp',
    role: 'operations_admin'
  }, 'request_reconcile1');
  assert.equal(result.status, 'SENT');
  assert.equal(context.__rows.NotificationQueue[0].status, 'SENT');
  assert.equal(context.__rows.Notifications[0].result, 'SENT');
  assert.equal(context.__rows.AuditLog[0].status, 'COMMITTED');
});

test('監査更新はappend戻り値の行番号に依存せずlog_idで再取得する', () => {
  const migrationAndQueue = [
    source('PermitMigration.gs'),
    source('NotificationQueue.gs')
  ].join('\n');
  assert.doesNotMatch(migrationAndQueue, /(?:audit|prepared)\._row/);
  assert.match(source('db.gs'), /function updateAuditEvent_\(/);
  assert.match(
    source('db.gs'),
    /findByKey_\(SHEETS\.AuditLog,\s*'log_id',\s*logId\)/
  );
});

test('低件数期間のmode昇格は10営業日・1件以上・未解決0件と理由を必須にする', () => {
  const context = baseContext();
  context.__properties.NOTIFICATION_MODE = 'INTERNAL_TEST';
  context.__properties.NOTIFICATION_MODE_CHANGED_AT = '2026-07-01T00:00:00+09:00';
  context.__rows.NotificationQueue.length = 0;
  context.__rows.Notifications.push({
    _row: 2,
    notification_id: 'N1',
    result: 'SENT',
    sent_at: '2026-07-15T00:00:00+09:00'
  });
  const user = {
    email: 'kanri.tic@tokai-ic.co.jp',
    role: 'operations_admin'
  };
  assert.throws(
    () => context.setNotificationModeSecure_({
      mode: 'MANUAL_PILOT',
      reason: 'pilotへ移行',
      confirmNoIncidents: true,
      confirmReconciled: true
    }, user, 'request_lowvolume0'),
    error => error.code === 'PROMOTION_GATE_NOT_MET'
  );
  const result = context.setNotificationModeSecure_({
    mode: 'MANUAL_PILOT',
    reason: 'pilotへ移行',
    confirmNoIncidents: true,
    confirmReconciled: true,
    confirmLowVolumeWaiver: true,
    lowVolumeWaiverReason: '期間中の期限通知対象が1件だけだったため'
  }, user, 'request_lowvolume1');
  assert.equal(result.mode, 'MANUAL_PILOT');
  assert.equal(result.lowVolumeWaiverUsed, true);
  assert.match(context.__rows.AuditLog[0].details, /期限通知対象が1件だけ/);
});

test('許可移行stagingは承認済み必須項目・重複・未判断を検証する', () => {
  const context = baseContext();
  context.PermitsModel = { findByUpsertKey: () => null };
  vm.runInContext(source('PermitMigration.gs'), context, {
    filename: 'PermitMigration.gs'
  });
  const approved = {
    migration_row_id: 'PM-0001-01',
    company_id: 'C0001',
    permit_authority_name: '岐阜県知事',
    permit_authority_name_normalized: '岐阜県知事',
    contractor_number: '99999',
    permit_category: '一般',
    expiry_date: '2030-07-30',
    review_status: 'APPROVED',
    source_sha256: 'a'.repeat(64)
  };
  assert.equal(
    context.validatePermitImportStaging_([approved], true).approved,
    1
  );
  assert.throws(
    () => context.validatePermitImportStaging_([
      approved,
      {
        ...approved,
        migration_row_id: 'PM-0002-01',
        source_sha256: 'b'.repeat(64)
      }
    ], true),
    error => error.code === 'PERMIT_STAGING_DUPLICATE'
  );
  assert.throws(
    () => context.validatePermitImportStaging_([{
      ...approved,
      review_status: 'PENDING'
    }], true),
    error => error.code === 'PERMIT_REVIEW_INCOMPLETE'
  );
  assert.throws(
    () => context.validatePermitImportStaging_([{
      ...approved,
      expiry_date: '',
      source_sha256: 'c'.repeat(64)
    }], true),
    error => error.code === 'PERMIT_STAGING_FIELDS_REQUIRED'
  );
});

test('監視対象移行は全社判断を必須にしINACTIVE会社を監視しない', () => {
  const context = baseContext();
  context.PermitsModel = { findByUpsertKey: () => null };
  vm.runInContext(source('PermitMigration.gs'), context, {
    filename: 'PermitMigration.gs'
  });
  const approved = {
    company_id: 'C0001',
    proposed_action: 'MONITOR',
    review_status: 'APPROVED'
  };
  assert.equal(
    context.validateMonitoringTargetStaging_([approved], true).monitor,
    1
  );
  assert.throws(
    () => context.validateMonitoringTargetStaging_([{
      ...approved,
      review_status: 'PENDING'
    }], true),
    error => error.code === 'MONITORING_REVIEW_INCOMPLETE'
  );
  context.__rows.Companies[0].status = 'INACTIVE';
  assert.throws(
    () => context.validateMonitoringTargetStaging_([approved], true),
    error => error.code === 'MONITORING_INACTIVE_COMPANY'
  );
});

test('会社・許可version列は日付表示を残さず整数形式へ矯正する', () => {
  const context = baseContext();
  let appliedFormat = null;
  context.getSheet_ = () => ({
    getLastColumn: () => 3,
    getMaxRows: () => 1000,
    getRange: (row, column, rowCount, columnCount) => {
      if (row === 1) {
        return {
          getValues: () => [[
            'permit_id', 'company_id', 'permit_data_version'
          ]]
        };
      }
      return {
        setNumberFormat: format => {
          appliedFormat = { row, column, rowCount, columnCount, format };
        }
      };
    }
  });
  vm.runInContext(source('Schema.gs'), context, { filename: 'Schema.gs' });
  const result = context.ensureIntegerColumnFormat_(
    context.SHEETS.Permits,
    'permit_data_version'
  );
  assert.equal(result.column, 3);
  assert.deepEqual(appliedFormat, {
    row: 2,
    column: 3,
    rowCount: 999,
    columnCount: 1,
    format: '0'
  });
});

test('監視対象移行は監査COMMITTED失敗時に会社versionと監視状態を復元する', () => {
  const context = baseContext();
  context.SHEETS.MonitoringTargetStaging = 'MonitoringTargetStaging';
  context.__rows.MonitoringTargetStaging = [{
    _row: 2,
    company_id: 'C0001',
    proposed_action: 'DO_NOT_MONITOR',
    review_status: 'APPROVED'
  }];
  context.__properties.MONITORING_STAGING_LOADED_SHA256 = 'a'.repeat(64);
  context.__properties.MONITORING_STAGING_APPLY_CONFIRMATION =
    `APPLY_MONITORING_STAGING_${'a'.repeat(12)}`;
  context.assertRecentBackupForMigration_ = () => {};
  context.PermitsModel = { findByUpsertKey: () => null };
  vm.runInContext(source('PermitMigration.gs'), context, {
    filename: 'PermitMigration.gs'
  });
  const originalAuditUpdate = context.updateAuditEvent_;
  context.updateAuditEvent_ = (logId, updates) => {
    if (updates.status === 'COMMITTED') {
      return false;
    }
    return originalAuditUpdate(logId, updates);
  };
  assert.throws(
    () => context.applyMonitoringTargetMigration_(),
    error => error.code === 'AUDIT_COMMIT_FAILED'
  );
  assert.equal(
    context.__rows.Companies[0].permit_monitoring_enabled,
    true
  );
  assert.equal(context.__rows.Companies[0].data_version, 1);
  assert.equal(context.__rows.Companies[0].updated_by, 'm-fujita@tokai-ic.co.jp');
  assert.equal(context.__rows.AuditLog[0].status, 'ABORTED');
});

test('マスタ移行は127行・業者番号一意・SYSTEM_ONLYごとの維持除外判断を必須にする', () => {
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
    () => context.assertSystemOnlyDecisions_(['C0002', 'C0001'], {}),
    error => error.code === 'SYSTEM_ONLY_DECISION_REQUIRED'
  );
  context.__properties.MASTER_SYSTEM_ONLY_DECISIONS = JSON.stringify({
    C0001: 'KEEP_SYSTEM_ONLY',
    C0002: 'ARCHIVE_EXCLUDE'
  });
  const decisions = context.parseSystemOnlyDecisions_();
  assert.doesNotThrow(() => (
    context.assertSystemOnlyDecisions_(['C0002', 'C0001'], decisions)
  ));
  const canonicalMap = {
    C0001: { status: 'INACTIVE' },
    C0002: {
      status: 'ACTIVE',
      contact_verified_at: '2026-07-01 10:00:00',
      contact_verified_by: 'reviewer@example.com'
    }
  };
  const summary = context.applySystemOnlyDecisions_(
    canonicalMap,
    ['C0001', 'C0002'],
    decisions
  );
  assert.equal(canonicalMap.C0001.status, 'ACTIVE');
  assert.equal(canonicalMap.C0002.status, 'INACTIVE');
  assert.equal(canonicalMap.C0002.contact_verified_at, '');
  assert.equal(summary.KEEP_SYSTEM_ONLY, 1);
  assert.equal(summary.ARCHIVE_EXCLUDE, 1);
  assert.equal(summary.PENDING, 0);
  context.__properties.MASTER_SYSTEM_ONLY_DECISIONS = '{"C0001":"DELETE"}';
  assert.throws(
    () => context.parseSystemOnlyDecisions_(),
    error => error.code === 'SYSTEM_ONLY_DECISIONS_INVALID'
  );
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
