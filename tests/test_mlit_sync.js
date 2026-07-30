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

function formatJst(value, pattern) {
  const date = new Date(value);
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).formatToParts(date).reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, {});
  const day = `${parts.year}-${parts.month}-${parts.day}`;
  return pattern === 'yyyy-MM-dd'
    ? day
    : `${day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

function makeContext() {
  const rows = {
    Companies: [{
      _row: 2, company_id: 'C1', company_name_raw: '会社1',
      company_name_normalized: '会社1', status: 'ACTIVE',
      permit_monitoring_enabled: true, data_version: 1
    }, {
      _row: 3, company_id: 'C2', company_name_raw: '会社2',
      company_name_normalized: '会社2', status: 'ACTIVE',
      permit_monitoring_enabled: false, data_version: 1
    }],
    Permits: [{
      _row: 2, permit_id: 'P1', company_id: 'C1',
      company_name_raw: '会社1', permit_authority_name: '愛知県知事',
      permit_authority_name_normalized: '愛知県知事',
      contractor_number: '012345', permit_number_full: '愛知県知事許可 第012345号',
      permit_category: '一般', expiry_date: '2026-08-31',
      current_status: 'VALID', permit_data_version: 1
    }, {
      _row: 3, permit_id: 'P2', company_id: 'C2',
      company_name_raw: '会社2', permit_authority_name: '愛知県知事',
      permit_authority_name_normalized: '愛知県知事',
      contractor_number: '99999', permit_number_full: '愛知県知事許可 第99999号',
      permit_category: '一般', expiry_date: '2027-01-01',
      current_status: 'VALID', permit_data_version: 1
    }],
    MLITPermits: [],
    NotificationQueue: [],
    AuditLog: []
  };
  const properties = { MLIT_SYNC_MODE: 'SHADOW' };
  let uuid = 0;
  const context = {
    Date, Error, JSON, Math, Number, Object, RegExp, String,
    Array, Boolean, console,
    SHEETS: {
      Companies: 'Companies', Permits: 'Permits', MLITPermits: 'MLITPermits',
      NotificationQueue: 'NotificationQueue', AuditLog: 'AuditLog'
    },
    MLIT_PERMITS_HEADERS_: [],
    Utilities: {
      formatDate: (date, _tz, pattern) => formatJst(date, pattern)
    },
    PropertiesService: {
      getScriptProperties: () => ({
        getProperty: key => Object.prototype.hasOwnProperty.call(properties, key)
          ? properties[key]
          : null,
        setProperty: (key, value) => { properties[key] = String(value); },
        deleteProperty: key => { delete properties[key]; }
      })
    },
    LockService: {
      getScriptLock: () => ({ tryLock: () => true, releaseLock: () => {} })
    },
    SpreadsheetApp: { flush: () => {} },
    Logger: { log: () => {} },
    getSheet_: name => ({ name }),
    ensureHeaders_: () => ({ added: [], headers: [] }),
    readRecords_: name => (rows[name] || []).map(row => ({ ...row })),
    findByKey_: (name, key, value) => {
      const found = (rows[name] || []).find(row => String(row[key]) === String(value));
      return found ? { ...found } : null;
    },
    appendRecord_: (name, record) => {
      const stored = { ...record, _row: (rows[name] || []).length + 2 };
      rows[name].push(stored);
      return { ...stored };
    },
    updateRecord_: (name, rowNumber, updates) => {
      const stored = (rows[name] || []).find(row => row._row === rowNumber);
      if (!stored) return false;
      Object.assign(stored, updates);
      return true;
    },
    appendAuditEvent_: event => {
      const stored = {
        ...event, log_id: `A${rows.AuditLog.length + 1}`,
        _row: rows.AuditLog.length + 2
      };
      rows.AuditLog.push(stored);
      return { ...stored };
    },
    generateUuid_: () => `U${++uuid}`,
    getNowString_: () => formatJst(new Date(), 'yyyy-MM-dd HH:mm:ss'),
    getSecureSetting_: key => properties[key] || '',
    setSecureSetting_: (key, value) => { properties[key] = String(value); },
    parseStrictBoolean_: value => value === true || String(value).toLowerCase() === 'true',
    isCompanyPermitMonitoringEnabled_: company => !!company &&
      String(company.status || 'ACTIVE').toUpperCase() === 'ACTIVE' &&
      (company.permit_monitoring_enabled === true ||
        String(company.permit_monitoring_enabled).toLowerCase() === 'true'),
    normalizePermitNumberForMlit_: value => String(value || '').replace(/^0+/, '') || '0',
    parsePermitNumber_: value => {
      const match = String(value || '').match(/(\d{3,})/);
      return match
        ? { parse_success: true, contractor_number: match[1] }
        : { parse_success: false };
    },
    normalizeTextInput_: (value, max, label) => {
      const text = String(value || '').trim();
      if (text.length > max) throw new Error(`${label} too long`);
      return text;
    },
    assertOnlyKeys_: (object, allowedKeys, label) => {
      Object.keys(object || {}).forEach(key => {
        if (!allowedKeys.includes(key)) {
          const error = new Error(`${label}: unexpected field ${key}`);
          error.code = 'INVALID_PAYLOAD';
          throw error;
        }
      });
    },
    appError_: (code, message, retryable) => {
      const error = new Error(message);
      error.code = code; error.retryable = retryable === true;
      return error;
    },
    resolveDaysRemaining_: expiry => {
      const target = new Date(`${String(expiry).slice(0, 10)}T00:00:00+09:00`);
      return Math.floor((target.getTime() - Date.now()) / 86400000);
    },
    daysUntil_: expiry => {
      const target = new Date(`${String(expiry).slice(0, 10)}T00:00:00+09:00`);
      return Math.floor((target.getTime() - Date.now()) / 86400000);
    },
    parseNotifyStages_: () => [90, 60, 30, 0],
    getConfig_: () => '90,60,30,0',
    withMlitRateLimit_: fn => fn(),
    getLicenseNoKbn_: () => '01',
    getPrefCode_: () => '23',
    searchMlitPermit_: () => ['candidate'],
    fetchMlitDetail_: () => ({
      found: true, apiName: '会社1', expiryTo: '2031-08-31', expiryWareki: 'R13',
      tradesIppan: ['建築'], tradesTokutei: []
    }),
    logError_: () => {},
    markCompanyQueueItemsStale_: () => 0,
    serializeCompanyForClient_: company => ({ ...company })
  };
  context.__rows = rows;
  context.__properties = properties;
  vm.createContext(context);
  vm.runInContext(source('PermitSyncService.gs'), context, {
    filename: 'PermitSyncService.gs'
  });
  vm.runInContext(source('MlitRolling.gs'), context, {
    filename: 'MlitRolling.gs'
  });
  return context;
}

test('CompaniesとPermits起点で監視対象だけをMLIT観測へseedする', () => {
  const context = makeContext();
  const candidates = context.pickMlitRollingCandidates_(10, 7);
  assert.equal(context.__rows.MLITPermits.length, 1);
  assert.equal(context.__rows.MLITPermits[0].permit_id, 'P1');
  assert.equal(context.__rows.MLITPermits[0].permit_number, '12345');
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].permit_id, 'P1');
});

test('期限切れ許可も更新後期限の検出対象としてMLIT巡回に残す', () => {
  const context = makeContext();
  context.__rows.Permits[0].current_status = 'EXPIRED';
  const candidates = context.pickMlitRollingCandidates_(10, 7);
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].permit_id, 'P1');
});

test('MLIT mode OFFでは観測行のseedも外部確認も行わない', () => {
  const context = makeContext();
  context.__properties.MLIT_SYNC_MODE = 'OFF';
  const result = context.runDailyMlitRolling_();
  assert.equal(result.skipped, true);
  assert.equal(context.__rows.MLITPermits.length, 0);
});

test('手動再確認は監査PREPAREDを書けない場合に予約行を変更しない', () => {
  const context = makeContext();
  context.appendAuditEvent_ = () => {
    throw new Error('audit unavailable');
  };
  assert.throws(() => context.requestMlitRefreshSecure_({
    companyId: 'C1',
    permitId: '',
    reason: '担当者確認'
  }, {
    email: 'm-fujita@example.com',
    role: 'master_editor'
  }, 'request_refresh_1'));
  assert.equal(context.__rows.MLITPermits.length, 0);
});

test('MLIT modeは監査PREPAREDを書けない場合に変更しない', () => {
  const context = makeContext();
  context.appendAuditEvent_ = () => {
    throw new Error('audit unavailable');
  };
  assert.throws(() => context.setMlitSyncModeSecure_({
    mode: 'MANUAL_APPLY',
    reason: 'UAT合格'
  }, {
    email: 'kanri@example.com',
    role: 'operations_admin'
  }, 'request_mode_1'));
  assert.equal(context.__properties.MLIT_SYNC_MODE, 'SHADOW');
});

test('MLIT検索失敗は成功時刻とlegacy last_syncedを進めない', () => {
  const context = makeContext();
  context.pickMlitRollingCandidates_(10, 7);
  Object.assign(context.__rows.MLITPermits[0], {
    last_synced: '2026-07-01 03:00:00',
    last_success_at: '2026-07-01 03:00:00'
  });
  context.searchMlitPermit_ = () => {
    const error = new Error('network down'); error.code = 'NETWORK';
    throw error;
  };
  const result = context.refreshOneMlitPermit_({ ...context.__rows.MLITPermits[0] });
  const stored = context.__rows.MLITPermits[0];
  assert.equal(result.result, '確認不可');
  assert.equal(stored.last_success_at, '2026-07-01 03:00:00');
  assert.equal(stored.last_synced, '2026-07-01 03:00:00');
  assert.ok(stored.last_attempted_at);
  assert.equal(stored.consecutive_failure_count, 1);
  assert.equal(stored.fetch_status, 'TRANSIENT_ERROR');
});

test('MLIT成功で差分を記録してもPermits正本は自動上書きしない', () => {
  const context = makeContext();
  const candidates = context.pickMlitRollingCandidates_(10, 7);
  const result = context.refreshOneMlitPermit_(candidates[0]);
  assert.equal(result.result, '差分');
  assert.equal(context.__rows.Permits[0].expiry_date, '2026-08-31');
  assert.equal(context.__rows.MLITPermits[0].observed_expiry_date, '2031-08-31');
  assert.equal(context.__rows.MLITPermits[0].diff_status, 'PENDING_REVIEW');
  assert.ok(context.__rows.MLITPermits[0].last_success_at);
});

test('NOT_FOUNDは3回連続するまで確認待ちにしない', () => {
  const context = makeContext();
  context.searchMlitPermit_ = () => [];
  let observation = context.pickMlitRollingCandidates_(10, 7)[0];
  let first = context.refreshOneMlitPermit_(observation);
  assert.equal(first.result, '確認不可');
  assert.equal(context.__rows.MLITPermits[0].diff_status, 'NONE');
  context.__rows.MLITPermits[0].next_retry_at = '';
  observation = { ...context.__rows.MLITPermits[0] };
  context.refreshOneMlitPermit_(observation);
  context.__rows.MLITPermits[0].next_retry_at = '';
  observation = { ...context.__rows.MLITPermits[0] };
  const third = context.refreshOneMlitPermit_(observation);
  assert.equal(third.result, '差分');
  assert.equal(context.__rows.MLITPermits[0].consecutive_not_found_count, 3);
  assert.equal(context.__rows.MLITPermits[0].diff_status, 'PENDING_REVIEW');
  assert.equal(context.__rows.MLITPermits[0].diff_type, 'NOT_FOUND');
});

test('期限差分は一致・5年更新・短縮・5年幅逸脱を分類する', () => {
  const context = makeContext();
  assert.deepEqual(
    JSON.parse(JSON.stringify(context.classifyMlitExpiryDifference_(
      '2026-08-31', '2026-08-31'
    ))),
    { type: 'NONE', riskFlags: [] }
  );
  assert.deepEqual(
    JSON.parse(JSON.stringify(context.classifyMlitExpiryDifference_(
      '2026-08-31', '2031-08-31'
    ))),
    { type: 'EXTENDED', riskFlags: [] }
  );
  assert.deepEqual(
    JSON.parse(JSON.stringify(context.classifyMlitExpiryDifference_(
      '2026-08-31', '2025-08-31'
    ))),
    { type: 'SHORTENED', riskFlags: ['EXPIRY_SHORTENED'] }
  );
  assert.deepEqual(
    JSON.parse(JSON.stringify(context.classifyMlitExpiryDifference_(
      '2026-08-31', '2028-08-31'
    ))),
    { type: 'EXTENDED', riskFlags: ['EXTENSION_OUTSIDE_FIVE_YEAR_RANGE'] }
  );
});

test('MLIT候補が複数なら先頭を採用せず確認待ちにする', () => {
  const context = makeContext();
  context.searchMlitPermit_ = () => ['candidate1', 'candidate2'];
  const candidate = context.pickMlitRollingCandidates_(10, 7)[0];
  const result = context.refreshOneMlitPermit_(candidate);
  assert.equal(result.result, '差分');
  assert.equal(context.__rows.MLITPermits[0].diff_status, 'PENDING_REVIEW');
  assert.equal(context.__rows.MLITPermits[0].diff_type, 'AMBIGUOUS_MATCH');
  assert.equal(context.__rows.MLITPermits[0].last_success_at || '', '');
});

test('MLIT日付異常は正本と成功時刻を更新しない', () => {
  const context = makeContext();
  context.fetchMlitDetail_ = () => ({
    found: true, apiName: '会社1', expiryTo: '解析不能',
    tradesIppan: [], tradesTokutei: []
  });
  const candidate = context.pickMlitRollingCandidates_(10, 7)[0];
  const result = context.refreshOneMlitPermit_(candidate);
  assert.equal(result.result, '確認不可');
  assert.equal(context.__rows.Permits[0].expiry_date, '2026-08-31');
  assert.equal(context.__rows.MLITPermits[0].last_success_at || '', '');
  assert.equal(context.__rows.MLITPermits[0].fetch_status, 'TRANSIENT_ERROR');
});

test('MLIT商号が会社正本と異なる場合は期限を自動採用しない', () => {
  const context = makeContext();
  context.fetchMlitDetail_ = () => ({
    found: true, apiName: '別会社', expiryTo: '2031-08-31',
    tradesIppan: [], tradesTokutei: []
  });
  const candidate = context.pickMlitRollingCandidates_(10, 7)[0];
  const result = context.refreshOneMlitPermit_(candidate);
  assert.equal(result.result, '差分');
  assert.equal(context.__rows.Permits[0].expiry_date, '2026-08-31');
  assert.equal(context.__rows.MLITPermits[0].diff_type, 'IDENTITY_MISMATCH');
  assert.match(
    context.__rows.MLITPermits[0].diff_risk_flags,
    /COMPANY_NAME_MISMATCH/
  );
  context.__rows.MLITPermits[0].diff_status = 'DISMISSED';
  assert.equal(
    context.getMlitObservationState_(context.__rows.Permits[0]).sendFresh,
    false
  );
});

test('MANUAL_APPLYはpermit versionを進めて未送信通知をSTALEにする', () => {
  const context = makeContext();
  context.__properties.MLIT_SYNC_MODE = 'MANUAL_APPLY';
  const candidates = context.pickMlitRollingCandidates_(10, 7);
  context.refreshOneMlitPermit_(candidates[0]);
  context.__rows.NotificationQueue.push({
    _row: 2, queue_id: 'Q1', permit_id: 'P1', company_id: 'C1',
    source_permit_version: 1, status: 'READY'
  });
  const result = context.applyMlitExpiryDiffSecure_({
    permitId: 'P1', permitDataVersion: 1, reason: 'MLIT更新確認'
  }, {
    email: 'm-fujita@example.com', role: 'master_editor'
  }, 'request_apply_1');
  assert.equal(result.expiryDate, '2031-08-31');
  assert.equal(context.__rows.Permits[0].permit_data_version, 2);
  assert.equal(context.__rows.NotificationQueue[0].status, 'STALE');
  assert.equal(context.__rows.MLITPermits[0].diff_status, 'APPLIED');
});

let failed = 0;
for (const item of tests) {
  try {
    item.fn();
    console.log(`PASS ${item.name}`);
  } catch (error) {
    failed++;
    console.error(`FAIL ${item.name}`);
    console.error(error && error.stack ? error.stack : error);
  }
}
if (failed) {
  console.error(`${failed} test(s) failed`);
  process.exit(1);
}
console.log(`${tests.length} test(s) passed`);
