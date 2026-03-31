/**
 * logic.gs — ビジネスロジック（プライベート関数 _ サフィックス）
 */

// ---------------------------------------------------------------------------
// ステータス判定
// ---------------------------------------------------------------------------
function resolvePermitStatus_(daysLeft) {
  if (daysLeft === null || daysLeft === undefined || daysLeft === '') return 'unknown';
  var d = Number(daysLeft);
  if (isNaN(d)) return 'unknown';
  if (d < 0) return 'expired';
  if (d <= 30) return 'danger';
  if (d <= 90) return 'warn';
  return 'ok';
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
function buildDashboardData_() {
  var permits = readRecords_(SHEETS.MLITPermits);
  var companies = readRecords_(SHEETS.Companies);
  var syncRuns = readRecords_(SHEETS.SyncRuns);

  // Company lookup
  var companyMap = {};
  companies.forEach(function(c) {
    companyMap[c.company_id] = c;
  });

  // Build rows
  var rows = [];
  var counts = { expired: 0, danger: 0, warn: 0, ok: 0, unknown: 0, total: 0 };
  var andon = { scrapeFail: 0, needsReview: 0 };

  permits.forEach(function(p) {
    // ホワイトリスト方式: OKのみ表示、それ以外（NOT_FOUND, DUPLICATE_DELETE等）は非表示
    if (p.fetch_status !== 'OK') return;

    var daysRemaining = (p.days_remaining !== '' && p.days_remaining !== 0 && p.days_remaining !== '0')
      ? Number(p.days_remaining) : null;
    var status = resolvePermitStatus_(daysRemaining);

    // Company status check
    var company = companyMap[p.company_id] || {};
    if (company.status === 'INACTIVE') return; // skip inactive

    counts.total++;
    if (counts[status] !== undefined) counts[status]++;

    rows.push({
      company_id: p.company_id || '',
      company_name: p.company_name || '',
      permit_number: p.permit_number || '',
      authority: p.authority || '',
      category: p.category || '',
      expiry_date: p.expiry_date || '',
      days_remaining: daysRemaining,
      trades_count: p.trades_count || 0,
      trades_ippan: p.trades_ippan || '',
      trades_tokutei: p.trades_tokutei || '',
      fetch_status: p.fetch_status || '',
      status: status
    });
  });

  // Sort by days_remaining ascending (most urgent first)
  rows.sort(function(a, b) {
    var da = a.days_remaining !== null ? a.days_remaining : 99999;
    var db = b.days_remaining !== null ? b.days_remaining : 99999;
    return da - db;
  });

  // Sync info
  var lastSync = syncRuns.length > 0 ? syncRuns[syncRuns.length - 1] : null;

  return {
    rows: rows,
    counts: counts,
    andon: andon,
    lastSync: lastSync
  };
}

// ---------------------------------------------------------------------------
// Company Detail
// ---------------------------------------------------------------------------
function getCompanyDetail_(companyId) {
  // Company info
  var company = findByKey_(SHEETS.Companies, 'company_id', companyId);

  // Permits from MLITPermits
  var permits = findAllByKey_(SHEETS.MLITPermits, 'company_id', companyId);

  // Notification history
  var allNotifs = readRecords_(SHEETS.Notifications);
  var notifs = allNotifs.filter(function(n) {
    return n.company_id === companyId;
  }).sort(function(a, b) {
    var sa = String(a.sent_at || ''), sb = String(b.sent_at || '');
    return sa < sb ? 1 : sa > sb ? -1 : 0;
  });

  // Audit log for this company
  var allLogs = readRecords_(SHEETS.AuditLog);
  var logs = allLogs.filter(function(l) {
    return l.target_id === companyId;
  }).sort(function(a, b) {
    var ta = String(a.timestamp || ''), tb = String(b.timestamp || '');
    return ta < tb ? 1 : ta > tb ? -1 : 0;
  }).slice(0, 20);

  return {
    company: company || { company_id: companyId, status: 'UNKNOWN' },
    permits: permits,
    notifications: notifs,
    auditLog: logs
  };
}

// ---------------------------------------------------------------------------
// Company CRUD
// ---------------------------------------------------------------------------
function addCompany_(formData, userEmail) {
  var companyId = 'C' + generateUuid().replace(/-/g, '').substring(0, 8);
  var now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');

  var record = {
    company_id: companyId,
    company_name_raw: formData.company_name || '',
    company_name_normalized: (formData.company_name || '').replace(/\s+/g, ''),
    representative_name: '',
    contact_person: formData.contact_person || '',
    contact_email: formData.contact_email || '',
    contact_email_cc: '',
    phone: '',
    status: 'ACTIVE',
    created_at: now,
    updated_at: now
  };

  appendRecord_(SHEETS.Companies, record);
  writeAuditLog_(userEmail, 'ADD_COMPANY', 'Company', companyId, formData.company_name);

  return { success: true, company_id: companyId };
}

function updateCompany_(companyId, formData, userEmail) {
  var existing = findByKey_(SHEETS.Companies, 'company_id', companyId);
  if (!existing) throw new Error('会社が見つかりません: ' + companyId);

  var now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  var updates = { updated_at: now };

  if (formData.company_name !== undefined) {
    updates.company_name_raw = formData.company_name;
    updates.company_name_normalized = formData.company_name.replace(/\s+/g, '');
  }
  if (formData.contact_email !== undefined) updates.contact_email = formData.contact_email;
  if (formData.contact_person !== undefined) updates.contact_person = formData.contact_person;

  updateRecord_(SHEETS.Companies, existing._row, updates, formData._updatedAt);
  writeAuditLog_(userEmail, 'UPDATE_COMPANY', 'Company', companyId,
    JSON.stringify(updates).substring(0, 200));

  return { success: true };
}

function deactivateCompany_(companyId, userEmail) {
  var existing = findByKey_(SHEETS.Companies, 'company_id', companyId);
  if (!existing) throw new Error('会社が見つかりません: ' + companyId);

  var now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  updateRecord_(SHEETS.Companies, existing._row, { status: 'INACTIVE', updated_at: now });
  writeAuditLog_(userEmail, 'DEACTIVATE_COMPANY', 'Company', companyId, '');

  return { success: true };
}

function reactivateCompany_(companyId, userEmail) {
  var existing = findByKey_(SHEETS.Companies, 'company_id', companyId);
  if (!existing) throw new Error('会社が見つかりません: ' + companyId);

  var now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  updateRecord_(SHEETS.Companies, existing._row, { status: 'ACTIVE', updated_at: now });
  writeAuditLog_(userEmail, 'REACTIVATE_COMPANY', 'Company', companyId, '');

  return { success: true };
}

// ---------------------------------------------------------------------------
// User Access（ユーザー管理）
// ---------------------------------------------------------------------------

/**
 * UserAccess にユーザーが存在しなければ作成する（初回ログイン時）
 * @param {string} email
 * @param {string} [displayName]
 * @param {string} [role]
 * @return {{ email: string, role: string, active: boolean, displayName: string }}
 */
function ensureUser_(email, displayName, role) {
  email = String(email || '').trim().toLowerCase();
  if (!email) throw new Error('ensureUser_: email is required');

  var existing = getUserAccessByEmail_(email);
  if (existing) return existing;

  // 新規作成（デフォルト role = 'user'、active = false — 管理者が有効化するまで待機）
  var now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  appendRecord_(SHEETS.UserAccess, {
    email: email,
    role: role || 'user',
    active: 'false',
    displayName: displayName || email.split('@')[0],
    updatedAt: now
  });

  return {
    email: email,
    role: role || 'user',
    active: false,
    displayName: displayName || email.split('@')[0]
  };
}

// ---------------------------------------------------------------------------
// Hard Delete
// ---------------------------------------------------------------------------

/**
 * 会社を物理削除する（管理者のみ）
 * アーカイブ → 依存データ削除 → 本体削除
 * @param {string} companyId
 * @param {string} userEmail
 */
function hardDeleteCompany_(companyId, userEmail) {
  // LockService で排他制御（30秒タイムアウト）
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) {
    throw new Error('他の削除処理が実行中です。しばらくしてからお試しください。');
  }

  try {
    var company = findByKey_(SHEETS.Companies, 'company_id', companyId);
    if (!company) throw new Error('会社が見つかりません: ' + companyId);

    var companyName = company.company_name_raw || company.company_name_normalized || companyId;

    // スナップショット保存（監査ログにアーカイブ）
    var snapshot = JSON.stringify({
      company: company,
      permits: findAllByKey_(SHEETS.MLITPermits, 'company_id', companyId),
      notifications: findAllByKey_(SHEETS.Notifications, 'company_id', companyId).slice(0, 10)
    });
    writeAuditLog_(userEmail, 'HARD_DELETE_ARCHIVE', 'Company', companyId,
      'name=' + companyName + ' snapshot_length=' + snapshot.length);

    // 依存データ削除（MLITPermits）
    var deletedPermits = deleteRowsByKey_(SHEETS.MLITPermits, 'company_id', companyId);

    // 依存データ削除（Notifications）
    var deletedNotifs = deleteRowsByKey_(SHEETS.Notifications, 'company_id', companyId);

    // 本体削除（Companies）
    deleteRowsByKey_(SHEETS.Companies, 'company_id', companyId);

    writeAuditLog_(userEmail, 'HARD_DELETE_DONE', 'Company', companyId,
      'name=' + companyName + ' permits=' + deletedPermits + ' notifs=' + deletedNotifs);

    return {
      success: true,
      deleted: { company: companyName, permits: deletedPermits, notifications: deletedNotifs }
    };
  } finally {
    lock.releaseLock();
  }
}

/**
 * 指定キーに一致する行を全て物理削除する（下から上に走査）
 * @param {string} sheetName
 * @param {string} keyField
 * @param {string} keyValue
 * @return {number} 削除した行数
 */
function deleteRowsByKey_(sheetName, keyField, keyValue) {
  var sheet = getSheet_(sheetName);
  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return 0;

  var headers = data[0];
  var keyIdx = -1;
  for (var i = 0; i < headers.length; i++) {
    if (String(headers[i]).trim() === keyField) { keyIdx = i; break; }
  }
  if (keyIdx < 0) return 0;

  // 下から上に走査（削除時にインデックスがずれないように）
  var deleted = 0;
  for (var r = data.length - 1; r >= 1; r--) {
    if (String(data[r][keyIdx]).trim() === String(keyValue).trim()) {
      sheet.deleteRow(r + 1);
      deleted++;
    }
  }
  return deleted;
}

// ---------------------------------------------------------------------------
// Manual Notification
// ---------------------------------------------------------------------------
function sendManualNotification_(companyId, permitKey, userEmail) {
  var company = findByKey_(SHEETS.Companies, 'company_id', companyId);
  if (!company) throw new Error('会社が見つかりません');

  var email = company.contact_email;
  if (!email) throw new Error('連絡先メールが未設定です');

  // Find permit
  var permits = findAllByKey_(SHEETS.MLITPermits, 'company_id', companyId);
  var permit = permits[0]; // First permit
  if (permitKey) {
    permit = permits.filter(function(p) { return p.permit_number === permitKey; })[0] || permits[0];
  }
  if (!permit) throw new Error('許可情報が見つかりません');

  var companyName = company.company_name_raw || company.company_name_normalized || companyId;
  var daysLeft = permit.days_remaining !== '' ? Number(permit.days_remaining) : '不明';
  var subject = '【ご案内】建設業許可の有効期限について（' + companyName + '様）';

  var body = companyName + ' 御中\n\n' +
    'いつもお世話になっております。\n' +
    '東海インプル建設株式会社です。\n\n' +
    '貴社の建設業許可の有効期限が近づいておりますのでご案内申し上げます。\n\n' +
    '許可番号: ' + (permit.authority || '') + ' (' + (permit.category || '') + ') 第' + (permit.permit_number || '') + '号\n' +
    '有効期限: ' + (permit.expiry_date || '') + '（残り' + daysLeft + '日）\n\n' +
    '更新申請は有効期限の90日前から30日前までに行う必要がございます。\n' +
    'お手続きがまだの場合は、早めのご対応をお願いいたします。\n\n' +
    '何卒よろしくお願いいたします。';

  // Check ENABLE_SEND
  var enableSend = getConfig('ENABLE_SEND');
  if (enableSend !== 'true' && enableSend !== 'TRUE') {
    writeAuditLog_(userEmail, 'NOTIFY_DRYRUN', 'Permit', companyId, 'ENABLE_SEND=false, not sent');
    return { success: true, dryRun: true, message: 'ENABLE_SEND=falseのため送信されませんでした' };
  }

  // Send via Gmail
  var adminEmails = getConfig('ADMIN_EMAILS') || '';
  GmailApp.sendEmail(email, subject, body, {
    cc: adminEmails,
    name: '東海インプル建設 許可管理システム'
  });

  // Record notification
  var now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
  appendRecord_(SHEETS.Notifications, {
    notification_id: generateUuid(),
    sent_at: now,
    company_id: companyId,
    permit_id: permit.permit_number || '',
    to_email: email,
    cc_email: adminEmails,
    stage: 'MANUAL',
    subject: subject,
    body: body.substring(0, 500),
    result: 'SENT',
    error_message: ''
  });

  writeAuditLog_(userEmail, 'NOTIFY_SENT', 'Permit', companyId, 'to=' + email);

  return { success: true, sentTo: email };
}
