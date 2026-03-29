/**
 * api.gs — フロントエンド向けパブリックAPI
 *
 * google.script.run から呼び出される関数群。
 * 全関数の最後の引数は clientUserKey（未使用だがスキル準拠で保持）。
 */

var __currentUserEmail = '';

function _setUser_() {
  __currentUserEmail = getCurrentUserEmail_();
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

/**
 * ダッシュボードデータを返す
 */
function apiGetDashboard() {
  _setUser_();
  try {
    var data = buildDashboardData_();
    return toSerializable_(data);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// Company CRUD
// ---------------------------------------------------------------------------

/**
 * 会社詳細を返す（許可+通知履歴+監査ログ）
 */
function apiGetCompany(companyId) {
  _setUser_();
  try {
    var data = getCompanyDetail_(companyId);
    return toSerializable_(data);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社追加
 */
function apiAddCompany(formData) {
  _setUser_();
  try {
    var result = addCompany_(formData, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社編集
 */
function apiUpdateCompany(companyId, formData) {
  _setUser_();
  try {
    var result = updateCompany_(companyId, formData, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社無効化（ソフトデリート）
 */
function apiDeactivateCompany(companyId) {
  _setUser_();
  try {
    var result = deactivateCompany_(companyId, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社再有効化
 */
function apiReactivateCompany(companyId) {
  _setUser_();
  try {
    var result = reactivateCompany_(companyId, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// Notification
// ---------------------------------------------------------------------------

/**
 * 手動通知送信（特定の許可証に対して）
 */
function apiSendNotification(companyId, permitKey) {
  _setUser_();
  try {
    var result = sendManualNotification_(companyId, permitKey, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 通知履歴を返す
 */
function apiGetNotifications(limit) {
  _setUser_();
  try {
    limit = limit || 50;
    var records = readRecords_(SHEETS.Notifications);
    // 新しい順
    records.sort(function(a, b) {
      var sa = String(a.sent_at || ''), sb = String(b.sent_at || '');
      return sa < sb ? 1 : sa > sb ? -1 : 0;
    });
    return toSerializable_(records.slice(0, limit));
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

/**
 * 監査ログを返す
 */
function apiGetAuditLog(limit) {
  _setUser_();
  try {
    limit = limit || 30;
    var records = readRecords_(SHEETS.AuditLog);
    records.sort(function(a, b) {
      var ta = String(a.timestamp || ''), tb = String(b.timestamp || '');
      return ta < tb ? 1 : ta > tb ? -1 : 0;
    });
    return toSerializable_(records.slice(0, limit));
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 同期状態を返す
 */
function apiGetSyncStatus() {
  _setUser_();
  try {
    var runs = readRecords_(SHEETS.SyncRuns);
    if (runs.length === 0) return { lastSync: null, records: 0 };
    var last = runs[runs.length - 1];
    return toSerializable_(last);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 現在のユーザーメールを返す
 */
function apiGetCurrentUser() {
  return getCurrentUserEmail_();
}
