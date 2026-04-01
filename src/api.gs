/**
 * api.gs — フロントエンド向けパブリックAPI
 *
 * google.script.run から呼び出される関数群。
 * 全関数の最後の引数は clientUserKey（OAuth認証済みユーザーキー）。
 *
 * Bug2 fix: __authedUser を clientUserKey から設定 → requireAuth_() が Session より優先で参照
 * Bug3 fix: localStorage → try/catch + memory variable fallback（index.html 側）
 */

// OAuth 認証済みユーザー（requireAuth_() が参照）
var __authedUser = null;

// 後方互換用
var __currentUserEmail = '';

/**
 * clientUserKey（OAuth ログイン結果 JSON）から __authedUser をセットする
 * Bug2: この関数が Session より先に呼ばれるよう requireAuth_() で __authedUser を先行チェック
 * @param {string} clientUserKey — JSON string {"email":"...","role":"...","displayName":"..."}
 */
function _setUser_(clientUserKey) {
  __authedUser = null;
  if (clientUserKey) {
    try {
      var parsed = JSON.parse(clientUserKey);
      if (parsed && parsed.email) {
        __authedUser = {
          email: String(parsed.email || '').toLowerCase(),
          role: String(parsed.role || 'user'),
          displayName: String(parsed.displayName || '')
        };
      }
    } catch (e) {
      // clientUserKey が JSON でない場合はそのままメールとして扱う（後方互換）
      if (clientUserKey.indexOf('@') >= 0) {
        __authedUser = { email: clientUserKey.toLowerCase(), role: 'user', displayName: '' };
      }
    }
  }
  // 後方互換
  __currentUserEmail = (__authedUser && __authedUser.email) ? __authedUser.email : getCurrentUserEmail_();
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

/**
 * ダッシュボードデータを返す
 * @param {string} [clientUserKey]
 */
function apiGetDashboard(clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var data = buildDashboardData_();
    return toSerializable_(data);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// MLIT Search
// ---------------------------------------------------------------------------

/**
 * 国交省建設業者検索
 * @param {Object} formData — {permit_authority, permit_number}
 * @param {string} [clientUserKey]
 */
function apiSearchMlit(formData, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var result = searchMlit_(formData);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * MLIT検索結果から会社+許可を一括登録
 * @param {Object} formData — MLIT検索結果+ユーザー入力
 * @param {string} [clientUserKey]
 */
function apiRegisterCompanyWithPermit(formData, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var result = registerCompanyWithPermit_(formData, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// Company CRUD
// ---------------------------------------------------------------------------

/**
 * 会社詳細を返す（許可+通知履歴+監査ログ）
 * @param {string} companyId
 * @param {string} [clientUserKey]
 */
function apiGetCompany(companyId, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var data = getCompanyDetail_(companyId);
    return toSerializable_(data);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社追加
 * @param {Object} formData
 * @param {string} [clientUserKey]
 */
function apiAddCompany(formData, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var result = addCompany_(formData, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社編集
 * @param {string} companyId
 * @param {Object} formData
 * @param {string} [clientUserKey]
 */
function apiUpdateCompany(companyId, formData, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var result = updateCompany_(companyId, formData, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社無効化（ソフトデリート）
 * @param {string} companyId
 * @param {string} [clientUserKey]
 */
function apiDeactivateCompany(companyId, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var result = deactivateCompany_(companyId, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 会社再有効化
 * @param {string} companyId
 * @param {string} [clientUserKey]
 */
function apiReactivateCompany(companyId, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
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
 * @param {string} companyId
 * @param {string} permitKey
 * @param {string} [clientUserKey]
 */
function apiSendNotification(companyId, permitKey, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var result = sendManualNotification_(companyId, permitKey, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 通知履歴を返す
 * @param {number} [limit]
 * @param {string} [clientUserKey]
 */
function apiGetNotifications(limit, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
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
 * 監査ログを返す（管理者のみ）
 * @param {number} [limit]
 * @param {string} [clientUserKey]
 */
function apiGetAuditLog(limit, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAdmin_();
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
 * @param {string} [clientUserKey]
 */
function apiGetSyncStatus(clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var runs = readRecords_(SHEETS.SyncRuns);
    if (runs.length === 0) return { lastSync: null, records: 0 };
    var last = runs[runs.length - 1];
    return toSerializable_(last);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

/**
 * 現在のユーザーメールを返す（後方互換）
 */
function apiGetCurrentUser() {
  return getCurrentUserEmail_();
}

/**
 * 認証済みユーザー情報を返す（OAuth対応）
 * @param {string} [clientUserKey]
 * @return {{ email: string, role: string, displayName: string }}
 */
function apiGetCurrentUserInfo(clientUserKey) {
  _setUser_(clientUserKey);
  try {
    var user = requireAuth_();
    return { email: user.email, role: user.role, displayName: user.displayName };
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

// ---------------------------------------------------------------------------
// CSV Export
// ---------------------------------------------------------------------------

/**
 * CSVエクスポート（サーバーサイド生成）
 * @param {string} [clientUserKey]
 * @return {{ csv: string, filename: string }}
 */
function apiExportCsv(clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAuth_();
    var data = buildDashboardData_();
    var rows = data.rows || [];

    // CSV header
    var headers = ['会社名','許可番号','行政庁','般/特','業種数','般業種','特業種','有効期限','残日数','ステータス'];
    var csvLines = [headers.join(',')];

    rows.forEach(function(r) {
      var status = r.status === 'expired' ? '期限切れ' : r.status === 'danger' ? '期限切迫' : r.status === 'warn' ? '要注意' : r.status === 'ok' ? '有効' : '不明';
      var line = [
        csvEscape_(r.company_name),
        csvEscape_(r.permit_number),
        csvEscape_(r.authority),
        csvEscape_(r.category),
        r.trades_count || 0,
        csvEscape_((r.trades_ippan || '').replace(/\|/g, '/')),
        csvEscape_((r.trades_tokutei || '').replace(/\|/g, '/')),
        csvEscape_(r.expiry_date),
        r.days_remaining !== null ? r.days_remaining : '',
        status
      ];
      csvLines.push(line.join(','));
    });

    var filename = '建設業許可一覧_' + Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyyMMdd') + '.csv';
    return { csv: csvLines.join('\n'), filename: filename };
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}

function csvEscape_(val) {
  var s = String(val || '');
  if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

// ---------------------------------------------------------------------------
// Hard Delete
// ---------------------------------------------------------------------------

/**
 * 会社ハード削除（管理者のみ、アーカイブ後に物理削除）
 * @param {string} companyId
 * @param {string} confirmCode — 確認コード（会社IDの先頭8文字）
 * @param {string} [clientUserKey]
 */
function apiHardDeleteCompany(companyId, confirmCode, clientUserKey) {
  _setUser_(clientUserKey);
  try {
    requireAdmin_();

    // 2ステップ確認: confirmCode が companyId の先頭8文字と一致
    if (!confirmCode || confirmCode !== String(companyId).substring(0, 8)) {
      return { _error: true, message: '確認コードが一致しません' };
    }

    var result = hardDeleteCompany_(companyId, __currentUserEmail);
    return toSerializable_(result);
  } catch (e) {
    return { _error: true, message: String(e.message || e) };
  }
}
