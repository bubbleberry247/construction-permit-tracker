/**
 * Scheduler.gs — 日次バッチ（通知・ステータス更新）
 */

/**
 * time-driven トリガーで毎日実行するメイン関数
 */
function runDailyNotifications() {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(60000);
  } catch (lockErr) {
    logError('runDailyNotifications ロック取得失敗（多重実行防止）', lockErr);
    return;
  }
  try {
    // 1. NOTIFY_STAGES_DAYS を配列に変換
    var stagesStr = getConfig('NOTIFY_STAGES_DAYS') || '90,60,30,0';
    var stageDays = stagesStr.split(',').map(function(s) {
      return parseInt(s.trim(), 10);
    }).filter(function(n) { return !isNaN(n); }).sort(function(a, b) { return b - a; });

    // 2. 全アクティブ permit 取得
    var permits = PermitsModel.getAllActive();

    permits.forEach(function(permit) {
      try {
        processPermit_(permit, stageDays);
      } catch (err) {
        logError('permit処理エラー (permit_id: ' + permit.permit_id + ')', err);
      }
    });

    // 3. CompanyView シート更新（会社別集約ビュー）
    try {
      refreshCompanyView();
    } catch (err) {
      logError('CompanyView更新エラー', err);
    }

    // 5. 毎月1日に月次サマリー送信
    if (new Date().getDate() === 1) {
      Mailer.sendMonthlySummary();
    }

  } catch (err) {
    logError('runDailyNotifications エラー', err);
    sendErrorAlert('日次バッチエラー', err.message + '\n' + (err.stack || ''));
  } finally {
    lock.releaseLock();
  }
}

/**
 * 1件の permit を処理する
 * @param {Object} permit
 * @param {number[]} stageDays  降順ソート済みの通知ステージ日数配列
 */
function processPermit_(permit, stageDays) {
  var days = daysUntil(permit.expiry_date);
  if (isNaN(days)) return;

  // 通知ステージ判定（累積方式: determineStage_ 内で未送信チェック済み）
  var stage = determineStage_(days, stageDays, permit.permit_id);

  if (stage !== null) {
    var company = CompaniesModel.findById(permit.company_id);
    if (company) {
      Mailer.sendExpiryNotification(permit, company, stage);
    }
  }

  // ステータス更新
  updatePermitStatus_(permit, days);
}

/**
 * 日数からステージを判定する（累積方式: トリガー欠落時の取りこぼし防止）
 * stageDays の中で days <= stageDays[i] かつ最も緊急な（日数が小さい）未送信ステージを返す
 * @param {number} days  今日から満了日までの日数（負=過去）
 * @param {number[]} stageDays  降順ソート済み
 * @param {string} permitId  重複送信チェック用
 * @return {string|null}  ステージ文字列 or null（該当なし）
 */
function determineStage_(days, stageDays, permitId) {
  // EXPIRED チェック
  if (days < 0) {
    if (!NotificationsModel.hasBeenSent(permitId, 'EXPIRED')) {
      return 'EXPIRED';
    }
  }

  // 各ステージを昇順（小→大）で走査し、最も緊急な未送信ステージを返す
  for (var i = stageDays.length - 1; i >= 0; i--) {
    var sd = stageDays[i];
    if (days <= sd && !NotificationsModel.hasBeenSent(permitId, String(sd))) {
      return String(sd);
    }
  }
  return null;
}

/**
 * permit の current_status を days に応じて更新する
 * @param {Object} permit
 * @param {number} days  満了日までの日数（負=満了超過）
 */
function updatePermitStatus_(permit, days) {
  var currentStatus = String(permit.current_status || '').toUpperCase();
  var newStatus = null;

  // みなし有効の条件: 証拠書類あり + 申請日が有効期限内 + 満了超過
  var evidenceOk = permit.evidence_renewal_application === true ||
                   String(permit.evidence_renewal_application).toLowerCase() === 'true';
  var evidenceFileOk = permit.evidence_file_path && String(permit.evidence_file_path).trim() !== '';
  var appDate = permit.renewal_application_date ? new Date(permit.renewal_application_date) : null;
  var expiryDate = permit.expiry_date ? new Date(permit.expiry_date) : null;
  var appBeforeExpiry = appDate && expiryDate && appDate <= expiryDate;

  if (days < 0) {
    if (evidenceOk && evidenceFileOk && appBeforeExpiry) {
      newStatus = 'RENEWAL_IN_PROGRESS';  // みなし有効（発注継続OK）
    } else {
      newStatus = 'EXPIRED';
    }
  } else {
    var renewal_days = permit.renewal_deadline_date
        ? Math.floor((new Date(permit.renewal_deadline_date) - new Date()) / 86400000)
        : null;
    if (renewal_days !== null && renewal_days < 0) {
      newStatus = 'RENEWAL_OVERDUE';
    } else if (days <= 90) {
      newStatus = 'EXPIRING';
    } else {
      newStatus = 'VALID';
    }
  }

  if (newStatus && newStatus !== currentStatus) {
    PermitsModel.update(permit.permit_id, { current_status: newStatus });
  }
}

/**
 * メニューから手動実行するためのラッパー
 */
function runNow() {
  runDailyNotifications();
  SpreadsheetApp.getUi().alert('期限チェックを実行しました。Notificationsシートをご確認ください。');
}
