/**
 * Scheduler.gs — 日次バッチ（通知・ステータス更新）
 */

/**
 * time-driven トリガーで毎日実行するメイン関数
 */
function runDailyNotifications() {
  try {
    // 1. NOTIFY_STAGES_DAYS を配列に変換
    var stagesStr = getConfig('NOTIFY_STAGES_DAYS') || '120,90,60,45,30,14,0';
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

    // 5. 月曜のみ週次サマリー送信
    if (isTodayDayOfWeek(1)) {
      Mailer.sendWeeklySummary();
    }

  } catch (err) {
    logError('runDailyNotifications エラー', err);
    sendErrorAlert('日次バッチエラー', err.message + '\n' + (err.stack || ''));
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

  // 通知ステージ判定
  var stage = determineStage_(days, stageDays);

  if (stage !== null) {
    // 未送信の場合のみ通知
    if (!NotificationsModel.hasBeenSent(permit.permit_id, stage)) {
      var company = CompaniesModel.findById(permit.company_id);
      if (company) {
        Mailer.sendExpiryNotification(permit, company, stage);
      }
    }
  }

  // ステータス更新
  updatePermitStatus_(permit, days);
}

/**
 * 日数からステージを判定する
 * @param {number} days  今日から満了日までの日数（負=過去）
 * @param {number[]} stageDays  降順ソート済み
 * @return {string|null}  ステージ文字列 or null（該当なし）
 */
function determineStage_(days, stageDays) {
  if (days < 0) return 'EXPIRED';

  // 各ステージ日数に対して ±1日の許容範囲で判定
  for (var i = 0; i < stageDays.length; i++) {
    var sd = stageDays[i];
    if (Math.abs(days - sd) <= 1) {
      return String(sd);
    }
  }
  return null;
}

/**
 * permit のステータスを days に応じて更新する
 * @param {Object} permit
 * @param {number} days
 */
function updatePermitStatus_(permit, days) {
  var currentStatus = String(permit.status || '').toUpperCase();
  var newStatus = null;
  var evidenceSubmitted = permit.evidence_renewal_application === true ||
                          String(permit.evidence_renewal_application).toLowerCase() === 'true';

  if (days < 0) {
    if (evidenceSubmitted) {
      newStatus = 'RENEWAL_IN_PROGRESS';
    } else {
      newStatus = 'EXPIRED';
    }
  } else if (days <= 30) {
    newStatus = 'EXPIRING';
  } else {
    // days > 30 の場合、RENEWAL_IN_PROGRESS は維持
    if (currentStatus !== 'RENEWAL_IN_PROGRESS') {
      newStatus = 'VALID';
    }
  }

  if (newStatus && newStatus !== currentStatus) {
    PermitsModel.update(permit.permit_id, {
      status:            newStatus,
      last_checked_date: new Date()
    });
  } else {
    // ステータス変更なしでも last_checked_date だけ更新
    PermitsModel.update(permit.permit_id, {
      last_checked_date: new Date()
    });
  }
}

/**
 * メニューから手動実行するためのラッパー
 */
function runNow() {
  runDailyNotifications();
  SpreadsheetApp.getUi().alert('期限チェックを実行しました。Notificationsシートをご確認ください。');
}
