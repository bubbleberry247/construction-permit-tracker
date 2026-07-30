/**
 * Scheduler.gs — 日次バッチ（通知・ステータス更新）
 */

/**
 * time-driven トリガーで毎日実行するメイン関数
 * 末尾 "_" によりWebクライアントからは呼び出せない。
 * @return {Object} 実行結果
 */
function runDailyNotifications_() {
  // 送信ゲートがScriptLockを使うため、バッチ多重実行防止はDocumentLockで分離する。
  // このプロジェクトはスプレッドシートにバインドされたスクリプト。
  var lock = LockService.getDocumentLock();
  try {
    lock.waitLock(60000);
  } catch (lockErr) {
    logError_('runDailyNotifications_ ロック取得失敗（多重実行防止）', lockErr);
    return {
      success: false,
      processed: 0,
      errors: 1,
      message: lockErr.message || String(lockErr)
    };
  }
  try {
    // 1. 通知候補だけを生成する。modeが自動送信段階の場合だけ後段で送信する。
    var candidateResult = generateNotificationCandidates_('SYSTEM_DAILY');

    // 2. 許可ステータス更新
    var permits = PermitsModel.getAllActive();
    var processed = 0;
    var errors = 0;

    permits.forEach(function(permit) {
      try {
        var days = daysUntil_(permit.expiry_date);
        if (!isNaN(days)) updatePermitStatus_(permit, days);
        processed++;
      } catch (err) {
        errors++;
        logError_('permit処理エラー (permit_id: ' + permit.permit_id + ')', err);
      }
    });

    // 3. CompanyView シート更新（会社別集約ビュー）
    try {
      refreshCompanyView_();
    } catch (err) {
      logError_('CompanyView更新エラー', err);
    }

    // 4. OFF / 手動modeではattempted=0。自動modeでもpolicy対象だけを最大20件処理する。
    var autoResult = sendAutoEligibleCandidates_(20);

    return {
      success: true,
      processed: processed,
      errors: errors,
      candidates: candidateResult,
      autoSend: autoResult,
      message: '通知候補生成と期限更新が完了しました'
    };

  } catch (err) {
    logError_('runDailyNotifications_ エラー', err);
    // エラー時に別経路の自動メールを送らず、実行ログとAuditLogで運用者へ接続する。
    try {
      appendAuditEvent_({
        user_email: 'SYSTEM_DAILY',
        action: 'DAILY_NOTIFICATION_WORKFLOW',
        target_type: 'Scheduler',
        target_id: '',
        details: String(err.message || err).substring(0, 500),
        status: 'ABORTED',
        error_code: String(err.code || 'DAILY_WORKFLOW_FAILED')
      });
    } catch (ignoredAuditError) {
      console.error('[DAILY_AUDIT_FAILED] ' + String(ignoredAuditError.message || ignoredAuditError));
    }
    return {
      success: false,
      processed: 0,
      errors: 1,
      message: err.message || String(err)
    };
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
  var days = daysUntil_(permit.expiry_date);
  if (isNaN(days)) return;
  // 後方互換private関数。送信は行わず、通知候補生成と配信を分離する。
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
    if (!NotificationsModel.hasBeenReservedOrSent(permitId, 'EXPIRED')) {
      return 'EXPIRED';
    }
  }

  // 各ステージを昇順（小→大）で走査し、最も緊急な未送信ステージを返す
  for (var i = stageDays.length - 1; i >= 0; i--) {
    var sd = stageDays[i];
    if (days <= sd && !NotificationsModel.hasBeenReservedOrSent(permitId, String(sd))) {
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
 * Spreadsheet UI専用。末尾 "_" によりWebクライアントからは呼び出せない。
 */
function runNow_() {
  var ui = SpreadsheetApp.getUi();
  var result = runDailyNotifications_();
  if (result && result.success) {
    ui.alert(
      '期限チェック完了',
      '処理件数: ' + result.processed + '件\n' +
        '個別エラー: ' + result.errors + '件\n' +
        'NotificationQueueをご確認ください。',
      ui.ButtonSet.OK
    );
  } else {
    ui.alert(
      '期限チェック停止',
      '安全条件を満たさないため処理を停止しました。\n' +
        ((result && result.message) || '詳細は実行ログをご確認ください。'),
      ui.ButtonSet.OK
    );
  }
  return result;
}
