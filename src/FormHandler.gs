/**
 * FormHandler.gs — Google フォーム送信トリガーハンドラ
 */

/**
 * フォーム送信時に呼び出されるメイン関数
 * @param {Object} e  フォームトリガーイベントオブジェクト
 */
function onFormSubmit(e) {
  var lock = LockService.getDocumentLock();
  try {
    lock.waitLock(30000);
  } catch (lockErr) {
    logError('onFormSubmit ロック取得失敗（同時実行制御）', lockErr);
    sendErrorAlert('フォーム処理ロック取得失敗', 'フォーム送信の同時処理でロック取得に失敗しました。手動確認が必要です。');
    return;
  }
  try {

  var triggerUid = (e && e.triggerUid) ? String(e.triggerUid) : '';

  // triggerUid による重複チェック
  if (triggerUid) {
    var existing = SubmissionsModel.findByTriggerUid(triggerUid);
    if (existing && existing.parsed_result === 'OK') {
      Logger.log('重複送信スキップ: triggerUid=' + triggerUid);
      return;
    }
  }

  var submissionId = generateUuid();
  var submittedAt = new Date();

  // 1. フォーム回答を取得
  var nv = e.namedValues || {};
  function first(key) {
    var arr = nv[key];
    return arr && arr.length > 0 ? String(arr[0]).trim() : '';
  }

  var companyNameRaw    = first('協力会社名');
  var contactPerson     = first('担当者名');
  var contactEmail      = first('通知先メール');
  var permitNumberRaw   = first('許可番号');
  var governorOrMin     = first('許可区分（知事/大臣）');
  var generalOrSpec     = first('一般/特定');
  var tradeCategories   = first('許可業種');
  var issueDateRaw      = first('許可年月日');
  var expiryDateRaw     = first('満了日');
  var noteRaw           = first('備考');

  // Google フォームのファイルアップロード項目はファイルIDが返る
  var permitFileId      = first('許可証PDF');
  var evidenceFileId    = first('更新申請受付票PDF');

  // Submissions にまず記録（後で更新）
  var submissionData = {
    submission_id:          submissionId,
    trigger_uid:            triggerUid,
    submitted_at:           submittedAt,
    company_name_raw:       companyNameRaw,
    contact_email_raw:      contactEmail,
    permit_number_raw:      permitNumberRaw,
    expiry_date_raw:        expiryDateRaw,
    uploaded_file_drive_id: permitFileId,
    uploaded_file_url:      '',
    parsed_result:          'PROCESSING',
    error_message:          ''
  };
  SubmissionsModel.create(submissionData);

  try {
    // 3. バリデーション
    var expiryDate = parseDate(expiryDateRaw);
    if (!expiryDate) {
      throw new Error('満了日の形式が不正です: ' + expiryDateRaw);
    }
    var daysLeft = daysUntil(expiryDate);
    if (daysLeft < -180) {
      throw new Error('満了日が180日以上前です（古すぎるデータ）: ' + expiryDateRaw);
    }

    var issueDate = parseDate(issueDateRaw);

    // 許可番号をパースして構造化データを取得
    var parsed = parsePermitNumber_(permitNumberRaw);
    var contractorNumber = parsed.parse_success ? parsed.contractor_number : '';
    var permitAuthorityName = parsed.parse_success ? parsed.permit_authority_name : '';
    var permitCategory = generalOrSpec || (parsed.parse_success ? parsed.permit_category : '');
    var permitYear = parsed.parse_success ? parsed.permit_year : 0;

    // 4. Submissions に詳細を更新（パース成功フラグ）
    // （最終的に OK/NG で上書きする）

    // 5. Company を検索・なければ作成（完全一致のみ）
    // normalizeCompanyName_ で Python 側と同じ正規化（株式会社等除去 + 空白除去 + 小文字化）
    var normalizedName = normalizeCompanyName_(companyNameRaw);
    if (!normalizedName) {
      // 正規化後に空になる場合は会社名不明 → エラー
      throw new Error('会社名の正規化結果が空です: "' + companyNameRaw + '"');
    }
    var company = CompaniesModel.findByNormalizedName(normalizedName);
    if (!company) {
      company = CompaniesModel.create({
        company_name_raw:        companyNameRaw,
        company_name_normalized: normalizedName,
        contact_person:          contactPerson,
        contact_email:           contactEmail,
        contact_email_cc:        '',
        status:                  'ACTIVE'
      });
    }
    var companyId = company.company_id;

    // 6. Permit を検索・更新 or 新規作成（upsertキーで検索）
    var existingPermit = PermitsModel.findByUpsertKey(companyId, permitAuthorityName, contractorNumber, permitCategory);
    var permitId;
    var permitVersion;

    if (existingPermit) {
      // 既存許可証の更新
      permitId = existingPermit.permit_id;
      permitVersion = (Number(existingPermit.permit_file_version) || 1) + 1;

      // 旧Drive URLをnoteに保存
      var oldNote = existingPermit.note || '';
      var oldUrl = existingPermit.permit_file_share_url || '';
      var newNote = oldNote;
      if (oldUrl) {
        newNote = (oldNote ? oldNote + '\n' : '') + '旧: ' + oldUrl;
      }

      PermitsModel.update(permitId, {
        contact_person:                  contactPerson,
        permit_authority_name:           permitAuthorityName,
        permit_authority_name_normalized: permitAuthorityName,
        permit_authority_type:           governorOrMin,
        permit_category:                 permitCategory,
        contractor_number:               contractorNumber,
        permit_year:                     permitYear,
        permit_number_full:              permitNumberRaw,
        trade_categories:                tradeCategories,
        issue_date:                      issueDate || issueDateRaw,
        expiry_date:                     expiryDate,
        note:                            newNote,
        permit_file_version:             permitVersion,
        last_received_date:              submittedAt
      });
    } else {
      // 新規許可証
      permitVersion = 1;
      var newPermit = PermitsModel.create({
        company_id:                      companyId,
        company_name_raw:                companyNameRaw,
        permit_number_full:              permitNumberRaw,
        permit_authority_name:           permitAuthorityName,
        permit_authority_name_normalized: permitAuthorityName,
        permit_authority_type:           governorOrMin,
        permit_category:                 permitCategory,
        permit_year:                     permitYear,
        contractor_number:               contractorNumber,
        trade_categories:                tradeCategories,
        issue_date:                      issueDate || issueDateRaw,
        expiry_date:                     expiryDate,
        renewal_deadline_date:           '',
        current_status:                  'VALID',
        last_received_date:              submittedAt,
        evidence_renewal_application:    false,
        note:                            noteRaw,
        permit_file_version:             1
      });
      permitId = newPermit.permit_id;
    }

    // 7. PDFファイルを Drive に移動・リネーム
    var permitFileUrl = '';
    var permitFileDriveId = '';
    var pdfSaveFailed = false;

    if (permitFileId) {
      try {
        var permitFile = DriveApp.getFileById(permitFileId);
        var rootFolderId = getConfig('DRIVE_ROOT_FOLDER_ID');
        var rootFolder = DriveApp.getFolderById(rootFolderId);

        // 会社別フォルダを取得 or 作成
        var companyFolder;
        var folderIter = rootFolder.getFoldersByName(companyNameRaw);
        if (folderIter.hasNext()) {
          companyFolder = folderIter.next();
        } else {
          companyFolder = rootFolder.createFolder(companyNameRaw);
        }

        // リネーム
        var expiryStr = formatDate(expiryDate, 'yyyyMMdd');
        var newFileName = companyNameRaw + '_建設業許可_' + permitNumberRaw +
                          '_満了' + expiryStr + '_v' + permitVersion + '.pdf';
        permitFile.setName(newFileName);

        // 移動
        permitFile.moveTo(companyFolder);

        permitFileUrl = permitFile.getUrl();
      } catch (fileErr) {
        logError('PDFファイル移動エラー', fileErr);
        pdfSaveFailed = true;
      }
    }

    // PDF保存失敗時: ユーザーがファイルをアップロードしたのに保存できなかった場合
    if (pdfSaveFailed) {
      SubmissionsModel.updateById(submissionId, {
        parsed_result: 'REVIEW_NEEDED',
        error_message: 'PDF保存失敗: 手動確認が必要です'
      });

      sendErrorAlert(
        'PDF保存失敗 (' + companyNameRaw + ')',
        '会社名: ' + companyNameRaw + '\n' +
        '許可番号: ' + permitNumberRaw + '\n' +
        'アップロードファイルID: ' + permitFileId + '\n' +
        'PDFファイルのDrive移動・リネームに失敗しました。\n' +
        '元ファイルがGoogleフォームの回答フォルダに残っている可能性があります。手動で確認してください。'
      );

      // 受領確認メールは送信しない（ユーザーに「OK」と誤通知しないため）
      return;
    }

    // 更新申請受付票の処理
    var evidenceFileUrl = '';
    if (evidenceFileId) {
      try {
        var evidenceFile = DriveApp.getFileById(evidenceFileId);
        evidenceFileUrl = evidenceFile.getUrl();
        PermitsModel.update(permitId, {
          evidence_renewal_application: true,
          evidence_file_path:           evidenceFileUrl
        });
      } catch (evErr) {
        logError('受付票ファイル取得エラー', evErr);
      }
    }

    // 8. Permits に permit_file_share_url / permit_file_path 更新
    if (permitFileUrl || permitFileDriveId) {
      PermitsModel.update(permitId, {
        permit_file_share_url: permitFileUrl,
        permit_file_path:      permitFileDriveId
      });
    }

    // Submissions を OK で更新
    SubmissionsModel.updateById(submissionId, {
      uploaded_file_url: permitFileUrl,
      parsed_result:     'OK',
      error_message:     ''
    });

    // 9. 受領通知メール送信
    try {
      var finalPermit = PermitsModel.findByCompanyAndNumber(companyId, permitNumberRaw);
      var finalCompany = CompaniesModel.findById(companyId);
      if (finalPermit && finalCompany) {
        Mailer.sendReceiptConfirmation(finalPermit, finalCompany);
      }
    } catch (mailErr) {
      logError('受領確認メール送信エラー（処理自体は成功）', mailErr);
    }

  } catch (err) {
    logError('onFormSubmit エラー', err);

    // Submissions を NG で更新
    SubmissionsModel.updateById(submissionId, {
      parsed_result: 'NG',
      error_message: err.message || String(err)
    });

    // ADMIN_EMAILS にエラー通知
    sendErrorAlert(
      'フォーム送信処理エラー (' + companyNameRaw + ')',
      '会社名: ' + companyNameRaw + '\n' +
      '許可番号: ' + permitNumberRaw + '\n' +
      'エラー: ' + (err.message || String(err)) + '\n' +
      (err.stack || '')
    );
  }

  } finally {
    lock.releaseLock();
  }
}
