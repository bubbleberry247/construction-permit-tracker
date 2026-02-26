/**
 * FormHandler.gs — Google フォーム送信トリガーハンドラ
 */

/**
 * フォーム送信時に呼び出されるメイン関数
 * @param {Object} e  フォームトリガーイベントオブジェクト
 */
function onFormSubmit(e) {
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

    // 4. Submissions に詳細を更新（パース成功フラグ）
    // （最終的に OK/NG で上書きする）

    // 5. Company を検索・なければ作成
    var company = CompaniesModel.findByName(companyNameRaw);
    if (!company) {
      company = CompaniesModel.create({
        company_name:        companyNameRaw,
        contact_person:      contactPerson,
        contact_email:       contactEmail,
        contact_email_cc:    '',
        status:              '稼働中'
      });
    }
    var companyId = company.company_id;

    // 6. Permit を検索・更新 or 新規作成
    var existingPermit = PermitsModel.findByCompanyAndNumber(companyId, permitNumberRaw);
    var permitId;
    var permitVersion;

    if (existingPermit) {
      // 既存許可証の更新
      permitId = existingPermit.permit_id;
      permitVersion = (Number(existingPermit.permit_file_version) || 1) + 1;

      // 旧URLをnoteに保存
      var oldNote = existingPermit.note || '';
      var oldUrl = existingPermit.permit_file_url || '';
      var newNote = oldNote;
      if (oldUrl) {
        newNote = (oldNote ? oldNote + '\n' : '') + '旧: ' + oldUrl;
      }

      PermitsModel.update(permitId, {
        contact_person:            contactPerson,
        governor_or_minister:      governorOrMin,
        general_or_specific:       generalOrSpec,
        trade_categories:          tradeCategories,
        issue_date:                issueDate || issueDateRaw,
        expiry_date:               expiryDate,
        note:                      newNote,
        permit_file_version:       permitVersion,
        last_received_date:        submittedAt
      });
    } else {
      // 新規許可証
      permitVersion = 1;
      var newPermit = PermitsModel.create({
        company_id:                companyId,
        permit_number:             permitNumberRaw,
        governor_or_minister:      governorOrMin,
        general_or_specific:       generalOrSpec,
        trade_categories:          tradeCategories,
        issue_date:                issueDate || issueDateRaw,
        expiry_date:               expiryDate,
        renewal_deadline_date:     '',
        status:                    'VALID',
        last_received_date:        submittedAt,
        evidence_renewal_application: false,
        note:                      noteRaw,
        permit_file_version:       1
      });
      permitId = newPermit.permit_id;
    }

    // 7. PDFファイルを Drive に移動・リネーム
    var permitFileUrl = '';
    var permitFileDriveId = '';

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
        permitFileDriveId = permitFile.getId();
      } catch (fileErr) {
        logError('PDFファイル移動エラー', fileErr);
        // ファイル操作失敗でも処理継続（URLは空のまま）
      }
    }

    // 更新申請受付票の処理
    var evidenceFileUrl = '';
    if (evidenceFileId) {
      try {
        var evidenceFile = DriveApp.getFileById(evidenceFileId);
        evidenceFileUrl = evidenceFile.getUrl();
        PermitsModel.update(permitId, {
          evidence_renewal_application: true,
          evidence_file_url:            evidenceFileUrl
        });
      } catch (evErr) {
        logError('受付票ファイル取得エラー', evErr);
      }
    }

    // 8. Permits に permit_file_url / permit_file_drive_id 更新
    if (permitFileUrl || permitFileDriveId) {
      PermitsModel.update(permitId, {
        permit_file_url:      permitFileUrl,
        permit_file_drive_id: permitFileDriveId
      });
    }

    // Submissions を OK で更新
    SubmissionsModel.updateById(submissionId, {
      uploaded_file_url: permitFileUrl,
      parsed_result:     'OK',
      error_message:     ''
    });

    // 9. 受領通知メール送信
    var finalPermit = PermitsModel.findByCompanyAndNumber(companyId, permitNumberRaw);
    var finalCompany = CompaniesModel.findById(companyId);
    if (finalPermit && finalCompany) {
      Mailer.sendReceiptConfirmation(finalPermit, finalCompany);
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
}
