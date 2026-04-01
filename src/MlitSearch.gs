/**
 * MlitSearch.gs -- 国交省(MLIT)建設業者検索システムとの連携
 *
 * Python verify_permits_full.py (L40-373) をGoogle Apps Scriptに移植。
 * 検索API (kensetuKensaku.do) で sv_licenseNo を取得し、
 * 詳細API (ksGaiyo.do) で商号・有効期間・業種を取得する。
 *
 * レート制限は logic.gs 側で制御する（本ファイルでは扱わない）。
 */

// ---------------------------------------------------------------------------
// 定数
// ---------------------------------------------------------------------------

var MLIT_SEARCH_URL_ = 'https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do';
var MLIT_DETAIL_URL_ = 'https://etsuran2.mlit.go.jp/TAKKEN/ksGaiyo.do';

/** MLIT 29業種 略称（HTML表示順） */
var MLIT_TRADE_ABBREV_ = [
  '土木', '建築', '大工', '左官', 'とび・土工',
  '石工', '屋根', '電気', '管', 'タイル・れんが・ブロック',
  '鋼構造物', '鉄筋', '舗装', 'しゅんせつ', '板金',
  'ガラス', '塗装', '防水', '内装仕上', '機械器具設置',
  '熱絶縁', '電気通信', '造園', 'さく井', '建具',
  '水道施設', '消防施設', '清掃施設', '解体'
];

/** 都道府県コード（行政庁名 → 2桁コード） */
var PREF_CODE_ = {
  '北海道知事': '01', '青森県知事': '02', '岩手県知事': '03', '宮城県知事': '04',
  '秋田県知事': '05', '山形県知事': '06', '福島県知事': '07', '茨城県知事': '08',
  '栃木県知事': '09', '群馬県知事': '10', '埼玉県知事': '11', '千葉県知事': '12',
  '東京都知事': '13', '神奈川県知事': '14', '新潟県知事': '15', '富山県知事': '16',
  '石川県知事': '17', '福井県知事': '18', '山梨県知事': '19', '長野県知事': '20',
  '岐阜県知事': '21', '静岡県知事': '22', '愛知県知事': '23', '三重県知事': '24',
  '滋賀県知事': '25', '京都府知事': '26', '大阪府知事': '27', '兵庫県知事': '28',
  '奈良県知事': '29', '和歌山県知事': '30', '鳥取県知事': '31', '島根県知事': '32',
  '岡山県知事': '33', '広島県知事': '34', '山口県知事': '35', '徳島県知事': '36',
  '香川県知事': '37', '愛媛県知事': '38', '高知県知事': '39', '福岡県知事': '40',
  '佐賀県知事': '41', '長崎県知事': '42', '熊本県知事': '43', '大分県知事': '44',
  '宮崎県知事': '45', '鹿児島県知事': '46', '沖縄県知事': '47'
};

/** 和暦→西暦オフセット */
var WAREKI_OFFSETS_ = { R: 2018, H: 1988, S: 1925, T: 1911 };

// ---------------------------------------------------------------------------
// ヘルパー関数
// ---------------------------------------------------------------------------

/**
 * 許可行政庁名から licenseNoKbn を返す
 * @param {string} authority  例: "愛知県知事", "国土交通大臣"
 * @return {string} "00"=大臣, "01"=知事
 */
function getLicenseNoKbn_(authority) {
  if (!authority) return '01';
  return authority.indexOf('大臣') !== -1 ? '00' : '01';
}

/**
 * 許可行政庁名から都道府県コードを返す
 * @param {string} authority
 * @return {string|null} 2桁コード。大臣の場合は "00"、不明なら null
 */
function getPrefCode_(authority) {
  if (!authority) return null;
  if (authority.indexOf('大臣') !== -1) return '00';
  return PREF_CODE_[authority] || null;
}

/**
 * 許可番号を正規化する（先頭の0を除去）
 * @param {string} num  例: "057805"
 * @return {string} 正規化後。例: "57805"。空なら "0"
 */
function normalizePermitNumberForMlit_(num) {
  if (!num) return '0';
  var s = String(num).replace(/^0+/, '');
  return s || '0';
}

/**
 * 和暦期間テキストをISO日付ペアに変換する
 * @param {string} text  例: "R07年03月01日からR12年03月31日まで"
 * @return {string[]} [startIso, endIso]。パース失敗時は ["",""]
 */
function warekiPeriodToIso_(text) {
  if (!text) return ['', ''];
  var pattern = /([RHST])(\d{2})年(\d{2})月(\d{2})日から([RHST])(\d{2})年(\d{2})月(\d{2})日まで/;
  var m = String(text).match(pattern);
  if (!m) return ['', ''];

  var era1 = m[1], y1 = m[2], m1 = m[3], d1 = m[4];
  var era2 = m[5], y2 = m[6], m2 = m[7], d2 = m[8];

  var off1 = WAREKI_OFFSETS_[era1] || 0;
  var off2 = WAREKI_OFFSETS_[era2] || 0;

  try {
    var startYear = off1 + parseInt(y1, 10);
    var startMonth = parseInt(m1, 10);
    var startDay = parseInt(d1, 10);
    var endYear = off2 + parseInt(y2, 10);
    var endMonth = parseInt(m2, 10);
    var endDay = parseInt(d2, 10);

    // Date の妥当性チェック
    var startDate = new Date(startYear, startMonth - 1, startDay);
    var endDate = new Date(endYear, endMonth - 1, endDay);
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) return ['', ''];

    var startIso = Utilities.formatDate(startDate, 'Asia/Tokyo', 'yyyy-MM-dd');
    var endIso = Utilities.formatDate(endDate, 'Asia/Tokyo', 'yyyy-MM-dd');
    return [startIso, endIso];
  } catch (e) {
    return ['', ''];
  }
}

// ---------------------------------------------------------------------------
// 検索パラメータ構築
// ---------------------------------------------------------------------------

/**
 * MLIT検索APIのPOSTパラメータを構築する
 * @param {string} licenseNoKbn  "00"=大臣, "01"=知事
 * @param {string} permitNumber  許可番号
 * @return {Object} POSTパラメータオブジェクト
 */
function buildMlitSearchParams_(licenseNoKbn, permitNumber) {
  var num = normalizePermitNumberForMlit_(permitNumber);
  return {
    CMD: 'search',
    caller: 'KS',
    rdoSelect: '1',
    comNameKanaOnly: '',
    comNameKanjiOnly: '',
    rdoSelectJoken: '1',
    licenseNoKbn: licenseNoKbn,
    licenseNoFrom: num,
    licenseNoTo: num,
    keyWord: '',
    kenCode: '',
    choice: '',
    gyosyu: '',
    gyosyuType: '',
    sortValue: '',
    rdoSelectSort: '1',
    dispCount: '10',
    dispPage: '1',
    resultCount: '0',
    pageCount: '0',
    sv_rdoSelect: '',
    sv_rdoSelectJoken: '',
    sv_rdoSelectSort: '',
    sv_kenCode: '',
    sv_choice: '',
    sv_gyosyu: '',
    sv_gyosyuType: '',
    sv_keyWord: '',
    sv_sortValue: '',
    sv_pageListNo1: '',
    sv_pageListNo2: '',
    sv_comNameKanaOnly: '',
    sv_comNameKanjiOnly: '',
    sv_licenseNoKbn: '',
    sv_licenseNoFrom: '',
    sv_licenseNoTo: '',
    sv_licenseNo: '',
    sv_dispCount: '0',
    sv_dispPage: '0'
  };
}

// ---------------------------------------------------------------------------
// 検索API
// ---------------------------------------------------------------------------

/**
 * MLIT検索APIで sv_licenseNo の候補を取得する
 *
 * 検索結果HTMLから js_ShowDetail('NNNNN') パターンを全て抽出し、
 * expectedPrefCode でフィルタリングした結果を配列で返す。
 *
 * @param {string} licenseNoKbn  "00"=大臣, "01"=知事
 * @param {string} permitNumber  許可番号
 * @param {string|null} expectedPrefCode  期待する都道府県コード（先頭2桁でフィルタ）
 * @return {string[]} マッチした sv_licenseNo の配列（0件なら空配列）
 */
function searchMlitPermit_(licenseNoKbn, permitNumber, expectedPrefCode) {
  var params = buildMlitSearchParams_(licenseNoKbn, permitNumber);

  var options = {
    method: 'post',
    payload: params,
    muteHttpExceptions: true
  };

  var response;
  try {
    response = UrlFetchApp.fetch(MLIT_SEARCH_URL_, options);
  } catch (e) {
    Logger.log('searchMlitPermit_ fetch error: ' + e.message);
    return [];
  }

  var responseCode = response.getResponseCode();
  if (responseCode >= 500) {
    Logger.log('searchMlitPermit_ HTTP ' + responseCode);
    return [];
  }

  // cp932 (Shift_JIS) デコード
  var html = Utilities.newBlob(response.getContent()).getDataAsString('Shift_JIS');

  // js_ShowDetail('NNNNN') パターンを全て抽出
  var pattern = /js_ShowDetail\('(\d+)'\)/g;
  var allCandidates = [];
  var match;
  while ((match = pattern.exec(html)) !== null) {
    var svLicenseNo = match[1];
    // 重複除去
    if (allCandidates.indexOf(svLicenseNo) === -1) {
      allCandidates.push(svLicenseNo);
    }
  }

  if (allCandidates.length === 0) return [];

  // expectedPrefCode でフィルタリング
  if (expectedPrefCode) {
    var filtered = allCandidates.filter(function(sv) {
      return sv.substring(0, 2) === expectedPrefCode;
    });
    return filtered;
  }

  // 大臣許可（prefCode不明）の場合はフィルタなし
  return allCandidates;
}

// ---------------------------------------------------------------------------
// 詳細API
// ---------------------------------------------------------------------------

/**
 * MLIT詳細APIで会社情報を取得する
 *
 * @param {string} svLicenseNo  検索APIで取得した sv_licenseNo
 * @return {Object} 結果オブジェクト:
 *   {boolean} found - 取得成功か
 *   {string} apiName - 商号
 *   {string} expiryFrom - 有効期間開始（ISO）
 *   {string} expiryTo - 有効期間終了（ISO）
 *   {string} expiryWareki - 有効期間（和暦原文）
 *   {string[]} tradesIppan - 一般建設業の業種略称配列
 *   {string[]} tradesTokutei - 特定建設業の業種略称配列
 *   {string} error - エラーメッセージ（成功時は空文字）
 */
function fetchMlitDetail_(svLicenseNo) {
  var result = {
    found: false,
    apiName: '',
    expiryFrom: '',
    expiryTo: '',
    expiryWareki: '',
    tradesIppan: [],
    tradesTokutei: [],
    error: ''
  };

  var options = {
    method: 'post',
    payload: { sv_licenseNo: svLicenseNo, caller: 'KS' },
    muteHttpExceptions: true
  };

  var response;
  try {
    response = UrlFetchApp.fetch(MLIT_DETAIL_URL_, options);
  } catch (e) {
    result.error = 'FETCH_ERROR: ' + e.message;
    return result;
  }

  var responseCode = response.getResponseCode();
  if (responseCode >= 500) {
    result.error = 'HTTP ' + responseCode;
    return result;
  }

  // cp932 (Shift_JIS) デコード
  var html = Utilities.newBlob(response.getContent()).getDataAsString('Shift_JIS');

  // --- 商号パース ---
  var nameMatch = html.match(/<th[^>]*>[^<]*商号[^<]*<\/th>\s*<td[^>]*>([\s\S]*?)<\/td>/i);
  if (!nameMatch) {
    result.error = 'PARSE_ERROR: 商号が見つからない';
    return result;
  }

  var nameTdContent = nameMatch[1];
  // <p class="phonetic">...</p> タグを除去
  nameTdContent = nameTdContent.replace(/<p\s+class="phonetic"[^>]*>[\s\S]*?<\/p>/gi, '');
  // 残りのHTMLタグを除去
  nameTdContent = nameTdContent.replace(/<[^>]+>/g, '');
  // 空白を正規化
  var apiName = nameTdContent.replace(/\s+/g, ' ').replace(/^\s+|\s+$/g, '');

  if (!apiName) {
    result.error = 'PARSE_ERROR: 商号が空';
    return result;
  }

  result.found = true;
  result.apiName = apiName;

  // --- 有効期間パース ---
  var expiryMatch = html.match(/<th[^>]*>[^<]*許可の有効期間[^<]*<\/th>\s*<td[^>]*>([\s\S]*?)<\/td>/i);
  if (expiryMatch) {
    var expiryTdContent = expiryMatch[1].replace(/<[^>]+>/g, '').replace(/\s+/g, '');
    result.expiryWareki = expiryTdContent;
    var isoDates = warekiPeriodToIso_(expiryTdContent);
    result.expiryFrom = isoDates[0];
    result.expiryTo = isoDates[1];
  }

  // --- 業種パース ---
  // re_summ_3 テーブルを探す
  var tableMatch = html.match(/<table[^>]*class="re_summ_3"[^>]*>([\s\S]*?)<\/table>/i);
  if (tableMatch) {
    var tableContent = tableMatch[1];
    // 2行目（データ行）の <tr> を取得
    var trMatches = tableContent.match(/<tr[^>]*>([\s\S]*?)<\/tr>/gi);
    if (trMatches && trMatches.length >= 2) {
      var dataRow = trMatches[1];
      // 各 <td> の値を取得
      var tdPattern = /<td[^>]*>([\s\S]*?)<\/td>/gi;
      var tdMatch;
      var tdIndex = 0;
      while ((tdMatch = tdPattern.exec(dataRow)) !== null) {
        var val = tdMatch[1].replace(/<[^>]+>/g, '').replace(/\s+/g, '');
        if (tdIndex < MLIT_TRADE_ABBREV_.length) {
          if (val === '1') {
            result.tradesIppan.push(MLIT_TRADE_ABBREV_[tdIndex]);
          } else if (val === '2') {
            result.tradesTokutei.push(MLIT_TRADE_ABBREV_[tdIndex]);
          }
        }
        tdIndex++;
      }
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Company ID 採番
// ---------------------------------------------------------------------------

/**
 * 次のC+連番のcompany_idを生成する
 *
 * Companies と MLITPermits の両方からcompany_idを収集し、
 * "C" + 数字パターンの最大値を求めて +1 する。
 * 4桁ゼロ埋め（例: C0146）。
 *
 * @return {string} 新しいcompany_id（例: "C0146"）
 */
function getNextCompanyId_() {
  var maxNum = 0;

  // Companies シートから収集
  var companies = readRecords_(SHEETS.Companies);
  for (var i = 0; i < companies.length; i++) {
    var cid = String(companies[i].company_id || '');
    var m = cid.match(/^C(\d+)$/);
    if (m) {
      var n = parseInt(m[1], 10);
      if (n > maxNum) maxNum = n;
    }
  }

  // MLITPermits シートから収集
  var mlitPermits = readRecords_(SHEETS.MLITPermits);
  for (var j = 0; j < mlitPermits.length; j++) {
    var mid = String(mlitPermits[j].company_id || '');
    var mm = mid.match(/^C(\d+)$/);
    if (mm) {
      var mn = parseInt(mm[1], 10);
      if (mn > maxNum) maxNum = mn;
    }
  }

  var nextNum = maxNum + 1;
  var padded = String(nextNum);
  while (padded.length < 4) {
    padded = '0' + padded;
  }
  return 'C' + padded;
}

// ---------------------------------------------------------------------------
// テスト関数
// ---------------------------------------------------------------------------

/**
 * GASエディタから手動実行するテスト関数
 * 既知の許可番号で検索・詳細取得をテストする
 */
function testMlitSearch_() {
  Logger.log('=== MLIT Search Test ===');

  // テストケース: 愛知県知事 許可番号 57805
  var authority = '愛知県知事';
  var permitNumber = '57805';

  var licenseNoKbn = getLicenseNoKbn_(authority);
  var prefCode = getPrefCode_(authority);

  Logger.log('authority: ' + authority);
  Logger.log('licenseNoKbn: ' + licenseNoKbn);
  Logger.log('prefCode: ' + prefCode);
  Logger.log('permitNumber: ' + permitNumber);

  // Step 1: 検索
  Logger.log('\n--- Step 1: Search ---');
  var candidates = searchMlitPermit_(licenseNoKbn, permitNumber, prefCode);
  Logger.log('candidates: ' + JSON.stringify(candidates));

  if (candidates.length === 0) {
    Logger.log('No candidates found. Test finished.');
    return;
  }

  // Step 2: 詳細取得（最初の候補）
  Logger.log('\n--- Step 2: Detail ---');
  var svLicenseNo = candidates[0];
  Logger.log('Fetching detail for svLicenseNo: ' + svLicenseNo);

  Utilities.sleep(1000); // レート制限

  var detail = fetchMlitDetail_(svLicenseNo);
  Logger.log('found: ' + detail.found);
  Logger.log('apiName: ' + detail.apiName);
  Logger.log('expiryFrom: ' + detail.expiryFrom);
  Logger.log('expiryTo: ' + detail.expiryTo);
  Logger.log('expiryWareki: ' + detail.expiryWareki);
  Logger.log('tradesIppan: ' + JSON.stringify(detail.tradesIppan));
  Logger.log('tradesTokutei: ' + JSON.stringify(detail.tradesTokutei));
  Logger.log('error: ' + detail.error);

  // ヘルパー関数テスト
  Logger.log('\n--- Helper Tests ---');
  Logger.log('warekiPeriodToIso_("R07年03月01日からR12年03月31日まで"): '
    + JSON.stringify(warekiPeriodToIso_('R07年03月01日からR12年03月31日まで')));
  Logger.log('normalizePermitNumberForMlit_("057805"): '
    + normalizePermitNumberForMlit_('057805'));
  Logger.log('normalizePermitNumberForMlit_("0"): '
    + normalizePermitNumberForMlit_('0'));
  Logger.log('normalizePermitNumberForMlit_(""): '
    + normalizePermitNumberForMlit_(''));
  Logger.log('getNextCompanyId_(): ' + getNextCompanyId_());

  Logger.log('\n=== Test Complete ===');
}
