'use strict';

/**
 * 推奨判断または承認済み判断を使い、MasterMigration.gsと同じロジックで
 * Companies正本候補をローカル生成する。Google Sheetsへの書込みは行わない。
 */

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.resolve(__dirname, '..');
const SRC = path.join(ROOT, 'src');

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) {
      throw new Error(`unexpected argument: ${token}`);
    }
    const key = token.slice(2);
    const value = argv[index + 1];
    if (!value || value.startsWith('--')) {
      throw new Error(`missing value for --${key}`);
    }
    result[key] = value;
    index += 1;
  }
  for (const required of ['input-dir', 'output-dir', 'decisions-json']) {
    if (!result[required]) throw new Error(`--${required} is required`);
  }
  return result;
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = '';
  let quoted = false;
  const source = String(text || '').replace(/^\uFEFF/, '');

  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];
    const next = source[index + 1];
    if (quoted) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === ',') {
      row.push(field);
      field = '';
    } else if (char === '\n') {
      row.push(field.replace(/\r$/, ''));
      rows.push(row);
      row = [];
      field = '';
    } else {
      field += char;
    }
  }
  if (field || row.length) {
    row.push(field.replace(/\r$/, ''));
    rows.push(row);
  }
  const headers = rows.shift();
  if (!headers) return [];
  return rows
    .filter(values => values.some(value => value !== ''))
    .map(values => Object.fromEntries(
      headers.map((header, index) => [header, values[index] || ''])
    ));
}

function sanitizeSpreadsheetValue(value) {
  const text = value == null ? '' : String(value);
  return /^[=+\-@]/.test(text) ? `'${text}` : text;
}

function csvCell(value) {
  const text = sanitizeSpreadsheetValue(value);
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function toCsv(headers, records) {
  return [
    headers.map(csvCell).join(','),
    ...records.map(record => headers.map(header => csvCell(record[header])).join(','))
  ].join('\r\n') + '\r\n';
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, ''));
}

function recordsFromSnapshot(filePath) {
  const payload = readJson(filePath);
  return Array.isArray(payload) ? payload : (payload.records || []);
}

function sha256File(filePath) {
  return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function companyIdsFrom(records) {
  return records
    .map(record => String(record.company_id || '').trim())
    .filter(Boolean);
}

function numericCompanyId(companyId) {
  const match = /^C(\d+)$/.exec(String(companyId || ''));
  return match ? Number(match[1]) : -1;
}

function atomicWrite(filePath, content) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tempPath, content);
  fs.renameSync(tempPath, filePath);
}

function buildContext(rows, decisions, generatedAt) {
  const properties = {
    MASTER_SYSTEM_ONLY_DECISIONS: JSON.stringify(decisions)
  };
  const context = {
    Array,
    Boolean,
    Date,
    Error,
    JSON,
    Math,
    Number,
    Object,
    RegExp,
    String,
    console,
    SHEETS: {
      Companies: 'Companies',
      Permits: 'Permits',
      MLITPermits: 'MLITPermits',
      MasterImportStaging: 'MasterImportStaging'
    },
    readRecords_: name => (rows[name] || []).map(record => ({ ...record })),
    getSecureSetting_: key => properties[key] || '',
    getNowString_: () => generatedAt,
    getCompanyVersion_: company => {
      const version = Number(company.data_version);
      return Number.isInteger(version) && version > 0 ? version : 1;
    },
    appError_: (code, message, retryable) => {
      const error = new Error(message);
      error.code = code;
      error.retryable = Boolean(retryable);
      return error;
    }
  };
  vm.createContext(context);
  vm.runInContext(
    fs.readFileSync(path.join(SRC, 'Schema.gs'), 'utf8'),
    context,
    { filename: 'Schema.gs' }
  );
  // Schema.gsのApps Script専用時刻関数をローカルdry-run用へ戻す。
  context.getNowString_ = () => generatedAt;
  context.getCompanyVersion_ = company => {
    const version = Number(company.data_version);
    return Number.isInteger(version) && version > 0 ? version : 1;
  };
  vm.runInContext(
    fs.readFileSync(path.join(SRC, 'MasterMigration.gs'), 'utf8'),
    context,
    { filename: 'MasterMigration.gs' }
  );
  return context;
}

function normalize(value) {
  return String(value || '').trim().toLowerCase()
    .replace(/[\s\u3000]/g, '')
    .replace(/㈱/g, '株式会社')
    .replace(/㈲/g, '有限会社')
    .replace(/[（(]株[）)]/g, '株式会社')
    .replace(/[（(]有[）)]/g, '有限会社');
}

function resolveNewCompany(stagingRow, canonical, initialIds) {
  const candidates = canonical.filter(company => !initialIds.has(company.company_id));
  const vendorNo = String(stagingRow.vendor_no || '').trim();
  if (vendorNo) {
    const vendorMatches = candidates.filter(
      company => String(company.vendor_no || '').trim() === vendorNo
    );
    if (vendorMatches.length === 1) return vendorMatches[0];
  }
  const normalized = normalize(
    stagingRow.company_name_normalized || stagingRow.company_name_raw
  );
  const nameMatches = candidates.filter(company => (
    normalize(company.company_name_normalized || company.company_name_raw) === normalized
  ));
  if (nameMatches.length !== 1) {
    throw new Error(
      `unable to resolve allocated company_id for source_row=${stagingRow.source_row}`
    );
  }
  return nameMatches[0];
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const inputDir = path.resolve(args['input-dir']);
  const outputDir = path.resolve(args['output-dir']);
  const decisionsPath = path.resolve(args['decisions-json']);
  const generatedAt = args['generated-at'] || new Date().toISOString();
  const approvalMode = args['approval-mode'] || 'simulation';
  if (!['simulation', 'formal'].includes(approvalMode)) {
    throw new Error('--approval-mode must be simulation or formal');
  }
  const formalApproval = approvalMode === 'formal';
  const reviewer = args.reviewer || (
    formalApproval ? 'USER_APPROVED_VIA_CODEX' : 'RECOMMENDATION_SIMULATION'
  );
  const approvalStatement = args['approval-statement'] || (
    formalApproval
      ? '推奨どおり正式承認で進めて'
      : '推奨判断によるローカルシミュレーション'
  );

  const inputPaths = {
    staging: path.join(inputDir, 'master_import_staging.csv'),
    systemOnly: path.join(inputDir, 'system_only.csv'),
    companies: path.join(inputDir, 'snapshot_companies.json'),
    permits: path.join(inputDir, 'snapshot_permits.json'),
    mlitPermits: path.join(inputDir, 'snapshot_mlit_permits.json'),
    summary: path.join(inputDir, 'summary.json')
  };
  const staging = parseCsv(fs.readFileSync(inputPaths.staging, 'utf8'));
  const systemOnly = parseCsv(fs.readFileSync(inputPaths.systemOnly, 'utf8'));
  const companies = recordsFromSnapshot(inputPaths.companies);
  const permits = recordsFromSnapshot(inputPaths.permits);
  const mlitPermits = recordsFromSnapshot(inputPaths.mlitPermits);
  const sourceSummary = readJson(inputPaths.summary);
  const decisions = readJson(decisionsPath);

  const approvedStaging = staging.map(record => ({
    ...record,
    review_status: 'APPROVED',
    reviewed_by: reviewer,
    reviewed_at: generatedAt,
    notes: [
      record.notes,
      formalApproval
        ? 'ユーザーがCodex会話で推奨判断どおり正式承認。本番反映は未実施'
        : '推奨判断によるローカルdry-run。正式承認・本番反映ではない'
    ].filter(Boolean).join('; ')
  }));

  const rows = {
    Companies: companies,
    Permits: permits,
    MLITPermits: mlitPermits,
    MasterImportStaging: approvedStaging
  };
  const context = buildContext(rows, decisions, generatedAt);
  const prepared = context.prepareCanonicalMasterMigration_();
  context.assertSystemOnlyDecisions_(
    prepared.systemOnlyIds,
    prepared.systemOnlyDecisions
  );

  const initialIds = new Set([
    ...companyIdsFrom(companies),
    ...companyIdsFrom(permits),
    ...companyIdsFrom(mlitPermits)
  ]);
  const canonicalById = new Map(
    prepared.canonical.map(company => [company.company_id, company])
  );
  const mappings = approvedStaging.map(record => {
    let finalCompany;
    const matchedId = String(record.matched_company_id || '').trim();
    if (matchedId) {
      finalCompany = canonicalById.get(matchedId);
    } else {
      finalCompany = resolveNewCompany(record, prepared.canonical, initialIds);
    }
    if (!finalCompany) {
      throw new Error(`final company not found for source_row=${record.source_row}`);
    }
    const classification = String(record.classification || '').toUpperCase();
    const action = classification === 'NEW_FROM_EXCEL'
      ? 'ALLOCATE_NEW'
      : (classification === 'REVIEW_REQUIRED'
        ? 'MATCH_REVIEW_RECOMMENDED'
        : 'KEEP_EXISTING');
    return {
      source_row: record.source_row,
      vendor_no: record.vendor_no,
      company_name_raw: record.company_name_raw,
      classification,
      previous_matched_company_id: matchedId,
      final_company_id: finalCompany.company_id,
      action,
      resulting_status: finalCompany.status
    };
  });

  const systemDecisionRows = systemOnly.map(record => {
    const companyId = record.company_id;
    const company = canonicalById.get(companyId);
    return {
      company_id: companyId,
      company_names: record.company_names,
      sources: record.sources,
      decision: decisions[companyId] || '',
      resulting_status: company ? company.status : '',
      reference_preserved: company ? 'TRUE' : 'FALSE'
    };
  });

  const canonicalIds = prepared.canonical.map(company => company.company_id);
  const vendorNos = prepared.canonical
    .map(company => String(company.vendor_no || '').trim())
    .filter(Boolean);
  const newIds = canonicalIds
    .filter(companyId => !initialIds.has(companyId))
    .sort((left, right) => numericCompanyId(left) - numericCompanyId(right));
  const checks = {
    staging_count_127: approvedStaging.length === 127,
    mapping_count_127: mappings.length === 127,
    canonical_company_id_unique: new Set(canonicalIds).size === canonicalIds.length,
    vendor_no_unique: new Set(vendorNos).size === vendorNos.length,
    orphan_company_id_zero: prepared.orphanCompanyIdCount === 0,
    system_only_decisions_complete:
      prepared.pendingSystemOnlyDecisionIds.length === 0,
    archived_records_inactive: systemDecisionRows
      .filter(record => record.decision === 'ARCHIVE_EXCLUDE')
      .every(record => record.resulting_status === 'INACTIVE'),
    kept_records_active: systemDecisionRows
      .filter(record => record.decision === 'KEEP_SYSTEM_ONLY')
      .every(record => record.resulting_status === 'ACTIVE')
  };
  if (Object.values(checks).some(value => !value)) {
    throw new Error(`dry-run validation failed: ${JSON.stringify(checks)}`);
  }

  const summary = {
    mode: formalApproval
      ? 'FORMAL_USER_APPROVAL_RECORDED_UAT_DRY_RUN'
      : 'RECOMMENDATION_SIMULATION_NOT_FORMAL_APPROVAL',
    generated_at: generatedAt,
    reviewed_by_marker: reviewer,
    approval_statement: approvalStatement,
    source_sha256: prepared.sourceSha256,
    input_hashes: Object.fromEntries(
      Object.entries(inputPaths).map(([key, filePath]) => [key, sha256File(filePath)])
    ),
    decisions_sha256: sha256File(decisionsPath),
    staging_count: prepared.stagingCount,
    existing_system_company_id_count: initialIds.size,
    canonical_count: prepared.canonicalCount,
    assigned_new_count: prepared.assignedNewCount,
    new_company_id_first: newIds[0] || '',
    new_company_id_last: newIds[newIds.length - 1] || '',
    active_count: prepared.activeCount,
    inactive_count: prepared.inactiveCount,
    vendor_no_count: prepared.vendorNoCount,
    orphan_company_id_count: prepared.orphanCompanyIdCount,
    system_only_ids: prepared.systemOnlyIds,
    system_only_decisions: prepared.systemOnlyDecisions,
    system_only_decision_summary: prepared.systemOnlyDecisionSummary,
    pending_system_only_decision_ids: prepared.pendingSystemOnlyDecisionIds,
    confirmation_required_after_formal_approval:
      `APPLY_CANONICAL_${prepared.sourceSha256.slice(0, 12)}`,
    formal_approval_ready: formalApproval,
    uat_staging_ready: formalApproval,
    production_apply_ready: false,
    checks
  };

  const canonicalHeaders = context.SECURE_COMPANIES_HEADERS_;
  const stagingHeaders = Object.keys(approvedStaging[0]);
  const mappingHeaders = Object.keys(mappings[0]);
  const decisionHeaders = Object.keys(systemDecisionRows[0]);
  fs.mkdirSync(outputDir, { recursive: true });
  atomicWrite(
    path.join(
      outputDir,
      formalApproval
        ? 'master_import_staging_approved.csv'
        : 'master_import_staging_recommended_simulation.csv'
    ),
    '\uFEFF' + toCsv(stagingHeaders, approvedStaging)
  );
  atomicWrite(
    path.join(outputDir, 'canonical_companies_candidate.csv'),
    '\uFEFF' + toCsv(canonicalHeaders, prepared.canonical)
  );
  atomicWrite(
    path.join(outputDir, 'migration_mapping.csv'),
    '\uFEFF' + toCsv(mappingHeaders, mappings)
  );
  atomicWrite(
    path.join(outputDir, 'system_only_decisions.csv'),
    '\uFEFF' + toCsv(decisionHeaders, systemDecisionRows)
  );
  atomicWrite(
    path.join(outputDir, 'canonical_migration_summary.json'),
    JSON.stringify(summary, null, 2) + '\n'
  );
  if (formalApproval) {
    const approvalRecord = {
      record_type: 'USER_FORMAL_APPROVAL',
      approved_at: generatedAt,
      approved_by_marker: reviewer,
      approval_channel: 'CODEX_CONVERSATION',
      approval_statement: approvalStatement,
      source_sha256: prepared.sourceSha256,
      scope: {
        automatic_classification_rows: 114,
        review_required_rows: 13,
        system_only_rows: 4
      },
      review_required_decision: 'APPROVE_MATCH',
      system_only_decisions: prepared.systemOnlyDecisions,
      production_changes_authorized_by_this_record: false,
      next_authorized_stage: 'UAT_REPRODUCTION'
    };
    atomicWrite(
      path.join(outputDir, 'formal_approval_record.json'),
      JSON.stringify(approvalRecord, null, 2) + '\n'
    );
  }

  const report = [
    formalApproval
      ? '# 会社マスタ正本化 正式承認済みUAT dry-run'
      : '# 会社マスタ正本化 推奨判断dry-run',
    '',
    `生成日時: ${generatedAt}`,
    '',
    '## 結論',
    '',
    formalApproval
      ? `- ユーザー指示「${approvalStatement}」を正式承認として記録した。`
      : '- この結果は推奨判断を使ったローカルシミュレーションであり、正式承認ではない。',
    '- Google Sheets、Apps Script、Script Properties、本番Companiesは変更していない。',
    `- 正本候補は${prepared.canonicalCount}社。ACTIVE ${prepared.activeCount}社、INACTIVE ${prepared.inactiveCount}社。`,
    `- Excel由来の新規採番は${prepared.assignedNewCount}社（${newIds[0]}〜${newIds[newIds.length - 1]}）。`,
    `- 業者番号あり${prepared.vendorNoCount}社、重複0、Permit/MLIT孤立参照0。`,
    '',
    '## SYSTEM_ONLYの推奨判断',
    '',
    '| company_id | 判断 | 移行後status |',
    '|---|---|---|',
    ...systemDecisionRows.map(record => (
      `| ${record.company_id} | ${record.decision} | ${record.resulting_status} |`
    )),
    '',
    formalApproval ? '## UAT・本番反映前の残件' : '## 本番反映前の残件',
    '',
    ...(formalApproval ? [
      '- 確定staging CSVと承認記録は作成済み。',
    ] : [
      '- 運用責任者が17件と自動分類114件を正式承認する。',
      '- 承認者・承認日時を入れた確定staging CSVを作る。',
    ]),
    '- UAT複製環境で同じdry-run結果を再現する。',
    '- 24時間以内のバックアップ成功後にのみ確認文字列を設定する。',
    '',
    '## 機械検証',
    '',
    ...Object.entries(checks).map(([key, value]) => (
      `- ${key}: ${value ? 'PASS' : 'FAIL'}`
    )),
    ''
  ].join('\n');
  atomicWrite(path.join(outputDir, 'canonical_migration_report.md'), report);

  console.log(JSON.stringify(summary, null, 2));
}

main();
