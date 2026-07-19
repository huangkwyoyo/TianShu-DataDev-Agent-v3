import { useState } from 'react';
import { SparkStageResult } from '../api/client';
import './SparkStageResultPanel.css';

interface Props {
  stage: string;
  result: SparkStageResult;
  status: string;
  visible: boolean;
}

/** 阶段名 → 中文映射 */
const STAGE_CN: Record<string, string> = {
  MAPPER: '映射',
  DEVELOPER: '标注',
  COMPILER: '编译',
  VALIDATOR: '校验',
  COMPARATOR: '对比',
  PHYSICAL_VERIFIER: '物理验证',
};

/** 对比结论 → 中文映射 */
const VERDICT_CN: Record<string, string> = {
  'LOGIC_EQUIVALENT': '逻辑等价',
  'LOGIC_MISMATCH': '逻辑不等价',
  'NOT_EXECUTED': '未执行',
  'NOT_COVERED': '未覆盖',
  'EQUIVALENT': '等价',
  'NOT_EQUIVALENT': '不等价',
  'UNSUPPORTED_COMPARISON': '不支持对比',
  'RESULT_CONSISTENT': '结果一致',
  'SAMPLED_CONSISTENT': '抽样一致',
  'RESULT_MISMATCH': '结果不一致',
};

/** 状态图标 */
function statusIcon(status: string): string {
  switch (status) {
    case 'ok': return '✅';
    case 'failed': return '❌';
    case 'skipped': return '⏭️';
    default: return '⬜';
  }
}

/** Spark 阶段结果面板——展示每个阶段执行后的具体产物 */
export function SparkStageResultPanel({ stage, result, status, visible }: Props) {
  if (!visible) return null;

  const stageCn = STAGE_CN[stage] || stage;
  // 编译阶段：切换「编译产物」/「独立运行脚本」
  const [compilerTab, setCompilerTab] = useState<'annotated' | 'standalone'>(
    result.standalone_pyspark ? 'standalone' : 'annotated'
  );
  // 代码复制按钮状态
  const [codeCopied, setCodeCopied] = useState(false);
  // 面板折叠
  const [collapsed, setCollapsed] = useState(false);

  const handleCopyCode = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCodeCopied(true);
      setTimeout(() => setCodeCopied(false), 1800);
    } catch { /* 剪贴板不可用 */ }
  };

  return (
    <div className={`spark-stage-result panel ${status === 'skipped' ? 'stage-skipped' : ''}`}>
      <div className="panel-header" onClick={() => setCollapsed(!collapsed)}>
        <h3>
          {statusIcon(status)} Spark {stageCn}
          <span className="spark-stage-id">
            {stage}
          </span>
          {result.skipped && <span className="skipped-badge">已跳过</span>}
        </h3>
        <span className={`panel-collapse-arrow${collapsed ? ' collapsed' : ''}`}>
          ▼
        </span>
      </div>

      <div className={`panel-body-content${collapsed ? ' is-collapsed' : ''}`}>

      {/* 失败态 */}
      {status === 'failed' && (
        <div className="spark-result-errors">
          <p className="error-title">阶段执行失败</p>
          {result.errors && result.errors.length > 0 && (
            <ul className="error-list">
              {result.errors.map((e, i) => <li key={i}>{e}</li>)}
            </ul>
          )}
        </div>
      )}

      {/* 跳过态——所有阶段共用（PHYSICAL_VERIFIER 有自己的 skipped 展示区，不重复渲染） */}
      {result.skipped && result.message && stage !== 'PHYSICAL_VERIFIER' && (
        <div className="spark-result-skipped">
          ⏭️ {result.message}
        </div>
      )}

      {/* DEVELOPER——标注结果表格（Phase 8） */}
      {result.type === 'developer' && !result.skipped && status === 'ok' && (
        <>
          <div className="section-title">🏷️ LLM 语义标注结果</div>
          <div className="spark-plan-summary">
            <span className="stat-label">标注步骤数</span>
            <span className="stat-value">{result.annotation_count}</span>
          </div>
          {result.annotations && result.annotations.length > 0 ? (
            <table className="spark-step-table">
              <thead>
                <tr>
                  <th>步骤 ID</th>
                  <th>意图分类</th>
                  <th>业务意图</th>
                  <th>操作描述</th>
                </tr>
              </thead>
              <tbody>
                {result.annotations.map((a, i) => (
                  <tr key={i}>
                    <td><code className="step-type-badge">{a.step_id}</code></td>
                    <td><span className="intent-badge">{a.intent}</span></td>
                    <td className="step-desc">{a.intent_detail}</td>
                    <td className="step-desc">{a.operation_summary}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="spark-result-message">标注结果为空</div>
          )}
          {result.warnings && result.warnings.length > 0 && (
            <div className="spark-result-errors">
              <p className="error-title">⚠️ 标注警告</p>
              <ul className="error-list">
                {result.warnings.map((w, i) => (
                  <li key={i}>[{w.severity}] {w.description}</li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}

      {/* DEVELOPER——失败态展示（Phase 8） */}
      {result.type === 'developer' && status === 'failed' && (
        <div className="spark-result-errors">
          <p className="error-title">❌ LLM 语义标注失败</p>
          {result.message && <p className="spark-result-note">{result.message}</p>}
        </div>
      )}

      {/* MAPPER——SparkPlan 步骤列表 */}
      {result.type === 'mapper' && status === 'ok' && result.steps && (
        <>
          <div className="section-title">📋 SparkPlan 步骤</div>
          <div className="spark-plan-summary">
            <span className="stat-label">步骤数</span>
            <span className="stat-value">{result.step_count}</span>
            {result.plan_id && (
              <>
                <span className="stat-label" style={{ marginLeft: 16 }}>Plan ID</span>
                <span className="stat-value" style={{ fontFamily: 'monospace', fontSize: 11 }}>
                  {result.plan_id}
                </span>
              </>
            )}
          </div>
          <table className="spark-step-table">
            <thead>
              <tr>
                <th>#</th>
                <th>步骤类型</th>
                <th>描述</th>
              </tr>
            </thead>
            <tbody>
              {result.steps.map((s, i) => (
                <tr key={i}>
                  <td className="step-idx">{i + 1}</td>
                  <td><code className="step-type-badge">{s.step_type}</code></td>
                  <td className="step-desc">{s.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {/* COMPILER——PySpark 代码展示（带标签切换 + 复制按钮） */}
      {result.type === 'compiler' && status === 'ok' && (
        <>
          <div className="section-title">🐍 PySpark DSL 代码（最终产物）</div>
          <div className="spark-plan-summary">
            <span className="stat-label">步骤数</span>
            <span className="stat-value">{result.step_count}</span>
            {result.raw_hash && (
              <>
                <span className="stat-label" style={{ marginLeft: 16 }}>代码哈希</span>
                <span className="stat-value" style={{ fontFamily: 'monospace', fontSize: 11 }} title={result.raw_hash}>
                  {result.raw_hash.slice(0, 16)}…
                </span>
              </>
            )}
          </div>

          {/* 标签切换：编译产物 / 独立运行脚本 */}
          {result.standalone_pyspark && (
            <div className="spark-code-tabs">
              <div
                className={`spark-code-tab${compilerTab === 'annotated' ? ' active' : ''}`}
                onClick={() => setCompilerTab('annotated')}
              >
                编译产物
              </div>
              <div
                className={`spark-code-tab${compilerTab === 'standalone' ? ' active' : ''}`}
                onClick={() => setCompilerTab('standalone')}
              >
                独立运行脚本
              </div>
            </div>
          )}

          <div className="code-block-wrapper">
            <div className="code-block-header">
              <span className="code-block-title">PySpark</span>
              <button
                className={`btn-copy${codeCopied ? ' copied' : ''}`}
                onClick={() => {
                  const text = compilerTab === 'standalone' && result.standalone_pyspark
                    ? result.standalone_pyspark
                    : (result.pyspark_code || '');
                  handleCopyCode(text);
                }}
              >
                {codeCopied ? '✅ 已复制' : '📋 复制'}
              </button>
            </div>
            <pre className="pyspark-code-block"><code>
              {compilerTab === 'standalone' && result.standalone_pyspark
                ? result.standalone_pyspark
                : result.pyspark_code}
            </code></pre>
          </div>
        </>
      )}

      {/* VALIDATOR——校验结果 */}
      {result.type === 'validator' && status === 'ok' && (
        <>
          <div className="section-title">🔒 安全校验</div>
          <div className={`validator-badge ${result.is_valid ? 'valid' : 'invalid'}`}>
            {result.is_valid ? '✅ 校验通过' : '❌ 校验未通过'}
          </div>
          {result.errors && result.errors.length > 0 && (
            <ul className="error-list">
              {result.errors.map((e, i) => <li key={i}>{e}</li>)}
            </ul>
          )}
          {result.is_valid && (
            <p className="spark-result-note">PySpark DSL 安全校验全部通过，无阻断项。</p>
          )}
        </>
      )}

      {/* COMPARATOR——逻辑对比报告 */}
      {result.type === 'comparator' && status === 'ok' && (
        <>
          <div className="section-title">🔗 SQL ↔ Spark 逻辑对比</div>
          <div className="spark-plan-summary">
            <span className="stat-label">对比状态</span>
            <span className="stat-value">{VERDICT_CN[result.status || ''] || result.status}</span>
          </div>
          {result.step_results && result.step_results.length > 0 && (
            <table className="spark-step-table">
              <thead>
                <tr>
                  <th>步骤类型</th>
                  <th>对比结论</th>
                </tr>
              </thead>
              <tbody>
                {result.step_results.map((r, i) => (
                  <tr key={i}>
                    <td><code className="step-type-badge">{r.step_type}</code></td>
                    <td>
                      <span className={`verdict-badge ${r.verdict === 'LOGIC_EQUIVALENT' ? 'equiv' : 'mismatch'}`}>
                        {VERDICT_CN[r.verdict] || r.verdict}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {result.unsupported_types && result.unsupported_types.length > 0 && (
            <p className="spark-result-note">
              ⚠️ 不支持对比的类型：{result.unsupported_types.join(', ')}
            </p>
          )}
          {result.uncovered_step_types && result.uncovered_step_types.length > 0 && (
            <p className="spark-result-note">
              📋 未覆盖的类型：{result.uncovered_step_types.join(', ')}
            </p>
          )}
        </>
      )}

      {/* PHYSICAL_VERIFIER——物理验证结果（含 skipped 原因展示） */}
      {result.type === 'physical_verify' && (
        <div className={`physver-section${result.skipped ? ' stage-skipped' : ''}`}>
          {/* 跳过态 */}
          {result.skipped && (
            <div className="physver-header">
              <span className="physver-status-icon">⏭️</span>
              <span className="physver-brief">物理验证已跳过</span>
            </div>
          )}

          {/* 执行态（ok 或 failed） */}
          {!result.skipped && (
            <>
              {/* 标题行：状态 + 验证结论 */}
              <div className="physver-header">
                <span className="physver-status-icon">
                  {result.status === 'failed' ? '❌' : '✅'}
                </span>
                <span className="physver-brief">
                  {result.status === 'failed'
                    ? '物理验证未通过'
                    : result.verification_status === 'SAMPLED_CONSISTENT'
                      ? '物理验证通过（抽样一致）'
                      : result.verification_status === 'RESULT_CONSISTENT'
                        ? '物理验证通过（全量一致）'
                        : result.verification_status
                          ? `物理验证通过（${result.verification_status}）`
                          : '物理验证通过'}
                </span>
              </div>

              {/* 双引擎行数与耗时对比 */}
              <div className="physver-metrics">
                <span className="physver-metric">
                  DuckDB 行数：{result.duckdb_row_count ?? '-'}
                </span>
                <span className="physver-metric" style={{ marginLeft: 12 }}>
                  Spark 行数：{result.spark_row_count ?? '-'}
                </span>
                {result.duckdb_time_ms !== undefined && result.duckdb_time_ms !== null && (
                  <span className="physver-metric" style={{ marginLeft: 12 }}>
                    DuckDB {result.duckdb_time_ms.toFixed(0)}ms
                  </span>
                )}
                {result.spark_time_ms !== undefined && result.spark_time_ms !== null && (
                  <span className="physver-metric" style={{ marginLeft: 12 }}>
                    Spark {result.spark_time_ms.toFixed(0)}ms
                  </span>
                )}
              </div>

              {/* 对比项——行数 / Schema / 差异 */}
              <div className="physver-metrics">
                {result.row_count_match !== undefined && (
                  <span className="physver-metric">
                    行数一致：{result.row_count_match ? '✅' : '❌'}
                  </span>
                )}
                {result.schema_match !== undefined && (
                  <span className="physver-metric" style={{ marginLeft: 12 }}>
                    Schema 一致：{result.schema_match ? '✅' : '❌'}
                  </span>
                )}
                {result.total_diff_count !== undefined && result.total_diff_count > 0 && (
                  <span className="physver-metric" style={{ marginLeft: 12 }}>
                    差异数：{result.total_diff_count}
                  </span>
                )}
                {result.total_diff_count === 0 && result.row_count_match && result.schema_match && (
                  <span className="physver-metric" style={{ marginLeft: 12 }}>
                    差异数：0
                  </span>
                )}
              </div>

              {/* 溢出降级说明 */}
              {result.message && (
                <div className="physver-message">{result.message}</div>
              )}

              {/* 抽样行对比——DuckDB vs Spark 双栏并排 */}
              {result.sample_rows && (
                (result.sample_rows.duckdb.length > 0 || result.sample_rows.spark.length > 0) && (
                  <details className="physver-details">
                    <summary className="physver-details-summary">
                      抽样行对比（DuckDB {result.sample_rows.duckdb.length} 行 / Spark {result.sample_rows.spark.length} 行）
                    </summary>
                    <div className="physver-diff-table-wrap">
                      <table className="physver-diff-table">
                        <thead>
                          <tr>
                            <th>引擎</th>
                            <th>#</th>
                            {result.sample_rows.duckdb.length > 0
                              && Object.keys(result.sample_rows.duckdb[0]).map((k) => (
                                <th key={k}>{k}</th>
                              ))
                            }
                          </tr>
                        </thead>
                        <tbody>
                          {result.sample_rows.duckdb.map((row, i) => (
                            <tr key={`ddb-${i}`}>
                              <td className="diff-val duckdb">DuckDB</td>
                              <td className="diff-row-idx">{i + 1}</td>
                              {Object.values(row).map((v, j) => (
                                <td key={j} className="diff-val duckdb">{String(v)}</td>
                              ))}
                            </tr>
                          ))}
                          {result.sample_rows.spark.map((row, i) => (
                            <tr key={`spk-${i}`}>
                              <td className="diff-val spark">Spark</td>
                              <td className="diff-row-idx">{i + 1}</td>
                              {Object.values(row).map((v, j) => (
                                <td key={j} className="diff-val spark">{String(v)}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </details>
                )
              )}

              {/* 差异详情——逐行展示 DuckDB vs Spark（仅失败时） */}
              {result.diffs && result.diffs.length > 0 && (
                <details className="physver-details">
                  <summary className="physver-details-summary">
                    差异明细 ({result.diffs.length} 条{(result.total_diff_count ?? 0) > result.diffs.length ? `，共 ${result.total_diff_count} 条` : ''})
                  </summary>
                  <div className="physver-diff-table-wrap">
                    <table className="physver-diff-table">
                      <thead>
                        <tr>
                          <th>行</th>
                          <th>列</th>
                          <th>DuckDB</th>
                          <th>Spark</th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.diffs.map((d, i) => (
                          <tr key={i}>
                            <td className="diff-row-idx">{d.row_index ?? '-'}</td>
                            <td className="diff-col">{d.column}</td>
                            <td className="diff-val duckdb">{String(d.duckdb_value)}</td>
                            <td className="diff-val spark">{String(d.spark_value)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              )}

              {/* 详细错误（折叠） */}
              {result.errors && result.errors.length > 1 && (
                <details className="physver-details">
                  <summary className="physver-details-summary">
                    详细错误 ({result.errors.length - 1} 条)
                  </summary>
                  <ul className="physver-error-list">
                    {result.errors.slice(1).map((e, i) => (
                      <li key={i}>{e}</li>
                    ))}
                  </ul>
                </details>
              )}
            </>
          )}

          {/* 跳过态的 message 和错误 */}
          {result.skipped && result.message && (
            <div className="physver-message">{result.message}</div>
          )}
          {result.skipped && result.errors && result.errors.length > 0 && (
            <ul className="physver-error-list">
              {result.errors.map((e, i) => <li key={i}>{e}</li>)}
            </ul>
          )}
        </div>
      )}
      </div>{/* panel-body-content */}
    </div>
  );
}
