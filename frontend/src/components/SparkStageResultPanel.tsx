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

  return (
    <div className={`spark-stage-result panel ${status === 'skipped' ? 'stage-skipped' : ''}`}>
      <div className="panel-header">
        <h3>
          {statusIcon(status)} Spark {stageCn}
          <span className="spark-stage-id">
            {stage}
          </span>
          {result.skipped && <span className="skipped-badge">已跳过</span>}
        </h3>
      </div>

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

      {/* 跳过态——所有阶段共用 */}
      {result.skipped && result.message && (
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

      {/* COMPILER——PySpark 代码展示（带标签切换） */}
      {result.type === 'compiler' && status === 'ok' && (
        <>
          <div className="section-title">🐍 PySpark DSL 代码（最终产物）</div>
          <div className="spark-plan-summary">
            <span className="stat-label">步骤数</span>
            <span className="stat-value">{result.step_count}</span>
            {result.raw_hash && (
              <>
                <span className="stat-label" style={{ marginLeft: 16 }}>代码哈希</span>
                <span className="stat-value" style={{ fontFamily: 'monospace', fontSize: 11 }}>
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

          <pre className="pyspark-code-block"><code>
            {compilerTab === 'standalone' && result.standalone_pyspark
              ? result.standalone_pyspark
              : result.pyspark_code}
          </code></pre>
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
            <span className="stat-value">{result.status}</span>
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
                        {r.verdict}
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

      {/* PHYSICAL_VERIFIER——物理验证结果 */}
      {result.type === 'physical_verify' && !result.skipped && (
        <div className="spark-result-message">
          {result.message || '物理验证结果'}
        </div>
      )}
    </div>
  );
}
