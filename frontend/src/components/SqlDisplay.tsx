import { ExecutionTraceSummary, ResultSummarySummary } from '../api/client';

interface Props {
  sql: string;
  sqlSha256: string;
  compilerVersion: string;
  trace: ExecutionTraceSummary;
  summary: ResultSummarySummary;
  visible: boolean;
}

/** SQL / SqlProgram 逐语句展示面板——含 SQL 文本、执行追踪和结果摘要 */
export function SqlDisplay({ sql, sqlSha256, compilerVersion, trace, summary, visible }: Props) {
  if (!visible) return null;

  return (
    <div className="panel">
      <div className="panel-header">
        <h3>📜 生成的 SQL</h3>
        <span className="dry-run-notice">确定性编译 · dry_run 执行</span>
      </div>

      {/* SQL 文本 */}
      <pre className="sql-block">{sql}</pre>

      {/* SQL 元信息 */}
      <div className="sql-meta">
        <span>Compiler: {compilerVersion}</span>
        <span>SHA-256: {sqlSha256.substring(0, 16)}...</span>
      </div>

      {/* 执行追踪 */}
      <div className="section-title">📊 执行追踪</div>
      <div className="exec-result">
        <div className="exec-stat">
          <div className="ex-label">状态</div>
          <div className="ex-value" style={{
            color: trace.status === 'RUNTIME_PASS' ? 'var(--green)' :
                   trace.status === 'RUNTIME_FAIL' ? 'var(--red)' : 'var(--text-muted)'
          }}>
            {trace.status}
          </div>
        </div>
        <div className="exec-stat">
          <div className="ex-label">返回行数</div>
          <div className="ex-value">{trace.row_count}</div>
        </div>
        <div className="exec-stat">
          <div className="ex-label">执行耗时</div>
          <div className="ex-value">{trace.execution_time_ms.toFixed(1)} ms</div>
        </div>
        {trace.error_message && (
          <div className="exec-stat" style={{ gridColumn: '1 / -1' }}>
            <div className="ex-label" style={{ color: 'var(--red)' }}>错误信息</div>
            <div className="ex-value" style={{ fontSize: 12 }}>{trace.error_message}</div>
          </div>
        )}
      </div>

      {/* 结果摘要 */}
      {summary.columns.length > 0 && (
        <>
          <div className="section-title">📋 结果摘要</div>
          <div className="exec-result">
            <div className="exec-stat">
              <div className="ex-label">输出列</div>
              <div className="ex-value" style={{ fontSize: 11 }}>
                {summary.columns.join(', ')}
              </div>
            </div>
            <div className="exec-stat">
              <div className="ex-label">列类型</div>
              <div className="ex-value" style={{ fontSize: 11 }}>
                {summary.column_types.join(', ')}
              </div>
            </div>
          </div>
          {/* NULL 计数 */}
          {Object.keys(summary.null_counts).length > 0 && (
            <div className="exec-result">
              {Object.entries(summary.null_counts).map(([col, count]) => (
                <div key={col} className="exec-stat">
                  <div className="ex-label">{col} NULL</div>
                  <div className="ex-value">{count}</div>
                </div>
              ))}
            </div>
          )}
          {/* 数值求和 */}
          {Object.keys(summary.numeric_sums).length > 0 && (
            <div className="exec-result">
              {Object.entries(summary.numeric_sums).map(([col, val]) => (
                <div key={col} className="exec-stat">
                  <div className="ex-label">{col} SUM</div>
                  <div className="ex-value">{val.toLocaleString()}</div>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}
