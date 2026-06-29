import { SpecRichResponse } from '../api/client';

interface Props {
  spec: SpecRichResponse;
  visible: boolean;
}

/** 结构化解析预览面板——展示表、指标、维度、Join、时间范围、输出规格 */
export function ParsePreview({ spec, visible }: Props) {
  if (!visible) return null;

  return (
    <div className="parse-preview panel">
      <div className="panel-header">
        <h3>🔍 解析预览</h3>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          {spec.spec_id}
        </span>
      </div>

      {/* 概览统计 */}
      <div className="spec-summary">
        <div className="stat-item">
          <span className="stat-label">标题</span>
          <span className="stat-value">{spec.title}</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">表数量</span>
          <span className="stat-value">{spec.tables.length}</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">指标</span>
          <span className="stat-value">{spec.metrics.length}</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">维度</span>
          <span className="stat-value">{spec.dimensions.length}</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">Join 声明</span>
          <span className="stat-value">{spec.joins.length}</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">时间范围</span>
          <span className="stat-value">{spec.time_range ? '✓' : '未指定'}</span>
        </div>
      </div>

      {/* 表声明 */}
      <div className="section-title">📊 源表声明</div>
      {spec.tables.map((t) => (
        <div key={t.table_alias} className="table-card">
          <div className="tbl-name">
            {t.table_alias} → {t.source_table}
            {t.role && (
              <span style={{ fontSize: 10, marginLeft: 6, color: 'var(--text-muted)' }}>
                [{t.role}]
              </span>
            )}
          </div>
          <div className="tbl-meta">
            列数: {t.column_count}
            {t.row_count != null && ` | 预估行数: ~${t.row_count.toLocaleString()}`}
            {t.has_time_field && ' | 含时间字段'}
            {t.has_partition && ' | 含分区字段'}
          </div>
        </div>
      ))}

      {/* 指标 */}
      {spec.metrics.length > 0 && (
        <>
          <div className="section-title">📈 指标</div>
          {spec.metrics.map((m, i) => (
            <div key={i} className="metric-item">
              <strong>{m.alias}</strong>: {m.aggregation}({m.input_column || '*'}) — {m.metric_name}
            </div>
          ))}
        </>
      )}

      {/* 维度 */}
      {spec.dimensions.length > 0 && (
        <>
          <div className="section-title">📐 维度</div>
          {spec.dimensions.map((d, i) => (
            <div key={i} className="dim-item">
              <strong>{d.dimension_name}</strong> → {d.column_ref}
            </div>
          ))}
        </>
      )}

      {/* Join 声明 */}
      {spec.joins.length > 0 && (
        <>
          <div className="section-title">🔗 Join 声明</div>
          {spec.joins.map((j, i) => (
            <div key={i} className="join-item">
              {j.left_table}.{j.left_key} = {j.right_table}.{j.right_key}
              <span style={{ marginLeft: 6, fontSize: 10, color: 'var(--text-muted)' }}>
                [{j.join_type}]
              </span>
            </div>
          ))}
        </>
      )}

      {/* 时间范围 */}
      {spec.time_range && (
        <>
          <div className="section-title">⏱️ 时间范围</div>
          <div className="metric-item">
            {spec.time_range.column_ref}: [{spec.time_range.start}, {spec.time_range.end}]
            {spec.time_range.inclusive ? ' (含边界)' : ' (不含边界)'}
          </div>
        </>
      )}

      {/* 输出规格 */}
      <div className="section-title">📤 输出规格</div>
      <div className="join-item">
        列: {spec.output_spec.columns.join(', ')}
      </div>
      <div className="join-item">
        粒度: {spec.output_spec.grain.join(', ')}
      </div>
      {spec.output_spec.sort_columns.length > 0 && (
        <div className="join-item">
          排序: {spec.output_spec.sort_columns.join(', ')}
        </div>
      )}
      {spec.output_spec.limit != null && (
        <div className="join-item">
          限制: {spec.output_spec.limit} 行
        </div>
      )}
    </div>
  );
}
