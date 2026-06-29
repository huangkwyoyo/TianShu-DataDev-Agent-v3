interface Props {
  value: string;
  onChange: (value: string) => void;
}

/** DeveloperSpec Markdown 编辑器——纯文本 textarea */
export function SpecEditor({ value, onChange }: Props) {
  return (
    <div className="spec-editor panel">
      <div className="panel-header">
        <h3>📝 DeveloperSpec 编辑器</h3>
        <span className="dry-run-notice">dry_run 模式</span>
      </div>
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={`在此输入 DeveloperSpec Markdown...

格式示例：
---
spec:
  type: aggregate_table
  target_table: ads.metrics_daily
  ...

  source_tables:
    - name: dwd.user_events
      alias: ue
      ...

  metrics:
    - metric_name: pv
      aggregation: COUNT
      input_column: id
      alias: pv

  dimensions:
    - dimension_name: stat_date
      column_ref: stat_date

  output_columns:
    - name: stat_date
      type: date
    - name: pv
      type: bigint
---

# 标题

## 业务目标
描述业务需求...
`}
        spellCheck={false}
      />
      <div className="editor-hint">
        Markdown + YAML front matter 格式 | 点击左侧模板按钮加载预置示例
      </div>
    </div>
  );
}
