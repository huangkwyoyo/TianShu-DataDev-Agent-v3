interface Props {
  value: string;
  onChange: (value: string) => void;
}

/** 从完整文本中提取内部内容（去除 ```markdown 包裹） */
function unwrap(text: string): string {
  // 匹配完整的 ```markdown ... ``` 包裹
  const fullMatch = text.match(/^```(?:markdown|md)\s*\r?\n([\s\S]*?)\r?\n?```\s*$/);
  if (fullMatch) return fullMatch[1];
  // 仅匹配开头的 ```markdown（用户正在编辑中）
  const startMatch = text.match(/^```(?:markdown|md)\s*\r?\n([\s\S]*)/);
  if (startMatch) return startMatch[1];
  // 无包裹，直接返回原文
  return text;
}

/** 给内容自动添加 ```markdown 包裹
 *
 * 防御性处理：先剥离可能残留的 fence 标记，再统一重新包裹，
 * 确保结果总是干净的单层 ```markdown ... ``` 格式。 */
function wrap(text: string): string {
  let inner = text;
  // 已有完整 ```markdown ... ``` 外层包裹 → 直接返回（避免重复）
  if (/^```(?:markdown|md)\s*\r?\n/.test(inner.trimStart()) &&
      /\n?```\s*$/.test(inner.trimEnd())) {
    return inner.trimEnd();
  }
  // 剥离残留的开头 fence（防止粘贴时带来的部分包裹）
  inner = inner.replace(/^```(?:markdown|md)?\s*\r?\n?/, '');
  // 剥离残留的尾部 fence
  inner = inner.replace(/\n?```\s*$/, '');
  return '```markdown\n' + inner + '\n```';
}

/** DeveloperSpec Markdown 编辑器
 *
 * textarea 仅展示内部正文，外层 ```markdown / ``` 以装饰性元素呈现。
 * onChange 自动添加外层包裹，确保提交到 API 的文本总是合法格式。 */
export function SpecEditor({ value, onChange }: Props) {
  const innerValue = unwrap(value);

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange(wrap(e.target.value));
  };

  return (
    <div className="spec-editor panel">
      <div className="panel-header">
        <h3>📝 DeveloperSpec 编辑器</h3>
        <span className="dry-run-notice">dry_run 模式</span>
      </div>
      <div className="editor-wrapper">
        <div className="editor-fence editor-fence-top">
          <span className="fence-marker">```</span>markdown
        </div>
        <textarea
          value={innerValue}
          onChange={handleChange}
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
        <div className="editor-fence editor-fence-bottom">
          <span className="fence-marker">```</span>
        </div>
      </div>
      <div className="editor-hint">
        Markdown + YAML front matter 格式 | 外层 ```markdown 包裹自动添加 | 点击左侧模板按钮加载预置示例
      </div>
    </div>
  );
}
