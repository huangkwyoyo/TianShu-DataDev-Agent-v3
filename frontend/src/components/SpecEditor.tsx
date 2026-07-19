import { useRef, useMemo, useCallback } from 'react';

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

const placeholder = `---
spec:
  type: aggregate_table
  target_table: ads.metrics_daily
  source_tables:
    - name: dwd.user_events
      alias: ue
      role: fact
      key_columns:
        - name: id
          type: varchar
      business_columns:
        - name: stat_date
          type: date
  output_columns:
    - name: stat_date
    - name: pv
---
# 每日事件量
按 stat_date 统计事件数量，输出 stat_date 和 pv。`;

/** DeveloperSpec Markdown 编辑器（含行号）
 *
 * textarea 仅展示内部正文，外层 ```markdown / ``` 以装饰性元素呈现。
 * 左侧行号动态跟随文本行数变化，滚动同步。 */
export function SpecEditor({ value, onChange }: Props) {
  const innerValue = unwrap(value);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);

  // 计算行号列表（空格处理——末尾空行也映射为行号）
  const lines = useMemo(() => {
    if (innerValue.length === 0) return [1];
    return innerValue.split('\n').map((_, i) => i + 1);
  }, [innerValue]);

  // 同步滚动——行号列跟随 textarea 滚动
  const handleScroll = useCallback(() => {
    if (textareaRef.current && gutterRef.current) {
      gutterRef.current.scrollTop = textareaRef.current.scrollTop;
    }
  }, []);

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange(wrap(e.target.value));
  };

  return (
    <div className="editor-container">
      <div className="editor-fence">
        <span className="prompt">$</span> markdown
      </div>
      <div className="spec-editor">
        <div className="editor-gutter" ref={gutterRef} aria-hidden="true">
          <div className="editor-gutter-inner">
            {lines.map(n => (
              <div key={n} className="editor-line-no">{n}</div>
            ))}
          </div>
        </div>
        <textarea
          ref={textareaRef}
          value={innerValue}
          onChange={handleChange}
          onScroll={handleScroll}
          placeholder={placeholder}
          spellCheck={false}
        />
      </div>
      <div className="editor-fence" style={{ borderBottom: 'none', borderTop: '1px solid var(--border)' }}>
        <span className="prompt" style={{ visibility: 'hidden' }}>$</span>
      </div>
    </div>
  );
}
