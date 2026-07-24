import { useState, useMemo } from 'react';
import { LlmTraceNode } from '../api/client';
import './LlmTracePanel.css';

/** 节点名 → 中文映射（完整 LLM 节点列表） */
const LLM_NODES: { key: string; label: string }[] = [
  { key: 'requirement_planner', label: '需求规划' },
  { key: 'spec_enricher', label: 'Spec 增强' },
  { key: 'relationship_planner', label: '关系规划' },
  { key: 'label_extractor', label: '标签提取' },
  { key: 'spark_developer', label: 'Spark 标注' },
];

/** 旧路径确定性节点——向后兼容 */
const DETERMINISTIC_NODES: { key: string; label: string }[] = [
  { key: 'parse_developer_spec', label: 'Spec 解析' },
  { key: 'sql_build_planner', label: 'SQL Plan 构建' },
  { key: 'sql_program_planner', label: 'SQL 程序生成' },
];

/** 按顺序合并所有节点，去重 */
function getOrderedNodes(traces: Record<string, LlmTraceNode>): { key: string; label: string }[] {
  const seen = new Set<string>();
  const result: { key: string; label: string }[] = [];
  // 先遍历 LLM 节点
  for (const n of LLM_NODES) {
    if (n.key in traces) {
      result.push(n);
      seen.add(n.key);
    }
  }
  // 再遍历确定性节点
  for (const n of DETERMINISTIC_NODES) {
    if (n.key in traces && !seen.has(n.key)) {
      result.push(n);
      seen.add(n.key);
    }
  }
  // 未识别的节点追加末尾
  for (const key of Object.keys(traces)) {
    if (!seen.has(key)) {
      result.push({ key, label: key });
      seen.add(key);
    }
  }
  return result;
}

interface Props {
  traces: Record<string, LlmTraceNode> | null | undefined;
  visible: boolean;
}

export function LlmTracePanel({ traces, visible }: Props) {
  const [expanded, setExpanded] = useState(false);

  const orderedNodes = useMemo(() => {
    if (!traces) return [];
    return getOrderedNodes(traces);
  }, [traces]);

  // 不显示时返回 null
  if (!visible) return null;

  // 无数据时显示精简提示面板
  if (orderedNodes.length === 0) {
    return (
      <div className="llm-trace-panel">
        <div className="llm-trace-header" onClick={() => setExpanded(!expanded)}>
          <span className={`llm-trace-chevron ${expanded ? 'expanded' : ''}`}>▶</span>
          LLM 调用追踪
          <span className="llm-trace-badge">无数据</span>
        </div>
        {expanded && (
          <div className="llm-trace-empty">
            暂无 LLM 调用数据——当前为确定性运行模式（Fake Adapter），未产生真实 LLM 调用。
          </div>
        )}
      </div>
    );
  }

  // 计算汇总
  let totalPrompt = 0;
  let totalCompletion = 0;
  let totalTokens = 0;
  let totalLatency = 0;
  for (const [name, trace] of Object.entries(traces!)) {
    if (trace.token_usage) {
      totalPrompt += trace.token_usage.prompt_tokens || 0;
      totalCompletion += trace.token_usage.completion_tokens || 0;
      totalTokens += trace.token_usage.total_tokens || 0;
    }
    totalLatency += trace.latency_ms || 0;
  }

  /** 指示灯颜色 */
  function statusColor(status: string): string {
    if (status === 'valid') return 'var(--success, #22c55e)';
    if (status === 'invalid' || status === 'error') return 'var(--error, #ef4444)';
    return 'var(--text-faint, #555)';
  }

  /** 状态提示文字 */
  function statusHint(status: string): string {
    if (status === 'valid') return '调用成功';
    if (status === 'invalid') return '校验失败';
    if (status === 'error') return '调用异常';
    return '已跳过';
  }

  return (
    <div className="llm-trace-panel">
      <div className="llm-trace-header" onClick={() => setExpanded(!expanded)}>
        <span className={`llm-trace-chevron ${expanded ? 'expanded' : ''}`}>▶</span>
        LLM 调用追踪
        <span className="llm-trace-badge">{orderedNodes.length} 节点</span>
      </div>

      {expanded && (
        <div className="llm-trace-body">
          {/* 汇总行 */}
          <div className="llm-trace-summary">
            <span className="llm-trace-summary-item">
              <span className="label">Prompt:</span>
              <span className="value">{totalPrompt || '-'}</span>
            </span>
            <span className="llm-trace-summary-item">
              <span className="label">Completion:</span>
              <span className="value">{totalCompletion || '-'}</span>
            </span>
            <span className="llm-trace-summary-item">
              <span className="label">总 Token:</span>
              <span className="value">{totalTokens || '-'}</span>
            </span>
            <span className="llm-trace-summary-item">
              <span className="label">总耗时:</span>
              <span className="value">{totalLatency > 0 ? `${totalLatency}ms` : '-'}</span>
            </span>
          </div>

          {/* 指示灯表格：两行——灯 + 详情 */}
          <div className="llm-trace-lights">
            {/* 行 1：指示灯 */}
            <div className="llm-trace-lights-row">
              {orderedNodes.map(({ key, label }) => {
                const trace = traces![key];
                return (
                  <div
                    key={key}
                    className="llm-trace-light-cell"
                    title={`${label} — ${statusHint(trace.status)}`}
                  >
                    <div className="llm-trace-light-wrapper">
                      <span
                        className="llm-trace-light-dot"
                        style={{ background: statusColor(trace.status) }}
                      />
                      <span className="llm-trace-light-label">{label}</span>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* 行 2：LLM 调用详情 */}
            <div className="llm-trace-lights-row">
              {orderedNodes.map(({ key }) => {
                const trace = traces![key];
                return (
                  <div key={key} className="llm-trace-light-cell">
                    <div className="llm-trace-detail">
                      <span className="llm-trace-detail-item">
                        模型: {trace.model || '-'}
                      </span>
                      {trace.token_usage && trace.token_usage.total_tokens ? (
                        <span className="llm-trace-detail-item">
                          Token: {trace.token_usage.total_tokens}
                        </span>
                      ) : null}
                      <span className="llm-trace-detail-item">
                        耗时: {trace.latency_ms > 0 ? `${trace.latency_ms}ms` : '-'}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
