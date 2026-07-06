import { useState, useMemo } from 'react';
import { LlmTraceNode } from '../api/client';
import './LlmTracePanel.css';

/** 节点名 → 中文映射 */
const NODE_CN: Record<string, string> = {
  parse_developer_spec: 'Spec 解析',
  relationship_planner: '关系规划',
  sql_build_planner: 'SQL Plan 构建',
  sql_program_planner: 'SQL 程序生成',
  spark_developer: 'Spark 标注',
};

interface Props {
  traces: Record<string, LlmTraceNode> | null | undefined;
  visible: boolean;
}

export function LlmTracePanel({ traces, visible }: Props) {
  const [expanded, setExpanded] = useState(false);

  const entries = useMemo(() => {
    if (!traces) return [];
    return Object.entries(traces);
  }, [traces]);

  // 不显示时返回 null
  if (!visible) return null;

  // 无数据时显示精简提示面板
  if (entries.length === 0) {
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
  const totalPrompt = entries.reduce((sum, [, t]) => sum + (t.token_usage?.prompt_tokens || 0), 0);
  const totalCompletion = entries.reduce((sum, [, t]) => sum + (t.token_usage?.completion_tokens || 0), 0);
  const totalTokens = entries.reduce((sum, [, t]) => sum + (t.token_usage?.total_tokens || 0), 0);
  const totalLatency = entries.reduce((sum, [, t]) => sum + (t.latency_ms || 0), 0);

  return (
    <div className="llm-trace-panel">
      <div className="llm-trace-header" onClick={() => setExpanded(!expanded)}>
        <span className={`llm-trace-chevron ${expanded ? 'expanded' : ''}`}>▶</span>
        LLM 调用追踪
        <span className="llm-trace-badge">{entries.length} 节点</span>
      </div>

      {expanded && (
        <table className="llm-trace-table">
          <thead>
            <tr>
              <th>节点名称</th>
              <th>模型</th>
              <th>Prompt Token</th>
              <th>Completion Token</th>
              <th>总 Token</th>
              <th>耗时 (ms)</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([name, trace]) => (
              <tr key={name} className={`status-${trace.status}`}>
                <td>{NODE_CN[name] || name}</td>
                <td>{trace.model || '-'}</td>
                <td>{trace.token_usage?.prompt_tokens ?? '-'}</td>
                <td>{trace.token_usage?.completion_tokens ?? '-'}</td>
                <td>{trace.token_usage?.total_tokens ?? '-'}</td>
                <td>{trace.latency_ms > 0 ? trace.latency_ms : '-'}</td>
                <td>{trace.status === 'valid' ? '✅' : trace.status === 'error' ? '❌' : trace.status === 'skipped' ? '⏭️' : trace.status}</td>
              </tr>
            ))}
            {/* 汇总行 */}
            <tr className="llm-trace-summary">
              <td>合计</td>
              <td>-</td>
              <td>{totalPrompt || '-'}</td>
              <td>{totalCompletion || '-'}</td>
              <td>{totalTokens || '-'}</td>
              <td>{totalLatency > 0 ? totalLatency : '-'}</td>
              <td>{entries.length} 次调用</td>
            </tr>
          </tbody>
        </table>
      )}
    </div>
  );
}
