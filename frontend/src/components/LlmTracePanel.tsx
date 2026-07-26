import { useMemo, useState } from 'react';
import { LlmTraceNode } from '../api/client';
import './LlmTracePanel.css';

interface TraceNodeDefinition {
  key: string;
  label: string;
}

interface TraceGroupDefinition {
  key: 'sql' | 'spark';
  label: string;
  nodes: TraceNodeDefinition[];
}

const TRACE_GROUPS: TraceGroupDefinition[] = [
  {
    key: 'sql',
    label: 'SQL 管线',
    nodes: [
      { key: 'requirement_planner', label: '需求规划' },
      { key: 'spec_enricher', label: 'Spec 增强' },
      { key: 'relationship_planner', label: '关系规划' },
      { key: 'label_extractor', label: '标签提取' },
    ],
  },
  {
    key: 'spark',
    label: 'Spark 管线',
    nodes: [
      { key: 'spark_developer', label: '语义标注' },
    ],
  },
];

interface Props {
  traces: Record<string, LlmTraceNode> | null | undefined;
  visible: boolean;
}

function isInvoked(trace: LlmTraceNode | undefined): boolean {
  return Boolean(trace && trace.status !== 'skipped' && trace.model !== 'deterministic');
}

function formatDuration(latencyMs: number): string {
  if (latencyMs <= 0) return '-';
  if (latencyMs < 1000) return `${latencyMs} ms`;
  return `${(latencyMs / 1000).toFixed(1)} s`;
}

function statusMeta(trace: LlmTraceNode | undefined): {
  label: string;
  className: string;
} {
  if (!trace) return { label: '未触发', className: 'idle' };
  if (trace.model === 'deterministic') return { label: '未调用', className: 'idle' };
  if (trace.status === 'valid') return { label: '成功', className: 'success' };
  if (trace.status === 'invalid') return { label: '校验失败', className: 'error' };
  if (trace.status === 'error') return { label: '调用失败', className: 'error' };
  return { label: '未调用', className: 'idle' };
}

export function LlmTracePanel({ traces, visible }: Props) {
  const [expanded, setExpanded] = useState(false);
  const traceMap = traces ?? {};

  const summary = useMemo(() => {
    const actualTraces = TRACE_GROUPS
      .flatMap((group) => group.nodes)
      .map((node) => traceMap[node.key])
      .filter((trace): trace is LlmTraceNode => isInvoked(trace));

    return actualTraces.reduce(
      (result, trace) => {
        result.calls += 1;
        result.promptTokens += trace.token_usage?.prompt_tokens || 0;
        result.completionTokens += trace.token_usage?.completion_tokens || 0;
        result.totalTokens += trace.token_usage?.total_tokens || 0;
        result.latencyMs += trace.latency_ms || 0;
        return result;
      },
      {
        calls: 0,
        promptTokens: 0,
        completionTokens: 0,
        totalTokens: 0,
        latencyMs: 0,
      },
    );
  }, [traceMap]);

  if (!visible) return null;

  return (
    <div className="llm-trace-panel">
      <button
        type="button"
        className="llm-trace-header"
        aria-expanded={expanded}
        onClick={() => setExpanded(!expanded)}
      >
        <span className={`llm-trace-chevron ${expanded ? 'expanded' : ''}`}>▶</span>
        LLM 调用追踪
        <span className="llm-trace-badge">
          {summary.calls > 0 ? `${summary.calls} 次调用` : '无实际调用'}
        </span>
      </button>

      {expanded && (
        <div className="llm-trace-body">
          <div className="llm-trace-summary">
            <span className="llm-trace-summary-item">
              <span className="label">Prompt</span>
              <span className="value">{summary.promptTokens || '-'}</span>
            </span>
            <span className="llm-trace-summary-item">
              <span className="label">Completion</span>
              <span className="value">{summary.completionTokens || '-'}</span>
            </span>
            <span className="llm-trace-summary-item">
              <span className="label">总 Token</span>
              <span className="value">{summary.totalTokens || '-'}</span>
            </span>
            <span className="llm-trace-summary-item">
              <span className="label">LLM 总耗时</span>
              <span className="value">{formatDuration(summary.latencyMs)}</span>
            </span>
          </div>

          <div className="llm-trace-groups">
            {TRACE_GROUPS.map((group) => {
              const invokedCount = group.nodes.filter((node) => (
                isInvoked(traceMap[node.key])
              )).length;

              return (
                <section
                  className="llm-trace-group"
                  data-pipeline={group.key}
                  key={group.key}
                >
                  <div className="llm-trace-group-header">
                    <span>{group.label}</span>
                    <span>{invokedCount}/{group.nodes.length} 已调用</span>
                  </div>
                  <div className="llm-trace-node-list">
                    {group.nodes.map((node) => {
                      const trace = traceMap[node.key];
                      const status = statusMeta(trace);
                      const model = trace?.model === 'deterministic' ? '-' : trace?.model || '-';

                      return (
                        <div className="llm-trace-node" key={node.key}>
                          <div className="llm-trace-node-title">
                            <span className={`llm-trace-status-dot ${status.className}`} />
                            <span className="llm-trace-node-label">{node.label}</span>
                            <span className={`llm-trace-status ${status.className}`}>
                              {status.label}
                            </span>
                          </div>
                          <dl className="llm-trace-node-details">
                            <div>
                              <dt>模型</dt>
                              <dd>{model}</dd>
                            </div>
                            <div>
                              <dt>耗时</dt>
                              <dd>{formatDuration(trace?.latency_ms || 0)}</dd>
                            </div>
                            <div>
                              <dt>Token</dt>
                              <dd>{trace?.token_usage?.total_tokens || '-'}</dd>
                            </div>
                          </dl>
                          {trace?.error_type && (
                            <div className="llm-trace-error">错误类型: {trace.error_type}</div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </section>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
