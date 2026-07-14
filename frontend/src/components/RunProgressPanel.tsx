import { FullRunEvent } from '../api/client';
import './RunProgressPanel.css';

interface Props {
  /** 累积的进度事件列表 */
  events: FullRunEvent[];
  /** 是否正在接收流数据 */
  isStreaming: boolean;
  /** 流错误（连接中断等） */
  streamError: string | null;
  /** 是否可见 */
  visible: boolean;
}

/** 状态 → 图标映射 */
function statusIcon(status: string): string {
  switch (status) {
    case 'completed': return '✅';
    case 'failed': return '❌';
    case 'skipped': return '⏭️';
    case 'started': return '🔄';
    default: return '⬜';
  }
}

/** 阶段名 → 中文显示名 */
function stageLabel(stage: string): string {
  const labels: Record<string, string> = {
    // SQL 管线短名（pipeline_stages 中的名称）
    parser: '解析', enrich: '增强', build: '构建', validate: '校验',
    compile: '编译', execute: '执行', contract: '契约', package: '打包',
    // SQL 管线长名（collector.stage() 真实节点名——TeeCollector 实时事件使用）
    sql_parser: '解析', sql_enricher: '增强', sql_builder: 'SQL 构建',
    sql_validator: 'SQL 校验', sql_compiler: 'SQL 编译', sql_executor: 'SQL 执行',
    contract_extractor: '契约提取', snapshot_builder: '快照', packager: '打包',
    // Spark 管线
    MAPPER: '映射', DEVELOPER: '标注', COMPILER: '编译',
    VALIDATOR: '校验', COMPARATOR: '对比', PHYSICAL_VERIFIER: '物理验证',
  };
  return labels[stage] || stage;
}

/** 将事件列表聚合为以 pipeline:stage 为 key 的最新状态映射 */
function aggregateStages(events: FullRunEvent[]): Map<string, FullRunEvent> {
  const stages = new Map<string, FullRunEvent>();
  for (const e of events) {
    if (e.event === 'stage') {
      stages.set(`${e.pipeline}:${e.stage}`, e);
    }
  }
  return stages;
}

/** Run-All 实时进度面板 */
export function RunProgressPanel({ events, isStreaming, streamError, visible }: Props) {
  if (!visible) return null;

  const stages = aggregateStages(events);
  const sqlStages = Array.from(stages.values()).filter(
    (e): e is FullRunEvent & { event: 'stage' } => e.event === 'stage' && e.pipeline === 'sql'
  );
  const sparkStages = Array.from(stages.values()).filter(
    (e): e is FullRunEvent & { event: 'stage' } => e.event === 'stage' && e.pipeline === 'spark'
  );

  // 检查是否有完成/致命事件
  const hasDone = events.some(e => e.event === 'done');
  const hasFatal = events.some(e => e.event === 'fatal');

  return (
    <div className={`run-progress-panel panel${hasFatal ? ' progress-fatal' : ''}${hasDone ? ' progress-done' : ''}`}>
      <div className="panel-header">
        <h3>
          📡 执行进度
          {isStreaming && <span className="streaming-badge">接收中...</span>}
          {hasDone && <span className="done-badge">✅ 完成</span>}
          {hasFatal && <span className="fatal-badge">❌ 致命错误</span>}
        </h3>
      </div>

      {/* 连接中断提示 */}
      {streamError && !isStreaming && !hasDone && (
        <div className="progress-stream-error">
          ⚠️ 连接中断，结果可能仍在后台执行——{streamError}
        </div>
      )}

      {/* SQL 管线 */}
      {sqlStages.length > 0 && (
        <div className="progress-pipeline-group">
          <div className="progress-pipeline-header">🟢 SQL 管线</div>
          <div className="progress-stage-list">
            {sqlStages.map((e) => (
              <div key={`${e.pipeline}:${e.stage}`} className={`progress-stage-row stage-${e.status}`}>
                <span className="progress-stage-icon">{statusIcon(e.status)}</span>
                <span className="progress-stage-name">{stageLabel(e.stage)}</span>
                {e.duration_ms != null && (
                  <span className="progress-stage-duration">{(e.duration_ms / 1000).toFixed(1)}s</span>
                )}
                {e.message && (
                  <span className="progress-stage-message" title={e.message}>
                    {e.message.length > 80 ? e.message.slice(0, 80) + '…' : e.message}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Spark 管线 */}
      {sparkStages.length > 0 && (
        <div className="progress-pipeline-group">
          <div className="progress-pipeline-header">🐍 Spark 管线</div>
          <div className="progress-stage-list">
            {sparkStages.map((e) => (
              <div key={`${e.pipeline}:${e.stage}`} className={`progress-stage-row stage-${e.status}`}>
                <span className="progress-stage-icon">{statusIcon(e.status)}</span>
                <span className="progress-stage-name">{stageLabel(e.stage)}</span>
                {e.duration_ms != null && (
                  <span className="progress-stage-duration">{(e.duration_ms / 1000).toFixed(1)}s</span>
                )}
                {e.message && (
                  <span className="progress-stage-message" title={e.message}>
                    {e.message.length > 80 ? e.message.slice(0, 80) + '…' : e.message}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 等待开始时提示 */}
      {events.length === 0 && isStreaming && (
        <div className="progress-waiting">⏳ 等待管线启动...</div>
      )}

      {/* 致命错误展示 */}
      {hasFatal && (
        <div className="progress-fatal-error">
          {(() => {
            const fatal = events.find(e => e.event === 'fatal');
            if (fatal && fatal.event === 'fatal') {
              return <><strong>[{fatal.error_code}]</strong> {fatal.message}</>;
            }
            return null;
          })()}
        </div>
      )}
    </div>
  );
}
