import { useState, useEffect, useRef } from 'react';

/** 流水线阶段状态 */
export interface StageInfo {
  stage: string;
  status: 'ok' | 'failed' | 'skipped';
  error_type?: string;
  error_message?: string;
}

/** 流水线错误信息 */
export interface PipelineError {
  stage: string;
  error_type: string;
  error_message: string;
}

interface Props {
  stages: StageInfo[];
  error: PipelineError | null;
  /** 指示灯标题——默认"流水线阶段"，Spark 侧传入"Spark 管线" */
  title?: string;
  /** Phase 9C: E2E 测试定位——根元素 data-testid 属性 */
  testId?: string;
}

/** 阶段英文 → 中文映射 */
const STAGE_CN: Record<string, string> = {
  // SQL 侧（已有）
  parser: '解析',
  enrich: '增强',
  build: '构建',
  validate: '验证',
  compile: '编译',
  execute: '执行',
  contract: '契约',   // SQL 侧新增——契约阶段
  package: '打包',    // SQL 侧新增——打包阶段
  // Spark 侧（新增）
  MAPPER: '映射',
  DEVELOPER: '标注',
  COMPILER: '编译',
  VALIDATOR: '校验',
  COMPARATOR: '对比',
  PHYSICAL_VERIFIER: '物理验证',
};

/** 状态 → 图标映射 */
function stageIcon(status: string): string {
  switch (status) {
    case 'ok': return '✅';
    case 'failed': return '❌';
    case 'skipped': return '⏭️';
    default: return '⬜';
  }
}

/** 流水线阶段指示灯——管线星图风格。
 *
 * 折叠时以管线/星轨形式展示各阶段状态，始终可见。
 * 点击展开详情下拉。 */
export function PipelineStageIndicator({ stages, error, title, testId }: Props) {
  const [expanded, setExpanded] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // 点击外部关闭
  useEffect(() => {
    if (!expanded) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setExpanded(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [expanded]);

  // 无数据时不渲染
  if (stages.length === 0 && !error) return null;

  // 计算摘要信息
  const failedStage = stages.find(s => s.status === 'failed');
  const hasFailure = !!failedStage;
  const allDone = stages.length > 0 && stages.every(s => s.status === 'ok' || s.status === 'skipped');
  const allOk = stages.length > 0 && stages.every(s => s.status === 'ok');
  const failedName = failedStage ? (STAGE_CN[failedStage.stage] || failedStage.stage) : '';

  // 星轨状态：找出当前执行中的阶段（last non-ok/non-skipped）
  const lastCompletedIdx = stages.reduce((max, s, i) =>
    (s.status === 'ok' || s.status === 'skipped') ? i : max, -1);
  const currentStageIdx = lastCompletedIdx + 1 < stages.length ? lastCompletedIdx + 1 : -1;

  const dotClass = hasFailure ? 'dot-error' : allDone ? 'dot-ok' : 'dot-loading';
  const summaryText = hasFailure ? `${failedName}失败` : allOk ? '全部成功' : allDone ? '已完成' : '处理中';

  return (
    <div className="pipeline-indicator" ref={ref} data-testid={testId}>
      <button
        className="pipeline-trigger"
        onClick={() => setExpanded(!expanded)}
        title="点击查看流水线阶段详情"
      >
        {/* 管线星轨——常驻可见 */}
        <span className="pipeline-star-chart">
          {stages.map((s, i) => (
            <span key={s.stage} className="pipeline-star-wrapper">
              {i > 0 && <span className={`pipeline-star-connector ${i <= lastCompletedIdx + 1 && !hasFailure ? 'connector-done' : ''}`} />}
              <span
                className={
                  `pipeline-star ${s.status === 'ok' ? 'star-ok' : ''}` +
                  `${s.status === 'failed' ? ' star-failed' : ''}` +
                  `${s.status === 'skipped' ? ' star-skipped' : ''}` +
                  `${i === currentStageIdx && s.status !== 'ok' && s.status !== 'failed' && s.status !== 'skipped' ? ' star-current' : ''}` +
                  `${i > lastCompletedIdx + 1 || (i > lastCompletedIdx && hasFailure) ? ' star-pending' : ''}`
                }
                title={STAGE_CN[s.stage] || s.stage}
              />
            </span>
          ))}
        </span>
        <span className={`status-dot ${dotClass}`} />
        <span className="pipeline-summary-text">{summaryText}</span>
        <span className="pipeline-chevron">{expanded ? '▴' : '▾'}</span>
      </button>

      {expanded && (
        <div className="pipeline-dropdown" data-testid="stage-list">
          <div className="pipeline-dropdown-header">{title || '流水线阶段'}</div>
          {stages.map((s) => (
            <div
              key={s.stage}
              className={`pipeline-stage-row ${s.status === 'failed' ? 'stage-failed' : s.status === 'skipped' ? 'stage-skipped' : ''}`}
            >
              <span className="stage-icon">{stageIcon(s.status)}</span>
              <span className="stage-name">{STAGE_CN[s.stage] || s.stage}</span>
              {s.status === 'failed' && s.error_type && (
                <span className="stage-error-type">{s.error_type}</span>
              )}
            </div>
          ))}
          {error && (
            <div className="pipeline-error-detail">
              <div className="error-detail-label">错误详情</div>
              <div className="error-detail-text">{error.error_message}</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
