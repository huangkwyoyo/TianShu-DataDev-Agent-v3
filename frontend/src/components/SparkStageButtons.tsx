import { useMemo, useState } from 'react';
import {
  runSparkStage,
  SparkStageItem,
  SparkStageResponse,
  ApiError,
} from '../api/client';
import './SparkStageButtons.css';

/** 阶段名 → 中文映射 */
const STAGE_CN: Record<string, string> = {
  MAPPER: '映射',
  DEVELOPER: '标注',
  COMPILER: '编译',
  VALIDATOR: '校验',
  COMPARATOR: '对比',
  PHYSICAL_VERIFIER: '物理验证',
};

/** 阶段 slug → 枚举名映射 */
const SLUG_TO_STAGE: Record<string, string> = {
  map: 'MAPPER',
  develop: 'DEVELOPER',
  compile: 'COMPILER',
  validate: 'VALIDATOR',
  compare: 'COMPARATOR',
  'physical-verify': 'PHYSICAL_VERIFIER',
};

/** 枚举名 → slug 映射 */
const STAGE_TO_SLUG: Record<string, string> = {
  MAPPER: 'map',
  DEVELOPER: 'develop',
  COMPILER: 'compile',
  VALIDATOR: 'validate',
  COMPARATOR: 'compare',
  PHYSICAL_VERIFIER: 'physical-verify',
};

/** 执行顺序 */
const STAGE_ORDER = ['MAPPER', 'DEVELOPER', 'COMPILER', 'VALIDATOR', 'COMPARATOR', 'PHYSICAL_VERIFIER'];

/** 计算哪些阶段可在当前状态下触发 */
function computeAvailableStages(stages: SparkStageItem[]): Set<string> {
  const available = new Set<string>();
  const statusMap: Record<string, string> = {};
  for (const s of stages) {
    statusMap[s.stage] = s.status;
  }

  // MAPPER 始终可用（依赖 contract——由 execute-rich 确保）
  available.add('MAPPER');

  // MAPPER 成功后 DEVELOPER 和 COMPILER 可用
  if (statusMap['MAPPER'] === 'ok') {
    available.add('DEVELOPER');
    available.add('COMPILER');
  }

  // COMPILER 成功后 VALIDATOR 可用
  if (statusMap['COMPILER'] === 'ok') {
    available.add('VALIDATOR');
  }

  // MAPPER 成功后 COMPARATOR 可用（额外需要 sql_plan——execute-rich 已确保）
  if (statusMap['MAPPER'] === 'ok') {
    available.add('COMPARATOR');
  }

  // COMPILER 成功后 PHYSICAL_VERIFIER 可用
  if (statusMap['COMPILER'] === 'ok') {
    available.add('PHYSICAL_VERIFIER');
  }

  return available;
}

/** 状态图标 */
function stageIcon(status: string): string {
  switch (status) {
    case 'ok': return '✅';
    case 'failed': return '❌';
    case 'skipped': return '⏭️';
    default: return '⬜';
  }
}

interface Props {
  requestId: string | null;
  artifactsReady: boolean;  // artifacts 是否就绪（execute-rich 成功后为 true）
  stages: SparkStageItem[];
  onStageComplete: (response: SparkStageResponse) => void;
  onError: (error: ApiError) => void;
  disabled: boolean;  // 顶层禁用（如 isLoading）
}

export function SparkStageButtons({ requestId, artifactsReady, stages, onStageComplete, onError, disabled }: Props) {
  const [loadingStage, setLoadingStage] = useState<string | null>(null);

  const available = useMemo(() => computeAvailableStages(stages), [stages]);

  const statusMap: Record<string, string> = {};
  for (const s of stages) {
    statusMap[s.stage] = s.status;
  }

  const handleClick = async (stageEnum: string) => {
    if (!requestId || !artifactsReady || disabled || loadingStage) return;
    const slug = STAGE_TO_SLUG[stageEnum];
    setLoadingStage(stageEnum);
    try {
      const result = await runSparkStage(requestId, slug);
      onStageComplete(result);
    } catch (err) {
      const apiErr: ApiError =
        err && typeof err === 'object' && 'error_code' in err
          ? (err as ApiError)
          : { error_code: 'NETWORK_ERROR', message: String(err), field_ref: null };
      onError(apiErr);
    } finally {
      setLoadingStage(null);
    }
  };

  return (
    <div className="spark-stage-buttons">
      {STAGE_ORDER.map((stageEnum) => {
        const status = statusMap[stageEnum] || 'none';
        const isAvailable = available.has(stageEnum);
        const isLoading = loadingStage === stageEnum;
        const cn = STAGE_CN[stageEnum] || stageEnum;

        // artifacts 未就绪时全部按钮不可用
        const hasRequest = !!requestId && artifactsReady;
        const isDisabled = !hasRequest || !isAvailable || disabled || !!loadingStage;
        const tooltip = !requestId
          ? '请先执行"编译执行"获取 request_id'
          : !artifactsReady
            ? '请先执行"编译执行"生成 Contract 等基础产物'
            : isAvailable
              ? `执行 ${cn} 阶段`
              : `${cn}：缺少前置产物`;

        return (
          <button
            key={stageEnum}
            className={`spark-stage-btn status-${status === 'ok' ? 'ok' : status === 'failed' ? 'failed' : status === 'skipped' ? 'skipped' : 'none'}`}
            disabled={isDisabled}
            onClick={() => handleClick(stageEnum)}
            title={tooltip}
          >
            <span className="stage-icon">
              {isLoading ? '⏳' : stageIcon(status)}
            </span>
            <span className="stage-label">{cn}</span>
          </button>
        );
      })}
    </div>
  );
}
