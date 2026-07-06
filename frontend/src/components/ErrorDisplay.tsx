import { ApiError } from '../api/client';

interface Props {
  error: ApiError;
  onDismiss: () => void;
}

/** 错误态展示——API 不可用、run 失败、artifact 缺失、REJECT 状态的统一错误展示。
 *
 * 不会误写为 REVIEW_READY 或上线批准——仅显示结构化错误信息。
 */
export function ErrorDisplay({ error, onDismiss }: Props) {
  // 根据错误码确定展示样式
  const isReject = error.error_code.includes('REJECT') ||
    error.error_code === 'VALIDATION_ERROR' ||
    error.error_code.startsWith('PIPELINE_') ||
    error.message.includes('阻断');

  const isNotFound = error.error_code === 'NOT_FOUND' ||
    error.error_code === 'PACKAGE_FETCH_FAILED';

  const isApiUnavailable = error.error_code === 'NETWORK_ERROR' ||
    error.error_code === 'PARSE_ERROR';

  const isPipelineError = error.error_code.startsWith('PIPELINE_');

  const borderColor = isReject ? 'var(--red)' :
    isNotFound ? 'var(--yellow)' :
    isApiUnavailable ? 'var(--red)' : 'var(--border)';

  return (
    <div className="error-display" style={{ borderColor }} data-testid="error-display">
      <div className="error-content">
        <div className="error-code">
          [{error.error_code}]
          {isReject && ' — 管线阻断'}
          {isPipelineError && error.field_ref && ` — ${error.field_ref} 阶段失败`}
          {isNotFound && ' — 资源不存在'}
          {isApiUnavailable && ' — API 不可用'}
        </div>
        <div className="error-message">{error.message}</div>
        {error.field_ref && (
          <div className="error-field">相关字段: {error.field_ref}</div>
        )}
        {isApiUnavailable && (
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
            请确认 API 服务已启动（uvicorn tianshu_datadev.api.app:create_app --reload）
          </div>
        )}
      </div>
      <button className="error-dismiss" onClick={onDismiss} title="关闭">
        ✕
      </button>
    </div>
  );
}
