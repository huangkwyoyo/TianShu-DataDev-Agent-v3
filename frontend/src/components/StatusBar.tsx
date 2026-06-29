interface Props {
  requestId: string | null;
  isLoading: boolean;
  hasError: boolean;
}

/** 状态栏——显示 request_id、加载状态和错误指示 */
export function StatusBar({ requestId, isLoading, hasError }: Props) {
  const dotClass = hasError ? 'dot-error' : isLoading ? 'dot-loading' : 'dot-ok';

  return (
    <footer className="status-bar">
      <div className="status-left">
        <span className={`status-dot ${dotClass}`} />
        <span>
          {hasError ? '错误' : isLoading ? '处理中' : '就绪'}
        </span>
        {requestId && (
          <span style={{ fontFamily: 'monospace', fontSize: 10 }}>
            {requestId}
          </span>
        )}
      </div>
      <div className="status-right">
        <span>dry_run 模式 · 不做生产写入</span>
      </div>
    </footer>
  );
}
