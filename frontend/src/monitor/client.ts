/**
 * 浏览器端监控客户端——轻量采集，零第三方依赖。
 *
 * 采集范围（白名单）：
 *   - fetch /api/* 耗时和 HTTP 状态（排除 /api/monitor/*）
 *   - window.onerror 运行时异常
 *   - unhandledrejection Promise 异常
 *
 * 安全约束：
 *   - 禁止记录：请求正文、响应正文、Authorization Header、Cookie、数据样本
 *   - monitor_token 校验：后端返回的 token 与请求中携带的一致
 *   - 上报失败静默处理——不影响页面正常功能
 *   - 域名白名单：仅上报到同源（window.location.origin）
 */

// ── 类型定义 ──

interface MonitorConfig {
  enabled: boolean;
  run_id?: string;
  monitor_token?: string;
}

interface MonitorPayload {
  event_type: 'api_call' | 'js_error' | 'promise_rejection';
  timestamp: string;          // ISO 8601 带时区
  run_id: string;
  monitor_token: string;
  // api_call 专用
  api_path?: string;          // 仅路径，不含查询参数
  api_status?: number;
  api_duration_ms?: number;
  // js_error / promise_rejection 专用
  error_type?: string;        // Error 构造函数名
  error_message?: string;     // 仅 message，不含 stack
  stack_frames?: string[];    // 仅前 5 帧，过滤掉 browser extension 和 node_modules
}

// ── 内部状态 ──

/** 启动保护——防止 initMonitor 被多次调用导致重复注册 */
let initialized = false;

/** 当前生效的监控配置（enabled=true 时赋值） */
let config: MonitorConfig | null = null;

/** 保存原始 fetch 引用，避免包装后的 fetch 造成递归 */
const nativeFetch = window.fetch;

/**
 * 保存 event listener 引用，以便将来可能的 removeEventListener。
 * 使用 addEventListener 而非属性赋值，避免覆盖页面上已有的错误处理程序。
 */
const _handlers: {
  onerror?: (event: ErrorEvent) => void;
  onrejection?: (event: PromiseRejectionEvent) => void;
} = {};

// ── 内部工具函数 ──

/** 生成带本地时区偏移的 ISO 8601 时间戳（如 2026-07-10T14:30:22.123+08:00） */
function toLocalISOString(date: Date): string {
  const offset = -date.getTimezoneOffset();
  const sign = offset >= 0 ? '+' : '-';
  const pad = (n: number) => String(Math.floor(Math.abs(n))).padStart(2, '0');
  return (
    date.getFullYear() + '-' +
    pad(date.getMonth() + 1) + '-' +
    pad(date.getDate()) + 'T' +
    pad(date.getHours()) + ':' +
    pad(date.getMinutes()) + ':' +
    pad(date.getSeconds()) + '.' +
    String(date.getMilliseconds()).padStart(3, '0') +
    sign + pad(offset / 60) + ':' + pad(offset % 60)
  );
}

/** 过滤 stack trace 中的无关帧（browser extension 和 node_modules） */
function filterStackFrames(stack: string): string[] {
  if (!stack) return [];
  const frames: string[] = [];
  for (const line of stack.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    // 跳过 browser extension 来源的帧
    if (
      trimmed.includes('chrome-extension://') ||
      trimmed.includes('moz-extension://') ||
      trimmed.includes('node_modules')
    ) {
      continue;
    }
    frames.push(trimmed);
    if (frames.length >= 5) break;
  }
  return frames;
}

/** 从 URL 中提取路径部分（不含查询参数） */
function extractPath(url: string): string {
  try {
    return new URL(url, window.location.origin).pathname;
  } catch {
    return url;
  }
}

/** 判断是否为监控自身端点（不应当被采集） */
function isMonitorApiPath(path: string): boolean {
  return path.startsWith('/api/monitor/');
}

/** 判断是否为业务 API 路径 */
function isApiPath(path: string): boolean {
  return path.startsWith('/api/');
}

// ── 内部上报函数 ──

/**
 * 发送监控事件到后端。
 * 使用原始 fetch 引用以避免递归，keepalive: true 确保页面关闭时也能发送。
 * 失败时静默处理——不写 console.error，避免错误上报循环。
 */
function sendEvent(payload: MonitorPayload): void {
  nativeFetch('/api/monitor/browser-event', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    keepalive: true,
  }).catch(() => {
    // 静默失败——不影响页面正常功能
  });
}

// ── 事件采集器注册 ──

/**
 * 注册 fetch 包装器——拦截 /api/* 请求并记录耗时与状态。
 * 排除 /api/monitor/* 请求以避免循环上报。
 */
function registerFetchWrapper(): void {
  const originalFetch = window.fetch;

  window.fetch = function (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const startTime = performance.now();

    // 提取请求 URL
    const url = typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.href
        : input.url;
    const path = extractPath(url);

    // 只关注 /api/* 路径，排除监控自身路径
    if (!isApiPath(path) || isMonitorApiPath(path)) {
      return originalFetch.call(window, input, init);
    }

    // 记录耗时，在请求完成后上报
    return originalFetch.call(window, input, init).then(
      (response) => {
        const duration = Math.round(performance.now() - startTime);
        if (config) {
          sendEvent({
            event_type: 'api_call',
            timestamp: toLocalISOString(new Date()),
            run_id: config.run_id || '',
            monitor_token: config.monitor_token || '',
            api_path: path,
            api_status: response.status,
            api_duration_ms: duration,
          });
        }
        return response;
      },
      (error) => {
        const duration = Math.round(performance.now() - startTime);
        if (config) {
          sendEvent({
            event_type: 'api_call',
            timestamp: toLocalISOString(new Date()),
            run_id: config.run_id || '',
            monitor_token: config.monitor_token || '',
            api_path: path,
            api_status: 0, // 网络错误时状态码为 0
            api_duration_ms: duration,
          });
        }
        throw error; // 保留原始错误传播链
      },
    );
  };
}

/**
 * 注册 error 事件监听器——捕获运行时异常。
 * 使用 addEventListener 而非 window.onerror 属性赋值，
 * 避免覆盖页面上已有的错误处理程序（如其他监控库、React error boundary 全局回退等）。
 */
function registerOnError(): void {
  _handlers.onerror = (event: ErrorEvent) => {
    if (!config) return;

    // ErrorEvent 属性：.message、.filename、.lineno、.colno、.error
    const msg = event.message || String(event);
    const errObj: Error | undefined = event.error;
    const frames = errObj?.stack ? filterStackFrames(errObj.stack) : [];

    sendEvent({
      event_type: 'js_error',
      timestamp: toLocalISOString(new Date()),
      run_id: config.run_id || '',
      monitor_token: config.monitor_token || '',
      error_type: errObj?.constructor?.name || 'Error',
      error_message: errObj?.message || msg,
      stack_frames: frames.length > 0 ? frames : undefined,
    });

    // 不调用 preventDefault——让浏览器默认错误处理也运行
  };
  window.addEventListener('error', _handlers.onerror);
}

/**
 * 注册 unhandledrejection 事件监听器——捕获未处理 Promise 拒绝。
 * 使用 addEventListener 而非 window.onunhandledrejection 属性赋值，
 * 避免覆盖页面上已有的处理程序。
 */
function registerOnUnhandledRejection(): void {
  _handlers.onrejection = (event: PromiseRejectionEvent) => {
    if (!config) return;

    const reason = event.reason;
    let errorMessage = '';
    let errorType = 'PromiseRejection';
    let frames: string[] | undefined;

    if (reason instanceof Error) {
      errorType = reason.constructor?.name || 'Error';
      errorMessage = reason.message;
      frames = reason.stack ? filterStackFrames(reason.stack) : undefined;
    } else if (typeof reason === 'string') {
      errorMessage = reason;
    } else if (reason && typeof reason === 'object') {
      // 尝试提取 message 属性，兜底用 JSON 序列化
      errorMessage = (reason as Record<string, unknown>).message as string || String(reason);
    } else {
      errorMessage = String(reason);
    }

    sendEvent({
      event_type: 'promise_rejection',
      timestamp: toLocalISOString(new Date()),
      run_id: config.run_id || '',
      monitor_token: config.monitor_token || '',
      error_type: errorType,
      error_message: errorMessage,
      stack_frames: frames && frames.length > 0 ? frames : undefined,
    });

    // 不调用 preventDefault——让浏览器默认处理也运行
  };
  window.addEventListener('unhandledrejection', _handlers.onrejection);
}

// ── 公开 API ──

/**
 * 初始化监控客户端。
 *
 * 流程：
 * 1. GET /api/monitor/config
 * 2. 若 enabled=false → 直接返回（零开销——不注册任何全局监听器）
 * 3. 若 enabled=true → 保存 config，注册三个全局采集器：
 *    a. fetch 包装——拦截所有 /api/* 请求（排除 /api/monitor/*）
 *    b. error 事件监听——捕获运行时异常（使用 addEventListener，不覆盖已有处理程序）
 *    c. unhandledrejection 事件监听——捕获未处理 Promise 拒绝（使用 addEventListener）
 * 4. 所有上报请求携带 monitor_token 用于后端校验
 *
 * 安全：initMonitor 可被多次调用，仅首次生效。
 */
export async function initMonitor(): Promise<void> {
  // 防止重复初始化
  if (initialized) return;
  initialized = true;

  try {
    const response = await nativeFetch('/api/monitor/config');
    if (!response.ok) {
      // 配置端点不可用，静默返回
      return;
    }
    const cfg: MonitorConfig = await response.json();

    if (!cfg.enabled) {
      // 监控未启用——零开销，不注册任何监听器
      return;
    }

    config = cfg;

    // 注册三个全局采集器
    registerFetchWrapper();
    registerOnError();
    registerOnUnhandledRejection();
  } catch {
    // 网络错误、超时等——不影响页面正常渲染
  }
}

/**
 * 手动上报事件（供将来扩展使用）。
 * 内部调用 sendEvent()。
 * 仅当监控已启用（initMonitor 成功）时才执行上报。
 */
export function reportEvent(
  payload: Omit<MonitorPayload, 'run_id' | 'monitor_token' | 'timestamp'>,
): void {
  if (!config || !config.enabled) return;

  sendEvent({
    ...payload,
    timestamp: toLocalISOString(new Date()),
    run_id: config.run_id || '',
    monitor_token: config.monitor_token || '',
  });
}
