import { useState } from 'react';
import { SpecEditor } from './components/SpecEditor';
import { TemplateSelector } from './components/TemplateSelector';
import { ParsePreview } from './components/ParsePreview';
import { OpenQuestionPanel } from './components/OpenQuestionPanel';
import { JoinEvidencePanel } from './components/JoinEvidencePanel';
import { PlanStepsPanel } from './components/PlanStepsPanel';
import { SqlDisplay } from './components/SqlDisplay';
import { PackageTree } from './components/PackageTree';
import { ErrorDisplay } from './components/ErrorDisplay';
import { StatusBar } from './components/StatusBar';
import {
  PipelineStageIndicator,
  type StageInfo,
  type PipelineError,
} from './components/PipelineStageIndicator';
import {
  parseSpecRich,
  buildPlanRich,
  executeRich,
  runAll,
  getPackageRich,
  ApiError,
  SpecRichResponse,
  PlanRichResponse,
  ExecuteRichResponse,
  PackageRichResponse,
  TemplateFull,
} from './api/client';
import './App.css';

/** 工作台面板类型 */
type Panel = 'parse' | 'plan' | 'sql' | 'package';

/** 应用全局状态 */
interface AppState {
  markdownText: string;
  requestId: string | null;
  isLoading: boolean;
  error: ApiError | null;
  activePanel: Panel | null;

  // 流水线阶段状态（执行后更新）
  pipelineStages: StageInfo[];
  pipelineError: PipelineError | null;

  // 各阶段产物
  specResult: SpecRichResponse | null;
  planResult: PlanRichResponse | null;
  executeResult: ExecuteRichResponse | null;
  packageResult: PackageRichResponse | null;
}

export default function App() {
  const [state, setState] = useState<AppState>({
    markdownText: '',
    requestId: null,
    isLoading: false,
    error: null,
    activePanel: null,
    pipelineStages: [],
    pipelineError: null,
    specResult: null,
    planResult: null,
    executeResult: null,
    packageResult: null,
  });

  /** 更新部分状态 */
  const update = (partial: Partial<AppState>) =>
    setState((prev) => ({ ...prev, ...partial }));

  /** 加载模板到编辑器 */
  const handleLoadTemplate = (template: TemplateFull) => {
    const confirmed = state.markdownText
      ? window.confirm('加载模板将替换当前编辑器内容，是否继续？')
      : true;
    if (confirmed) {
      update({
        markdownText: template.markdown_template,
        error: null,
        specResult: null,
        planResult: null,
        executeResult: null,
        packageResult: null,
        activePanel: null,
      });
    }
  };

  /** 清除错误和流水线阶段状态 */
  const clearError = () => update({ error: null, pipelineError: null, pipelineStages: [] });

  /** 通用 API 调用包装——支持同步和异步回调。
   *  自动从响应中提取 pipeline_error / pipeline_stages 用于阶段指示灯。 */
  const runAction = async <T,>(
    fn: () => Promise<T>,
    onSuccess: (result: T) => Partial<AppState> | Promise<Partial<AppState>>,
  ) => {
    update({ isLoading: true, error: null });
    try {
      const result = await fn();
      // 提取流水线阶段信息（200 响应中可能包含 pipeline_error）
      const resultAny = result as Record<string, unknown>;
      const plError = (resultAny.pipeline_error as PipelineError | undefined) || null;
      const plStages = (resultAny.pipeline_stages as StageInfo[] | undefined) || [];
      const partial = await onSuccess(result);
      update({
        isLoading: false,
        ...partial,
        pipelineError: plError,
        pipelineStages: plStages,
      });
    } catch (err) {
      const apiErr: ApiError =
        err && typeof err === 'object' && 'error_code' in err
          ? (err as ApiError)
          : { error_code: 'NETWORK_ERROR', message: String(err), field_ref: null };
      update({ isLoading: false, error: apiErr });
    }
  };

  /** 解析 DeveloperSpec */
  const handleParse = () => {
    if (!state.markdownText.trim()) {
      update({ error: { error_code: 'EMPTY_INPUT', message: '请输入 DeveloperSpec 内容', field_ref: 'markdown_text' } });
      return;
    }
    runAction(
      () => parseSpecRich(state.markdownText),
      (result) => ({
        specResult: result,
        planResult: null,
        executeResult: null,
        packageResult: null,
        requestId: result.request_id,
        activePanel: 'parse',
      }),
    );
  };

  /** 构建 Plan */
  const handlePlan = () => {
    if (!state.markdownText.trim()) {
      update({ error: { error_code: 'EMPTY_INPUT', message: '请输入 DeveloperSpec 内容', field_ref: 'markdown_text' } });
      return;
    }
    runAction(
      () => buildPlanRich(state.markdownText),
      (result) => ({
        planResult: result,
        executeResult: null,
        packageResult: null,
        requestId: result.request_id,
        activePanel: 'plan',
      }),
    );
  };

  /** 执行 */
  const handleExecute = () => {
    if (!state.markdownText.trim()) {
      update({ error: { error_code: 'EMPTY_INPUT', message: '请输入 DeveloperSpec 内容', field_ref: 'markdown_text' } });
      return;
    }
    runAction(
      () => executeRich(state.markdownText),
      (result) => ({
        executeResult: result,
        packageResult: null,
        requestId: result.request_id,
        activePanel: 'sql',
      }),
    );
  };

  /** 全流程一键执行 */
  const handleRunAll = () => {
    if (!state.markdownText.trim()) {
      update({ error: { error_code: 'EMPTY_INPUT', message: '请输入 DeveloperSpec 内容', field_ref: 'markdown_text' } });
      return;
    }
    runAction(
      () => runAll(state.markdownText),
      async (result) => {
        // 执行成功后尝试获取 package
        try {
          const pkg = await getPackageRich(result.request_id);
          return {
            executeResult: {
              request_id: result.request_id,
              spec_id: result.spec_id,
              plan_id: result.plan_id,
              generated_sql: '',
              sql_sha256: '',
              compiler_version: '',
              execution_trace: result.execution_trace,
              result_summary: result.result_summary,
              open_questions: [],
            },
            packageResult: pkg,
            requestId: result.request_id,
            activePanel: 'package' as Panel,
          };
        } catch {
          return {
            requestId: result.request_id,
            activePanel: 'sql' as Panel,
            error: {
              error_code: 'PACKAGE_FETCH_FAILED',
              message: 'RunAll 成功但获取 Package 失败',
              field_ref: null,
            },
          };
        }
      },
    );
  };

  const hasContent = state.markdownText.trim().length > 0;

  return (
    <div className="app">
      <header className="app-header">
        <h1>TianShu DataDev Agent — 内部工作台</h1>
        <div className="header-right">
          <PipelineStageIndicator
            stages={state.pipelineStages}
            error={state.pipelineError}
          />
          <span className="app-version">v0.1.0 | dry_run 模式 | 不做生产执行</span>
        </div>
      </header>

      <div className="app-body">
        {/* 左侧：编辑器 + 模板 */}
        <aside className="app-sidebar">
          <TemplateSelector onSelect={handleLoadTemplate} />
        </aside>

        <main className="app-main">
          <SpecEditor
            value={state.markdownText}
            onChange={(v) => update({ markdownText: v })}
          />

          {/* 操作按钮栏 */}
          <div className="action-bar">
            <button
              className="btn btn-primary"
              disabled={!hasContent || state.isLoading}
              onClick={handleParse}
            >
              解析预览
            </button>
            <button
              className="btn btn-secondary"
              disabled={!hasContent || state.isLoading}
              onClick={handlePlan}
            >
              构建 Plan
            </button>
            <button
              className="btn btn-secondary"
              disabled={!hasContent || state.isLoading}
              onClick={handleExecute}
            >
              编译执行
            </button>
            <button
              className="btn btn-accent"
              disabled={!hasContent || state.isLoading}
              onClick={handleRunAll}
            >
              全流程 Run-All
            </button>
            {state.isLoading && <span className="loading-indicator">处理中...</span>}
          </div>

          {/* 错误态展示 */}
          {state.error && (
            <ErrorDisplay error={state.error} onDismiss={clearError} />
          )}

          {/* 面板区域 */}
          <div className="panels">
            {state.specResult && (
              <ParsePreview spec={state.specResult} visible={state.activePanel === 'parse'} />
            )}

            {state.specResult && state.specResult.open_questions.length > 0 && (
              <OpenQuestionPanel questions={state.specResult.open_questions} />
            )}

            {state.planResult && state.planResult.open_questions.length > 0 && (
              <OpenQuestionPanel questions={state.planResult.open_questions} />
            )}

            {state.executeResult && state.executeResult.open_questions.length > 0 && (
              <OpenQuestionPanel questions={state.executeResult.open_questions} />
            )}

            {state.planResult && state.planResult.join_evidence.length > 0 && (
              <JoinEvidencePanel evidence={state.planResult.join_evidence} />
            )}

            {state.planResult && state.planResult.steps.length > 0 && (
              <PlanStepsPanel
                steps={state.planResult.steps}
                validationPassed={state.planResult.validation_passed}
                visible={state.activePanel === 'plan'}
              />
            )}

            {state.executeResult && state.executeResult.generated_sql && (
              <SqlDisplay
                sql={state.executeResult.generated_sql}
                sqlSha256={state.executeResult.sql_sha256}
                compilerVersion={state.executeResult.compiler_version}
                trace={state.executeResult.execution_trace}
                summary={state.executeResult.result_summary}
                visible={state.activePanel === 'sql'}
              />
            )}

            {state.packageResult && (
              <PackageTree
                pkg={state.packageResult}
                visible={state.activePanel === 'package'}
              />
            )}
          </div>
        </main>
      </div>

      <StatusBar
        requestId={state.requestId}
        isLoading={state.isLoading}
        hasError={state.error !== null}
      />
    </div>
  );
}
