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
import { SparkStageButtons } from './components/SparkStageButtons';
import { SparkStageResultPanel } from './components/SparkStageResultPanel';
import { LlmTracePanel } from './components/LlmTracePanel';
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
  sparkVerify,
  checkArtifactsStatus,
  ApiError,
  SpecRichResponse,
  PlanRichResponse,
  ExecuteRichResponse,
  PackageRichResponse,
  SparkVerifyResponse,
  SparkStageResponse,
  LlmTraceNode,
  RunAllResponse,
  SparkStageResult,
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

  // Spark 管线验证结果
  sparkStages: StageInfo[];
  sparkVerifyResult: SparkVerifyResponse | null;

  // Spark 管线——仅 artifacts_ready 时可触发单阶段
  artifactsReady: boolean;

  // Spark 单阶段触发的产物内容（供面板展示）
  sparkStageResult: { stage: string; result: SparkStageResult; status: string } | null;

  // LLM 调用追踪（各阶段累积）
  llmTraces: Record<string, LlmTraceNode> | null;
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
    sparkStages: [],
    sparkVerifyResult: null,
    artifactsReady: false,
    sparkStageResult: null,
    llmTraces: null,
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
        sparkStageResult: null,
        activePanel: null,
      });
    }
  };

  /** 清除错误状态——保留 pipelineStages 以便指示灯持续可见 */
  const clearError = () => update({ error: null, pipelineError: null });

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
        // 管线错误同时写入 error，使 ErrorDisplay 在主面板区也能展示错误原因
        error: plError
          ? {
              error_code: `PIPELINE_${plError.stage.toUpperCase()}_FAILED`,
              message: plError.error_message,
              field_ref: plError.stage,
            }
          : null,
        pipelineError: plError,
        pipelineStages: plStages,
        ...partial,
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
        artifactsReady: false,  // parse 不产生 contract
        sparkStageResult: null,
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
        artifactsReady: false,  // plan 不产生 contract
        sparkStageResult: null,
      }),
    );
  };

  /** 执行——编译执行成功后验证 artifacts 就绪状态 */
  const handleExecute = () => {
    if (!state.markdownText.trim()) {
      update({ error: { error_code: 'EMPTY_INPUT', message: '请输入 DeveloperSpec 内容', field_ref: 'markdown_text' } });
      return;
    }
    runAction(
      () => executeRich(state.markdownText),
      async (result) => {
        // execute-rich 成功后异步验证 artifacts 是否真正就绪
        let artifactsReady = false;
        if (result.request_id) {
          try {
            const status = await checkArtifactsStatus(result.request_id);
            artifactsReady = status.artifacts_ready;
          } catch {
            artifactsReady = false;
          }
        }
        return {
          executeResult: result,
          packageResult: null,
          requestId: result.request_id,
          activePanel: 'sql' as Panel,
          llmTraces: (result as ExecuteRichResponse).llm_traces || null,
          artifactsReady,
          // 重置 Spark 状态——新的 execute 需要重新执行 Spark 阶段
          sparkStages: [],
          sparkVerifyResult: null,
          sparkStageResult: null,
        };
      },
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
        // run-all 成功后异步验证 artifacts 是否就绪
        let artifactsReady = false;
        if (result.request_id) {
          try {
            const status = await checkArtifactsStatus(result.request_id);
            artifactsReady = status.artifacts_ready;
          } catch {
            artifactsReady = false;
          }
        }
        // 如果管线执行失败（Validator 阻断等），不尝试获取 package
        // pipeline_error 和 pipeline_stages 由 runAction 自动提取并展示在 PipelineStageIndicator
        if (result.pipeline_error) {
          return {
            requestId: result.request_id,
            activePanel: 'sql' as Panel,
            artifactsReady,
            llmTraces: (result as RunAllResponse).llm_traces || null,
            sparkStageResult: null,
          };
        }
        // 管线成功——尝试获取 package
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
              execution_trace: result.execution_trace!,
              result_summary: result.result_summary!,
              open_questions: [],
            },
            packageResult: pkg,
            requestId: result.request_id,
            activePanel: 'package' as Panel,
            artifactsReady,
            sparkStageResult: null,
            llmTraces: (result as RunAllResponse).llm_traces || null,
            // SQL 管线成功——设置全部 8 阶段为 ok，使指示灯在成功后仍然可见
            pipelineStages: [
              { stage: 'parser', status: 'ok' },
              { stage: 'enrich', status: 'ok' },
              { stage: 'build', status: 'ok' },
              { stage: 'validate', status: 'ok' },
              { stage: 'compile', status: 'ok' },
              { stage: 'execute', status: 'ok' },
              { stage: 'contract', status: 'ok' },
              { stage: 'package', status: 'ok' },
            ],
          };
        } catch {
          return {
            requestId: result.request_id,
            activePanel: 'sql' as Panel,
            artifactsReady,
            llmTraces: (result as RunAllResponse).llm_traces || null,
            sparkStageResult: null,
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

  /** Spark 管线验证 */
  const handleSparkVerify = () => {
    if (!state.requestId) {
      update({ error: { error_code: 'NO_REQUEST_ID', message: '请先执行全流程 Run-All 生成 request_id', field_ref: null } });
      return;
    }
    update({ isLoading: true, error: null });
    sparkVerify(state.requestId)
      .then((result) => {
        update({
          isLoading: false,
          sparkStages: result.spark_stages,
          sparkVerifyResult: result,
        });
      })
      .catch((err) => {
        const apiErr: ApiError =
          err && typeof err === 'object' && 'error_code' in err
            ? (err as ApiError)
            : { error_code: 'NETWORK_ERROR', message: String(err), field_ref: null };
        update({ isLoading: false, error: apiErr, sparkStages: [], sparkVerifyResult: null });
      });
  };

  /** Spark 单阶段完成回调 */
  const handleSparkStageComplete = (response: SparkStageResponse) => {
    // 将后端返回的 spark_stages 映射为前端 StageInfo 格式
    const stages: StageInfo[] = response.spark_stages.map((s) => ({
      stage: s.stage,
      status: s.status,
    }));
    update({
      sparkStages: stages,
      sparkVerifyResult: {
        request_id: response.request_id,
        spark_stages: response.spark_stages,
        overall_status: '',
        comparator_status: '',
        review_ready: false,
        package_id: '',
        errors: response.errors,
      },
      // 存储阶段产物内容（供 SparkStageResultPanel 渲染）
      sparkStageResult: response.result
        ? { stage: response.stage, result: response.result, status: response.status }
        : null,
      // 合并 llm_traces——后续阶段的追踪追加到已有数据
      llmTraces: response.llm_traces
        ? { ...(state.llmTraces || {}), ...response.llm_traces }
        : state.llmTraces,
    });
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
            testId="run-all-status"
          />
          <PipelineStageIndicator
            stages={state.sparkStages}
            error={null}
            title="Spark 管线"
            testId="spark-status"
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
            <SparkStageButtons
              requestId={state.requestId}
              artifactsReady={state.artifactsReady}
              stages={state.sparkStages}
              onStageComplete={handleSparkStageComplete}
              onError={(err) => update({ error: err })}
              disabled={state.isLoading}
            />
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

            {/* Spark 单阶段结果面板——位于 SQL 面板上方，方便对比审查 */}
            {state.sparkStageResult && (
              <SparkStageResultPanel
                stage={state.sparkStageResult.stage}
                result={state.sparkStageResult.result}
                status={state.sparkStageResult.status}
                visible={true}
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

            {/* LLM 调用追踪——编译执行后或 Spark 阶段后 */}
            <LlmTracePanel
              traces={state.llmTraces}
              visible={
                (state.activePanel === 'sql' || state.activePanel === 'package') &&
                state.executeResult !== null
              }
            />

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
