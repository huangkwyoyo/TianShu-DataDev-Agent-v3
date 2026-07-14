# 物理验证后代码展示与下载 实施计划

> **执行方式**：inline 执行（单会话），使用 systematic-debugging 排查问题。

**目标**：物理验证成功后，展示 SQL 和 PySpark 代码，每个代码框上方有下载按钮（.sql / .py）。

**架构**：纯前端方案——SQL 和 PySpark 在执行阶段已生成并返回前端，无需新增后端 API。

## 全局约束

- 不新增后端 API——SQL 和 PySpark 代码数据已在前端
- 不修改 `SparkStageButtons.tsx`——按钮逻辑不变
- 不修改 `SqlDisplay.tsx`——保持独立渲染逻辑
- 下载纯前端实现（Blob + URL.createObjectURL），不经过后端
- 所有注释使用中文

---

### Task 1：按钮栏重构——加管线标签 + Run-All 右移

**文件：**
- 修改：`frontend/src/App.tsx:410-448`

**目标**：在操作按钮栏添加"SQL 管线""Spark 管线"文字标签，Run-All 按钮移到最右侧。

- [ ] **Step 1：修改按钮栏 JSX 布局**

将 `App.tsx` 中 action-bar 的按钮区域改为：

```tsx
{/* 操作按钮栏 */}
<div className="action-bar">
  {/* SQL 管线 */}
  <span className="pipeline-label">SQL 管线</span>
  <button className="btn btn-primary" disabled={...} onClick={handleParse}>
    解析预览
  </button>
  <button className="btn btn-secondary" disabled={...} onClick={handlePlan}>
    构建 Plan
  </button>
  <button className="btn btn-secondary" disabled={...} onClick={handleExecute}>
    编译执行
  </button>

  <span className="pipeline-separator">|</span>

  {/* Spark 管线 */}
  <span className="pipeline-label">Spark 管线</span>
  <SparkStageButtons ... />

  <span className="pipeline-separator">|</span>

  {/* 全流程 Run-All */}
  <button className="btn btn-accent" disabled={...} onClick={handleRunAll}>
    全流程 Run-All
  </button>

  {state.isLoading && <span className="loading-indicator">处理中...</span>}
</div>
```

- [ ] **Step 2：添加 CSS 样式**

在 `App.css` 末尾添加：

```css
.pipeline-label {
  font-size: 12px;
  font-weight: 600;
  color: #94a3b8;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-right: 4px;
  user-select: none;
}

.pipeline-separator {
  color: #e2e8f0;
  margin: 0 8px;
  font-size: 18px;
  user-select: none;
}
```

- [ ] **Step 3：重启前端验证布局**

```bash
./dev-reload.sh --frontend
```

检查组：按钮栏布局正确，标签和分隔线可见。

---

### Task 2：持久化 COMPILER 代码 + Run-All 成功后自动展开

**文件：**
- 修改：`frontend/src/App.tsx`

**目标**：保存 COMPILER 阶段的 PySpark 代码，Run-All 成功后自动设置展开标志。

- [ ] **Step 1：新增 `compilerCode` 和 `showCodeDownload` 状态**

在 `AppState` 接口中添加两个字段：

```typescript
// 持久化 COMPILER 阶段产物（不被后续阶段覆盖）
compilerCode: { pyspark: string; standalone: string } | null;
// 是否展示代码下载区
showCodeDownload: boolean;
```

在初始 state 中添加默认值：

```typescript
compilerCode: null,
showCodeDownload: false,
```

- [ ] **Step 2：在 `handleSparkStageComplete` 中持久化 COMPILER 结果**

修改 `handleSparkStageComplete` 回调，当阶段为 COMPILER 且成功时保存代码：

```typescript
const handleSparkStageComplete = (response: SparkStageResponse) => {
  const stages: StageInfo[] = response.spark_stages.map((s) => ({
    stage: s.stage,
    status: s.status,
  }));

  // 持久化 COMPILER 阶段的 PySpark 代码
  const compilerCode =
    response.stage === 'COMPILER' && response.status === 'ok' && response.result
      ? {
          pyspark: response.result.pyspark_code || '',
          standalone: response.result.standalone_pyspark || '',
        }
      : state.compilerCode;  // 保留已有值

  update({
    sparkStages: stages,
    // ... 其他已有逻辑 ...
    compilerCode,
    // PHYSICAL_VERIFIER 成功时自动展示代码下载区
    showCodeDownload:
      (response.stage === 'PHYSICAL_VERIFIER' && response.status === 'ok') ||
      state.showCodeDownload,
  });
};
```

- [ ] **Step 3：Run-All 成功后自动展开**

修改 `handleRunAll` 成功回调，在返回的 partial 中设置：

```typescript
showCodeDownload: true,
```

同时在 `handleExecute`（编译执行）和 `handleRunAll` 中重置 `showCodeDownload: false`，确保新流程开始时隐藏。

- [ ] **Step 4：传递新 props 给 SparkStageResultPanel**

修改 `SparkStageResultPanel` 的调用处，传入新 props：

```tsx
{state.sparkStageResult && (
  <SparkStageResultPanel
    stage={state.sparkStageResult.stage}
    result={state.sparkStageResult.result}
    status={state.sparkStageResult.status}
    visible={true}
    sqlCode={state.executeResult?.generated_sql || ''}
    pysparkCode={state.compilerCode?.pyspark || state.compilerCode?.standalone || ''}
    showCodeDownload={state.showCodeDownload}
  />
)}
```

---

### Task 3：代码展示区 + 下载按钮 UI

**文件：**
- 修改：`frontend/src/components/SparkStageResultPanel.tsx`
- 修改：`frontend/src/components/SparkStageResultPanel.css`

**目标**：PHYSICAL_VERIFIER 成功时展示 SQL 和 PySpark 代码框，每框上方有下载按钮。

- [ ] **Step 1：扩展 Props 接口**

```typescript
interface Props {
  stage: string;
  result: SparkStageResult;
  status: string;
  visible: boolean;
  sqlCode?: string;           // 新增
  pysparkCode?: string;       // 新增
  showCodeDownload?: boolean; // 新增
}
```

- [ ] **Step 2：添加下载辅助函数**

```typescript
/** 触发浏览器下载 */
function downloadFile(content: string, filename: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
```

- [ ] **Step 3：在 PHYSICAL_VERIFIER 成功区域添加代码展示**

将 PHYSICAL_VERIFIER 的渲染区（当前行 268-273）替换为：

```tsx
{/* PHYSICAL_VERIFIER——物理验证结果 + 代码展示 */}
{result.type === 'physical_verify' && (
  <>
    <div className={`spark-result-message${result.skipped ? ' stage-skipped' : ''}`}>
      {result.skipped ? '⏭️ ' : '✅ '}
      {result.message || '物理验证结果'}
    </div>

    {/* 代码展示区——物理验证成功后显示 */}
    {status === 'ok' && !result.skipped && showCodeDownload && (
      <div className="code-download-section">
        {/* SQL 代码框 */}
        {sqlCode && (
          <div className="code-block-wrapper">
            <div className="code-block-header">
              <span className="code-block-title">📜 SQL 代码</span>
              <button
                className="btn-download"
                onClick={() => downloadFile(sqlCode, 'query.sql', 'text/sql')}
              >
                ⬇ 下载 .sql
              </button>
            </div>
            <pre className="code-block"><code>{sqlCode}</code></pre>
          </div>
        )}

        {/* PySpark 代码框 */}
        {pysparkCode && (
          <div className="code-block-wrapper">
            <div className="code-block-header">
              <span className="code-block-title">🐍 PySpark 代码</span>
              <button
                className="btn-download"
                onClick={() => downloadFile(pysparkCode, 'spark_job.py', 'text/x-python')}
              >
                ⬇ 下载 .py
              </button>
            </div>
            <pre className="code-block"><code>{pysparkCode}</code></pre>
          </div>
        )}

        {/* 无代码数据时提示 */}
        {!sqlCode && !pysparkCode && (
          <p className="spark-result-note">代码数据不可用——请先执行编译和 COMPILER 阶段。</p>
        )}
      </div>
    )}
  </>
)}
```

- [ ] **Step 4：添加 CSS 样式**

在 `SparkStageResultPanel.css` 末尾添加：

```css
/* 代码下载区 */
.code-download-section {
  margin-top: 12px;
  border-top: 1px solid #e2e8f0;
  padding-top: 12px;
}

.code-block-wrapper {
  margin-bottom: 16px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  overflow: hidden;
}

.code-block-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  background: #f8fafc;
  border-bottom: 1px solid #e2e8f0;
}

.code-block-title {
  font-size: 13px;
  font-weight: 600;
  color: #334155;
}

.btn-download {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 10px;
  font-size: 12px;
  font-weight: 600;
  color: #fff;
  background: #3b82f6;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.15s;
}

.btn-download:hover {
  background: #2563eb;
}

.code-block {
  margin: 0;
  padding: 12px;
  font-size: 12px;
  line-height: 1.5;
  background: #1e293b;
  color: #e2e8f0;
  overflow-x: auto;
  max-height: 480px;
  overflow-y: auto;
  white-space: pre;
}

.code-block code {
  font-family: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
}
```

---

### Task 4：集成验证

- [ ] **Step 1：重启服务**

```bash
./dev-reload.sh
```

- [ ] **Step 2：手动验证——单阶段触发**

1. 加载模板 → 编译执行 → COMPILER → PHYSICAL_VERIFIER
2. 检查：物理验证成功后出现代码框
3. 点击下载按钮：检查 .sql / .py 文件内容正确

- [ ] **Step 3：手动验证——Run-All 全流程**

1. 加载模板 → 全流程 Run-All
2. 检查：完成后自动展示代码框

- [ ] **Step 4：手动验证——边界情况**

1. 编译执行后直接点 PHYSICAL_VERIFIER（跳过 COMPILER）→ 不应显示 PySpark 代码
2. PHYSICAL_VERIFIER 失败 → 不显示代码下载区

- [ ] **Step 5：运行前端 ESLint 检查**

```bash
cd frontend && npx eslint src/ --ext .tsx,.ts 2>&1 | tail -20
```
