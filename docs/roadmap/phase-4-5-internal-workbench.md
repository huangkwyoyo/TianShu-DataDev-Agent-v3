# Phase 4.5：内部交互验证口（DeveloperSpec 编辑器 + 模板按钮）

> 状态：已实施（Phase 4.5 补全——模板从 3 个扩展到 6 个）✅
> 前置依赖：Phase 4 退出条件全部满足

## 执行前必须阅读

1. `AGENTS.md` §1 — System Role
2. `docs/00-product-charter.md` §7 — 不把业务人员自然语言问数作为产品主入口
3. `docs/09-test-strategy.md` §7 Phase 4.5

## 只允许修改

- `src/tianshu_datadev/api/` — 新建模块
  - `rest_api.py`：REST API（FastAPI 或等价框架）
  - `request_schema.py`：请求/响应 Pydantic Schema
- `frontend/` — 新建前端目录
  - 简单 Web 页面：DeveloperSpec 编辑器 + 模板加载 + 解析预览 + OpenQuestion 面板
- `tests/` — 新增 test_api.py

## 禁止修改

- SQL/Spark 核心逻辑——只通过 API 调用
- 不做生产执行入口
- 不做面向业务人员的自然语言问数 UI

## 新增模型

### REST API Schema

```python
class SubmitDeveloperSpecRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    developer_spec: str                 # Markdown + YAML-like 全文
    template_id: str | None             # 模板 ID（可选）
    options: SubmitOptions | None

class SubmitOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_sql: bool = True               # 是否执行 SQL 验证
    run_harness: bool = False           # 是否触发 Harness 评测

class SubmitDeveloperSpecResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    status: str                         # ACCEPTED | REJECTED | PROCESSING
    parsed_spec: ParsedDeveloperSpec | None
    open_questions: list[OpenQuestion]
    errors: list[ValidationError]
    review_package_path: str | None

class DeveloperSpecTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    template_id: str
    name: str                           # 如 "单表聚合"、"两表 Join"、"窗口 TopN"
    description: str
    markdown_template: str              # 预填的 DeveloperSpec 模板正文
```

### 前端技术栈建议

- **框架**：轻量静态页面（HTML + vanilla JS 或 htmx），不引入重型 SPA 框架
- **编辑器**：CodeMirror 或 Monaco Editor（Markdown + YAML 语法高亮）
- **模板按钮**：预设 5-8 个 DeveloperSpec 模板卡片，点击加载到编辑器

### 模板按钮交互规格

1. 模板卡片列表展示：单表聚合、两表 Join、多表 Join + 聚合、窗口 TopN、CASE 标签分类、自定义空模板
2. 点击模板卡片 → 编辑器加载对应 Markdown 模板（替换当前内容，提示确认）
3. 提交按钮 → POST `/api/submit` → 轮询 `/api/status/{request_id}` → 展示解析预览 + OpenQuestion 面板
4. 解析预览：展示 ParsedDeveloperSpec 的结构化视图（表、字段、指标、维度、Join）
5. OpenQuestion 面板：展示 blocking 问题（红色）和非 blocking 问题（黄色），程序员可在此确认/修改

## artifact schema

- REST API 请求/响应 JSON Schema
- DeveloperSpec 模板库（5-8 个模板文件）

## 必须新增的测试

| 测试类别 | 数量 | 覆盖点 |
|----------|------|--------|
| REST API Schema | 3 | 请求合法通过、extra 字段拒绝、非法 DeveloperSpec 返回结构化错误 |
| CLI 确定性 | 2 | CLI 和 Web 同输入同输出 |
| 前端输入校验 | 2 | 非法输入展示结构化拒绝原因、不崩溃 |

## 必须运行的检查

```bash
python -m pytest tests/ -q -k "api or cli"
curl /api/health  # API 健康检查
git diff --check
```

## B/C 暂停条件

- 前端技术栈选型需团队确认（React / Vue / htmx / vanilla）
- 模板按钮的 5-8 个模板需基于 Phase 4 的真实使用数据确定
- 内部验证口的访问控制策略（仅开发团队用，需确认部署环境）

## 退出条件

1. ✅ REST API 请求/响应 Schema 正确校验
2. ✅ CLI 和 Web 同输入同输出
3. ✅ 非法输入展示结构化拒绝原因，不崩溃
4. ✅ **DeveloperSpec 模板至少 5 个可用**——现有 6 个（Phase 4.5 补全：+两表 Join +窗口 TopN +自定义空模板）
5. ✅ 打开浏览器可加载编辑器、选择模板、提交解析、查看预览和 OpenQuestion
6. ✅ Phase 1A-4 测试保持通过——1203 测试全绿

### Phase 4.5 补全说明（2026-06-29）

原模板仅 3 个（汇总表、标签表、多步骤加工），不满足退出条件 #4（≥5 个）。

新增 3 个模板：

| 模板 ID | 名称 | 分类 | 说明 |
|---------|------|------|------|
| `tpl_two_table_join` | 两表 Join | join（关联宽表） | 事实表 LEFT JOIN 维度表，展开宽表字段，不做聚合 |
| `tpl_window_topn` | 窗口 TopN | window（窗口排名） | ROW_NUMBER 分组排名取 TopN，如各品类销售额 Top10 |
| `tpl_empty` | 自定义空模板 | empty（空白模板） | 带注释的 DeveloperSpec 骨架，从零开始编写 |

模板总数：3 → **6 个**（满足 ≥5 要求）

前端 `TemplateSelector` 已同步更新分类标签映射。

---

> Phase 4.5 | 已实施 + 补全 ✅ | 6 个模板可用 | 下一阶段：Phase 4.6 或 Phase 5
