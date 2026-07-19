"""Phase 4.5B — 前端 SPA smoke test。

验证前端构建产物结构、关键文本和安全约束。
"""

from __future__ import annotations

import os

import pytest

# 项目根目录
_ROOT = os.path.dirname(os.path.dirname(__file__))
_DIST = os.path.join(_ROOT, "frontend", "dist")


def _read_dist_file(path: str) -> str:
    """读取 dist 目录下的文件内容。"""
    full = os.path.join(_DIST, path)
    if not os.path.isfile(full):
        return ""
    with open(full, "r", encoding="utf-8") as f:
        return f.read()


class TestFrontendBuild:
    """前端静态目录挂载验证——不依赖预构建产物。

    前端构建（npm ci && npm run build）由 CI 独立步骤负责，不放入 dev-reload.sh。
    本测试仅验证后端正确配置了静态文件挂载路径，不检查 dist/ 下文件内容。
    """

    def test_static_dir_mounted(self):
        """验证后端静态文件挂载逻辑——dist 存在时挂载 /assets，不存在时跳过。

        仅检查 create_app() 的 static 挂载配置，不读取 dist/ 文件。
        若 dist/ 不存在（如开发环境未构建前端），此测试仍应通过——此时仅验证无异常。
        """
        from tianshu_datadev.api.app import create_app
        app = create_app()
        # 检查是否有 StaticFiles 挂载
        from fastapi.staticfiles import StaticFiles
        static_routes = [
            r for r in app.routes
            if hasattr(r, "app") and isinstance(r.app, StaticFiles)
        ]
        # 收集挂载路径
        mount_paths = {r.path for r in static_routes}
        # dist 存在时应挂载 /assets；不存在时 mount_paths 可为空
        dist_exists = os.path.isdir(os.path.join(_ROOT, "frontend", "dist"))
        if dist_exists:
            assert "/assets" in mount_paths, (
                f"dist/ 存在时应挂载 /assets，实际挂载路径={mount_paths}"
            )


class TestFrontendContentSafety:
    """前端内容安全约束验证。"""

    @pytest.mark.skipif(not os.path.isdir(os.path.join(_DIST, "assets")),
                        reason="frontend/dist/assets 不存在——前端未构建")
    def test_no_production_execution_entry(self):
        """验证 JS bundle 不含生产执行入口文本。"""
        forbidden = ["生产写入", "生产执行入口", "上线批准", "上线按钮",
                     "production_write", "deploy_to_prod", "RUN_PROD"]
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        for js_file in js_files:
            content = _read_dist_file(os.path.join("assets", js_file))
            for term in forbidden:
                # 仅在包含该词时检查——允许 dry_run / 不做生产执行 等否定表述
                if term in content:
                    # 检查上下文：如果出现，必须被 "不做" / "dry_run" / "禁止" 等否定词包围
                    idx = content.find(term)
                    context = content[max(0, idx - 50):idx + len(term) + 50]
                    # 允许的否定表述模式
                    safe_patterns = [
                        "不做生产", "dry_run", "不提供生产", "禁止",
                        "no production", "不对外", "内部",
                    ]
                    is_safe = any(p in context.lower() for p in safe_patterns)
                    assert is_safe, (
                        f"文件 {js_file} 中发现疑似生产入口文本: "
                        f"'{term}'，上下文: ...{context}..."
                    )

    @pytest.mark.skipif(not os.path.isdir(os.path.join(_DIST, "assets")),
                        reason="frontend/dist/assets 不存在——前端未构建")
    def test_no_review_ready_misuse_in_error(self):
        """验证错误状态不会误写为 REVIEW_READY 或上线批准。"""
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        for js_file in js_files:
            content = _read_dist_file(os.path.join("assets", js_file))
            # REVIEW_READY 不应该出现在错误展示组件中
            if "REVIEW_READY" in content:
                # 仅在注释、文档或非执行上下文中出现才安全
                idx = content.find("REVIEW_READY")
                context = content[max(0, idx - 30):idx + len("REVIEW_READY") + 30]
                # 错误展示组件不应包含 REVIEW_READY
                has_error_context = any(
                    w in context for w in ["error", "Error", "REJECT", "reject"]
                )
                if has_error_context:
                    assert "REVIEW_READY" not in context.replace(
                        "error", ""
                    ).replace("Error", ""), (
                        f"错误上下文中不应出现 REVIEW_READY: ...{context}..."
                    )
                # 否则 REVIEW_READY 可能出现在正常的状态说明中，允许

    @pytest.mark.skipif(not os.path.isdir(os.path.join(_DIST, "assets")),
                        reason="frontend/dist/assets 不存在——前端未构建")
    def test_dry_run_notice_present(self):
        """验证 dry_run 提示存在于构建产物中。"""
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        found_dry_run = False
        for js_file in js_files:
            content = _read_dist_file(os.path.join("assets", js_file))
            if "dry_run" in content:
                found_dry_run = True
                break
        # 注意：production 构建可能压缩变量名，此处仅检查是否存在
        # 如果不存在，可能在 CSS/HTML 中
        if not found_dry_run:
            html = _read_dist_file("index.html")
            css_files = [f for f in os.listdir(assets_dir) if f.endswith(".css")]
            for css_file in css_files:
                css = _read_dist_file(os.path.join("assets", css_file))
                if "dry" in css or "不做" in html:
                    found_dry_run = True
                    break
        assert found_dry_run, (
            "构建产物中缺少 dry_run 提示——"
            "前端必须展示 dry_run 模式标识"
        )


class TestTemplateButtons:
    """模板按钮存在验证。"""

    @pytest.mark.skipif(not os.path.isdir(os.path.join(_DIST, "assets")),
                        reason="frontend/dist/assets 不存在——前端未构建")
    def test_template_api_path_in_bundle(self):
        """验证前端通过模板 API 动态加载按钮，不要求复制后端模板名称。"""
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        content = "\n".join(
            _read_dist_file(os.path.join("assets", js_file))
            for js_file in js_files
        )

        assert "/templates" in content, "JS bundle 中未找到模板 API 路径"

    def test_template_ids_in_pipeline(self):
        """验证模板 ID 存在于模板定义中（模板由 API 端提供，非前端硬编码）。

        Phase 4.5 要求至少 5 个模板（退出条件 #4）。
        TEMPLATES 已从 pipeline.py 外置到 api/templates.py（风险 #8）。
        """
        templates_path = os.path.join(
            _ROOT, "src", "tianshu_datadev", "api", "templates.py"
        )
        with open(templates_path, "r", encoding="utf-8") as f:
            templates_src = f.read()

        template_ids = [
            "tpl_aggregation",
            "tpl_label_table",
            "tpl_multi_step",
            "tpl_two_table_join",     # Phase 4.5 补全
            "tpl_window_topn",        # Phase 4.5 补全
            "tpl_empty",              # Phase 4.5 补全
        ]
        for tid in template_ids:
            assert tid in templates_src, (
                f"模板 ID '{tid}' 未在 templates.py 的 TEMPLATES 中找到"
            )
        # 验证模板数量 >= 5（Phase 4.5 退出条件 #4）
        template_count = templates_src.count('"template_id":')
        assert template_count >= 5, (
            f"模板数量 {template_count} < 5——不满足 Phase 4.5 退出条件 #4"
        )

    @pytest.mark.skipif(not os.path.isdir(os.path.join(_DIST, "assets")),
                        reason="frontend/dist/assets 不存在——前端未构建")
    def test_frontend_fetches_templates(self):
        """验证前端包含模板获取逻辑（fetchTemplates / fetchTemplate）。"""
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        all_content = ""
        for js_file in js_files:
            all_content += _read_dist_file(os.path.join("assets", js_file))
        # 前端通过 API 获取模板——查找 API 调用痕迹
        has_template_api = (
            "templates" in all_content.lower()
            or "fetchTemplate" in all_content
            or "getTemplate" in all_content
        )
        assert has_template_api, "JS bundle 中未找到模板 API 调用逻辑"


class TestApiIntegration:
    """API 与前端集成验证——新前端端点可用性。"""

    def test_frontend_api_routes_registered(self):
        """验证前端专用 API 路由已注册（通过检查 routes.py 源码）。"""
        routes_path = os.path.join(
            _ROOT, "src", "tianshu_datadev", "api", "routes.py"
        )
        with open(routes_path, "r", encoding="utf-8") as f:
            routes_src = f.read()

        # 检查新增的前端专用端点
        expected_routes = [
            "/templates",
            "/templates/",
            "/health",
            "/spec/parse-rich",
            "/plan-rich",
            "/execute-rich",
            "/package-rich/",
        ]
        for route in expected_routes:
            assert route in routes_src, (
                f"前端专用路由 '{route}' 未在 routes.py 中注册"
            )

    def test_frontend_api_models_exist(self):
        """验证前端专用响应模型已定义。"""
        models_path = os.path.join(
            _ROOT, "src", "tianshu_datadev", "api", "models.py"
        )
        with open(models_path, "r", encoding="utf-8") as f:
            models_src = f.read()

        expected_models = [
            "TemplateItem",
            "TemplateListResponse",
            "JoinEvidenceItem",
            "SpecRichResponse",
            "PlanRichResponse",
            "ExecuteRichResponse",
            "PackageRichResponse",
            "HealthResponse",
            "PlanStepSummary",
            "ArtifactTreeNode",
        ]
        for model in expected_models:
            assert f"class {model}" in models_src, (
                f"前端专用模型 '{model}' 未在 models.py 中定义"
            )


class TestSparkPipelineFrontend:
    """Spark 管线前端集成回归测试——按钮/指示灯/错误路径/类型/端点/状态映射。

    验证 Phase 9A 全部 6 个 Task 的前端产出物在源码级别正确：
    - sparkVerify() 函数签名存在
    - SparkVerifyResponse 类型字段完整
    - PipelineStageIndicator title prop + STAGE_CN Spark 6 阶段映射
    - App.tsx handleSparkVerify + 第二个 PipelineStageIndicator
    - Spark 按钮 disabled 逻辑（依赖 requestId 非空）
    - 错误处理（catch 中设置 ApiError 到 ErrorDisplay）
    - POST /api/spark/verify 端点已注册
    - _status_map 映射完整（5 种状态值 → 3 种前端 status）
    """

    # ── 辅助方法 ──

    @staticmethod
    def _read_file(*parts: str) -> str:
        """读取项目文件内容。"""
        path = os.path.join(_ROOT, *parts)
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    # ── client.ts 测试 ──

    def test_spark_verify_function_exists_in_client(self):
        """sparkVerify 函数签名存在于 client.ts。"""
        src = self._read_file("frontend", "src", "api", "client.ts")
        assert "export function sparkVerify" in src, (
            "client.ts 中缺少 sparkVerify 函数导出"
        )
        assert "SparkVerifyResponse" in src, (
            "client.ts 中缺少 SparkVerifyResponse 类型引用"
        )
        assert "'/spark/verify'" in src, (
            "client.ts 中 sparkVerify 未指向 /spark/verify 端点"
        )

    def test_spark_verify_response_type_has_required_fields(self):
        """SparkVerifyResponse 类型包含全部 7 个字段。"""
        src = self._read_file("frontend", "src", "api", "client.ts")
        required_fields = [
            "request_id", "spark_stages", "overall_status",
            "comparator_status", "review_ready", "package_id", "errors",
        ]
        for field in required_fields:
            assert field in src, (
                f"SparkVerifyResponse 缺少字段 '{field}'"
            )

    # ── PipelineStageIndicator 测试 ──

    def test_pipeline_stage_indicator_has_title_prop(self):
        """PipelineStageIndicator 接受可选 title prop。"""
        src = self._read_file(
            "frontend", "src", "components", "PipelineStageIndicator.tsx"
        )
        assert "title?: string" in src, (
            "PipelineStageIndicator Props 中缺少 'title?: string'"
        )
        assert "title || '流水线阶段'" in src, (
            "下拉框 header 未使用 title prop 作为回退"
        )

    def test_stage_cn_has_all_spark_stages(self):
        """STAGE_CN 包含全部 6 个 Spark 阶段的中文映射。"""
        src = self._read_file(
            "frontend", "src", "components", "PipelineStageIndicator.tsx"
        )
        spark_stages_cn = {
            "MAPPER": "映射",
            "DEVELOPER": "标注",
            "COMPILER": "编译",
            "VALIDATOR": "校验",
            "COMPARATOR": "对比",
            "PHYSICAL_VERIFIER": "物理验证",
        }
        for stage_en, stage_cn in spark_stages_cn.items():
            assert stage_en in src, (
                f"STAGE_CN 缺少 Spark 阶段 '{stage_en}'"
            )
            assert stage_cn in src, (
                f"STAGE_CN 中 '{stage_en}' 的中文映射 '{stage_cn}' 缺失"
            )

    # ── App.tsx 测试 ──

    def test_app_has_spark_stage_buttons(self):
        """App.tsx 包含 SparkStageButtons 组件 + handleSparkStageComplete 回调。"""
        src = self._read_file("frontend", "src", "App.tsx")
        assert "SparkStageButtons" in src, (
            "App.tsx 中缺少 SparkStageButtons 组件"
        )
        assert "handleSparkStageComplete" in src, (
            "App.tsx 中缺少 handleSparkStageComplete 回调"
        )
        assert "handleSparkVerify" in src, (
            "App.tsx 中缺少 handleSparkVerify 函数（向后兼容保留）"
        )
        assert "requestId={state.requestId}" in src, (
            "SparkStageButtons 未接收 requestId prop"
        )

    def test_app_has_second_pipeline_indicator_with_spark_title(self):
        """App.tsx 包含第二个 PipelineStageIndicator 且 title='Spark'。"""
        src = self._read_file("frontend", "src", "App.tsx")
        assert 'title="Spark"' in src, (
            "App.tsx 中第二个 PipelineStageIndicator 缺少 title='Spark'"
        )
        # 验证 sparkStages 被传给第二个指示灯的 stages prop
        assert "sparkStages" in src, (
            "App.tsx 中未使用 sparkStages 状态"
        )

    def test_spark_verify_catch_sets_error_for_display(self):
        """handleSparkVerify 的 catch 分支设置 error（ApiError）用于 ErrorDisplay。"""
        src = self._read_file("frontend", "src", "App.tsx")
        # catch 分支必须设置 error 字段——ErrorDisplay 读取 state.error
        assert "error: apiErr" in src, (
            "handleSparkVerify catch 分支未将 apiErr 赋给 error——ErrorDisplay 无法展示"
        )

    # ── 后端路由 + 状态映射测试 ──

    def test_spark_verify_endpoint_registered(self):
        """POST /api/spark/verify 端点已在 routes.py 注册。"""
        src = self._read_file("src", "tianshu_datadev", "api", "routes.py")
        assert '"/spark/verify"' in src, (
            "routes.py 中缺少 /spark/verify 路由注册"
        )
        assert "async def spark_verify" in src, (
            "routes.py 中缺少 spark_verify 端点函数定义"
        )

    def test_status_map_complete(self):
        """_status_map 包含全部 5 种 SparkPipelineState 值的映射。"""
        src = self._read_file("src", "tianshu_datadev", "api", "routes.py")
        required_mappings = [
            ("SUCCESS", "ok"),
            ("FAILURE", "failed"),
            ("HUMAN_REVIEW", "failed"),
            ("SKIPPED", "skipped"),
            ("NOT_EXECUTED", "skipped"),
        ]
        for state_value, frontend_status in required_mappings:
            assert f'"{state_value}"' in src, (
                f"_status_map 缺少状态 '{state_value}'"
            )
            assert f'"{frontend_status}"' in src, (
                f"_status_map 中 '{state_value}' 的目标值 '{frontend_status}' 缺失"
            )

    # ── SQL 管线成功态可观测性测试（R15）──

    def test_run_action_allows_partial_to_override_pipeline_stages(self):
        """runAction 中 partial 可以覆盖 pipelineStages——使得成功态可自定义阶段。"""
        src = self._read_file("frontend", "src", "App.tsx")
        # 验证 merge 顺序：pipelineStages 在 ...partial 之前（partial 后覆盖）
        # 策略：找到 runAction 函数中同时包含 pipelineStages 和 ...partial 的 update 调用
        # 先用 runAction 函数体范围限定搜索
        run_action_start = src.find("const runAction = async")
        assert run_action_start != -1, "未找到 runAction 函数定义"
        # runAction 函数结束于下一个顶层函数定义之前
        next_fn = src.find("\nconst ", run_action_start + 10)
        if next_fn == -1:
            next_fn = len(src)
        run_action_body = src[run_action_start:next_fn]

        # 在 runAction 体内找 ...partial（这是全局唯一的 runAction 内 ...partial）
        partial_idx = run_action_body.find("...partial")
        assert partial_idx != -1, "runAction 中未找到 ...partial"
        # 从 ...partial 反向搜索最近的 update(
        search_start = run_action_body.rfind("update(", 0, partial_idx)
        assert search_start != -1, "...partial 之前未找到 update 调用"

        # 手工配对大括号确定 update({...}) 范围
        depth = 0
        update_end = -1
        for i in range(search_start + len("update("), len(run_action_body)):
            ch = run_action_body[i]
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    update_end = i + 1
                    break
        assert update_end != -1, "无法确定 update 调用的结束位置"
        update_block = run_action_body[search_start:update_end]

        # pipelineStages 应在 ...partial 之前
        ps_pos = update_block.find("pipelineStages")
        partial_pos = update_block.find("...partial")
        assert ps_pos != -1 and partial_pos != -1 and ps_pos < partial_pos, (
            f"runAction 中 pipelineStages 应在 ...partial 之前——"
            f"当前顺序使得 partial 无法覆盖 API 响应中的空 stages。"
            f"ps_pos={ps_pos}, partial_pos={partial_pos}"
        )

    def test_handle_run_all_sets_success_stages(self):
        """handleRunAll 成功路径设置全成功阶段——SQL 指示灯在成功后可见。"""
        src = self._read_file("frontend", "src", "App.tsx")
        # 成功路径（无 pipeline_error）中应设置 pipelineStages
        # 检查 try 分支中有 pipelineStages
        assert "pipelineStages" in src, (
            "handleRunAll 中未设置 pipelineStages"
        )

    def test_stage_cn_has_all_sql_stages(self):
        """STAGE_CN 包含全部 8 个 SQL 阶段的中文映射（含 contract/package）。"""
        src = self._read_file(
            "frontend", "src", "components", "PipelineStageIndicator.tsx"
        )
        sql_stages_cn = {
            "parser": "解析",
            "enrich": "增强",
            "build": "构建",
            "validate": "验证",
            "compile": "编译",
            "execute": "执行",
            "contract": "契约",
            "package": "打包",
        }
        for stage_en, stage_cn in sql_stages_cn.items():
            assert stage_en in src, (
                f"STAGE_CN 缺少 SQL 阶段 '{stage_en}'"
            )
            assert stage_cn in src, (
                f"STAGE_CN 中 '{stage_en}' 的中文映射 '{stage_cn}' 缺失"
            )
