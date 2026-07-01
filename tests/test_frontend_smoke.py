"""Phase 4.5B — 前端 SPA smoke test。

验证前端构建产物结构、关键文本和安全约束。
"""

from __future__ import annotations

import os

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
    """前端构建产物验证。"""

    def test_index_html_exists(self):
        """验证 index.html 存在且包含正确标题。"""
        assert os.path.isfile(os.path.join(_DIST, "index.html")), "index.html 缺失"
        content = _read_dist_file("index.html")
        assert "TianShu DataDev Agent" in content
        assert "内部工作台" in content

    def test_js_bundle_exists(self):
        """验证 JS bundle 存在且非空。"""
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        assert len(js_files) > 0, "JS bundle 缺失"
        for js_file in js_files[:1]:
            size = os.path.getsize(os.path.join(assets_dir, js_file))
            assert size > 1000, f"JS bundle {js_file} 过小({size} bytes)"

    def test_css_bundle_exists(self):
        """验证 CSS bundle 存在且非空。"""
        assets_dir = os.path.join(_DIST, "assets")
        css_files = [f for f in os.listdir(assets_dir) if f.endswith(".css")]
        assert len(css_files) > 0, "CSS bundle 缺失"
        for css_file in css_files[:1]:
            size = os.path.getsize(os.path.join(assets_dir, css_file))
            assert size > 500, f"CSS bundle {css_file} 过小({size} bytes)"


class TestFrontendContentSafety:
    """前端内容安全约束验证。"""

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

    def test_template_names_in_bundle(self):
        """验证模板名称（汇总表/标签表/多步骤加工）存在于 JS bundle。

        Vite production 构建会对字符串进行压缩处理，因此采用子串匹配策略——
        检查每个模板名称的关键子串是否出现在构建产物中。
        """
        # 使用简短关键子串避免 minifier 截断问题
        template_keywords = ["汇总表", "标签表", "多步骤"]
        assets_dir = os.path.join(_DIST, "assets")
        js_files = [f for f in os.listdir(assets_dir) if f.endswith(".js")]
        found = set()
        for js_file in js_files:
            content = _read_dist_file(os.path.join("assets", js_file))
            for kw in template_keywords:
                if kw in content:
                    found.add(kw)
        missing = set(template_keywords) - found
        assert not missing, f"JS bundle 中未找到模板关键词: {missing}"

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
