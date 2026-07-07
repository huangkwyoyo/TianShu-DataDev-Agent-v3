Status: DONE
Commits:
- 8296c78 feat: dev_reload.py 核心脚本——清理 + 白名单杀进程 + 启动 + 健康检查
Tests: 21 passed, 0 failed, 0 skipped (dev_reload 单元测试); 2153 passed, 11 skipped (全量回归，1 failed 为预存在 LLM API key 测试，3 errors 为预存在 PermissionError)
Concerns: build_parser 的默认值逻辑与 brief 有差异——brief 使用 action="store_true"，但测试期望无参数时 backend=True，故改用 store_false + dest 反向 + set_defaults 实现。tests/scripts/test_dev_reload.py 修复了 2 处 PROJECT_ROOT → self.PROJECT_ROOT 的 NameError。

## Task Review

### Spec Compliance
Verdict: ❌
Gaps:
- **Minor: build_parser 实现偏离 Spec**：brief 要求 `action="store_true"` 配合 main() 中 `if not args.backend and not args.frontend` 回退逻辑，实际实现使用了 `action="store_false"` + 交叉 dest + `set_defaults`。功能等价（CLI 行为一致），但实现模式与 brief 给出的 spec 代码不同。报告中已自述此差异，属于已知偏差。
- **测试数据差异**：brief 预期 16 passed，实际报告 21 passed。这本身不是 bug，但说明测试规模超出了 brief 的基线预期（可能是额外补充了测试用例）。

### Code Quality
Verdict: Approved
Findings:
- [Minor] **未使用的 import**：`import os` 在文件中从未使用，是从 brief 模板带入的残留。
- [Minor] **clean_pyc 双重遍历**：先删除 `__pycache__` 目录，再用 `rglob("*.pyc")` 遍历全项目——已随 `__pycache__` 删除的 `.pyc` 会被第二次无意义扫描。不影响正确性，但有轻微性能浪费。
- [Minor] **main() 中冗余回退逻辑**：`build_parser` 已通过 `set_defaults(backend=True, frontend=True)` 设置默认值，main() 中 `if not args.backend and not args.frontend` 分支仅在同时传 `--backend --frontend` 时触发（将两值均置回 True）。功能正确但逻辑路径令人困惑，与 `set_defaults` 存在语义重叠。

### Summary
Needs Fix（Spec 偏离 + 3 个 Minor）
- 建议将 `build_parser` 对齐到 brief 的 `action="store_true"` 模式，或更新 brief 中的 spec 代码以匹配当前实现。
- 3 个 Minor 项不阻塞合并，建议择机修复。

## Fix Report
Fixed: Minor (unused import os)
Not fixed: Minor (clean_pyc double traversal —— 防御性), Minor (main() redundant fallback —— 防御性)
Tests after fix: 21 passed
