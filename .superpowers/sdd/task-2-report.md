# Task 2 实现报告：`_enhance_comment_with_annotation` helper

## 改动内容

**文件：** `src/tianshu_datadev/spark/compiler.py`

1. **第 11 行：** 在 `import hashlib` 之后添加 `import re`
2. **第 27 行：** 在 `from tianshu_datadev.spark.renderer import ...` 之后添加 `from tianshu_datadev.spark.annotations import StepAnnotation`
3. **第 796-851 行：** 在 `_build_comment_block` 方法之后（第 794 行 `return`），`_verify_no_comment_injection` 静态方法之前，插入 `_enhance_comment_with_annotation` 方法（56 行）

## 测试结果

- **6 个 Annotation 注入测试：** 全部 PASS（TDD 绿阶段验证）
- **完整编译器测试套件：** 78/78 PASS

## 注意事项 / 偏差

- **显著发现：** `uv run pytest` 使用了系统全局的 `pytest.exe`（位于 `D:\Program Files\Python312\Scripts\`），该版本使用系统 Python 3.12，无法通过 `.venv` 的可编辑安装看到源码改动。为此，已通过 `uv pip install pytest pytest-cov` 将 pytest 安装在虚拟环境内，然后使用 `uv run python -m pytest` 运行测试。此问题是测试基础设施的预先存在的配置差异，现有 .bat / CI 脚本也可能受影响，但本次任务范围外。

## 提交

（提交后补充）
