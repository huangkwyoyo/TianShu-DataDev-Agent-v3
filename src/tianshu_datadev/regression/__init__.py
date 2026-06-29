"""Phase 4A 回归用例管理——加载、执行和验证 Prompt 回归用例。

公开导出：
- RegressionCase：单个回归用例数据模型
- RegressionRunner：回归用例执行器
- RegressionReport：回归执行报告
"""

from tianshu_datadev.regression.runner import RegressionCase, RegressionReport, RegressionRunner

__all__ = [
    "RegressionCase",
    "RegressionReport",
    "RegressionRunner",
]
