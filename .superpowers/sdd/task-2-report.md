# Task 2 报告：模块级合并函数——_normalize_unique_keys_list + _merge_unique_keys_from_sources

## 1. 状态

**DONE**

## 2. 提交的 commits

| 短 hash | 消息 |
|---------|------|
| `c47967b` | feat(source_manifest): 新增模块级 _normalize_unique_keys_list + _merge_unique_keys_from_sources 纯函数 |

## 3. 验证结果

### 导入验证

```bash
python -c "from tianshu_datadev.developer_spec.source_manifest import _normalize_unique_keys_list, _merge_unique_keys_from_sources; print('OK')"
```

输出：`OK`

### 功能验证（6 个测试用例全部通过）

| 测试用例 | 描述 | 结果 |
|----------|------|------|
| Test 1 | 组间去重：`[['id'], ['name'], ['id']]` → `[['id'], ['name']]` | 通过 |
| Test 2 | `None` 输入 → `[]` | 通过 |
| Test 3 | 空列表 `[]` → `[]` | 通过 |
| Test 4 | 合并多来源去重：`src1=[['id']]`, `src2=[['name'], ['id']]` → `[['id'], ['name']]` | 通过 |
| Test 5 | 多 `None` 来源 → `[]` | 通过 |
| Test 6 | 多列键组顺序保留：`[['first_name', 'last_name'], ['last_name', 'first_name']]` 不同顺序视为不同组 | 通过 |

## 4. 疑虑

无。
