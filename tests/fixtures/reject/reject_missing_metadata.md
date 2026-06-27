# 禁止宽松 1：YAML metadata block 不存在

> 应抛出 ParseError(E001)

这个文件没有 fenced code block，也没有 YAML metadata。
Parser 应在 _extract_fenced_block 阶段直接失败。
