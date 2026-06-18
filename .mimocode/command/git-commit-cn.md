---
description: "基于暂存区差异生成中文 conventional commit 消息并提交"
---

# Git Commit 消息生成

根据用户暂存的代码变更，自动生成符合 conventional commits 规范的中文 commit 消息并提交。

## 步骤

1. 运行 `git diff --cached --stat` 查看暂存文件概览
2. 运行 `git log -5 --oneline` 了解项目 commit 风格
3. 运行 `git diff --cached` 查看完整暂存差异
4. 基于差异内容，生成中文 conventional commit 消息，格式为：
   ```
   <type>(<scope>): <简短描述>

   - <详细变更点1>
   - <详细变更点2>
   ...
   ```
   - type: feat / fix / refactor / docs / chore / test / style / perf
   - scope: 变更涉及的模块名（可选）
   - 描述和详情均使用中文
   - 风格参考 `git log` 中最近的提交
5. 向用户展示生成的 commit 消息，确认后执行 `git commit -m "<message>"`
6. 运行 `git status` 验证提交成功

## 注意事项

- 不要自动 push，除非用户明确要求
- 如果没有暂存的变更（`git diff --cached` 为空），提示用户先 `git add`
- commit 消息长度：标题行 ≤ 72 字符，详情每行 ≤ 72 字符
