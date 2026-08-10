# CarrotQuant.Data Web 极速金融终端使用指南

本文档为开发人员与量化研究员提供 `CarrotQuant.Data` 极速 React Web 终端 (`web/`) 的安装、配置、功能模块与集成指南。

---

## 1. 架构与技术栈概览

- **定位**：现代暗黑极速本地金融 Web 终端，基于 REST API HTTP 端点提供全市场股票 K 线图形化分析与概念成分股穿透。
- **技术栈**：
  - **包管理器**：Bun (强制遵循包管理器规约)
  - **构建框架**：Vite 6 + React 19 + TypeScript 7
  - **样式系统**：Tailwind CSS v4 + Dark Glassmorphism 极客终端设计
  - **图表引擎**：TradingView Lightweight Charts v4 (3-Pane 时间轴强同步)

---

## 2. 快速启动指南

### 2.1 方式一：一键内置托管启动 (用户开箱即用 - 推荐)

打包或开发完成静态前端后，无需安装 Node.js 或 Bun，直接运行命令：

```bash
cqdata server --port 8888 --open
```

该命令会启动 FastAPI API 服务，内置托管前端 UI 界面，并自动调起系统默认浏览器访问 `http://localhost:8888/`。

### 2.2 方式二：前端 UI 开发与热重载调试 (开发者)

如果需要修改 UI 组件或进行前端增量调试：

1. **启动后端服务**：
   ```bash
   cqdata server --port 8888
   ```
2. **启动 Vite 热重载服务**（在 `web/` 目录下）：
   ```bash
   cd web
   bun install
   bun dev
   ```

打开浏览器访问 `http://localhost:5173/` 享受 0.1s Vite HMR 极速实时热更新。


---

## 3. 核心功能视图

### 3.1 视图 1：股票市场 (`StockListView`)
- 全市场已探查数据表与股票标的快速过滤检索网格。
- 支持按代码、名称或拼音全量检索。

### 3.2 视图 2：板块概念穿透 (`ConceptIndustryView`) — **方案 A 落地**
- 基于东方财富 `ashare.concept.eastmoney` / `ashare.industry.eastmoney` 数据表。
- **方案 A 极速穿透算法**：初始化一次性全量拉取，前端内存按 `board_code` 分组索引，提供 **0 延时** 概念板块与成分股实时联动。
- 点击任意成分股卡片（如 `四川路桥 sh.600039`），可无缝无刷新穿透跳转至 K 线详情视图。

### 3.3 视图 3：K 线三窗格分析 (`StockDetailView`)
- **Pane 1 (主图)**：OHLC Candlestick 蜡烛图 + MA5/10/20/60 均线 Overlay + **金叉买(B)/死叉卖(S) 信号原生 Marker 标记**。
- **Pane 2 (副图 1)**：成交量 VOL 柱状图（涨红跌绿符合 A 股习惯）。
- **Pane 3 (副图 2)**：MACD 指标（DIF 快线 / DEA 慢线 / MACD 能量柱）。
- **时间轴强同步**：三窗格通过 `subscribeVisibleLogicalRangeChange` 双向强绑定，滚轮缩放与拖拽无缝联动。
- **手势 Bar 限制**：支持快捷切换 250Bars、500Bars、1000Bars 或 ALL 全量切片。
- **2D 矩阵切片**：点击“查看 2D 矩阵”，可直观查看原始 Polars 二维切片数据。

---

## 4. 防白屏防护 (ErrorBoundary)

在 `App.tsx` 与 `StockDetailView.tsx` 中嵌入了全局与组件级 `ErrorBoundary` 异常边界：
- 运行时捕捉崩溃异常，避免整个页面白屏。
- 自动呈现包含错误栈与“重试恢复组件”按钮的暗黑防护界面。
