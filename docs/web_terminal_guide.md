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

### 2.2 方式二：全栈热重载开发调试 (开发者 - 推荐)

如果需要修改代码或进行前后端联动调试：

- **Windows 用户一键启动 (推荐)**：
  在项目根目录下双击或在终端运行：
  ```cmd
  .\dev.bat
  ```
  该脚本会自动在两个独立的窗口中同时拉起 Python 后端热重载服务 (`uv run cqdata server -p 8888 -r`) 与 React 前端热重载服务 (`cd web && bun dev`)。

- **手动分窗口启动**：
  1. **窗口 1 (Python 后端热重载)**：`uv run cqdata server --port 8888 --reload`
  2. **窗口 2 (React 前端热重载)**：`cd web && bun dev`

打开浏览器访问 `http://localhost:5173/` 即可享受前后端全栈无缝热重载开发体验。


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

### 3.4 视图 4：数据管理中心 (`DataManagementView`)
- **独立格式水位线表格 (`TableManagementGrid`)**：展开数据表后，独立显示 `Parquet` 与 `CSV` 各在物理磁盘上的水位线起止时间、总记录行数与独立复选框。
- **快捷“同步至最新”**：默认自动以当前时间为终点增量补全数据，支持自定义起止日期与“强制全量刷新”开关。
- **命令行式动态提示**：表格行与右下角 **悬浮 Widget (`FloatingSyncWidget`)** 实时展示动态、可读的命令行式提示信息（如：`🟡 45% - 正在抓取 sh.600000 (45/100)`）。
- **右下角悬浮 Widget**：切去查看 K 线或股票列表时，后台同步不受中断， Widget 会在右下角提供精简下载进度，支持一键缩小为 `⏳ 45%` 的环形呼吸灯图标。

---

## 4. 防白屏防护 (ErrorBoundary)

在 `App.tsx` 与 `StockDetailView.tsx` 中嵌入了全局与组件级 `ErrorBoundary` 异常边界：
- 运行时捕捉崩溃异常，避免整个页面白屏。
- 自动呈现包含错误栈与“重试恢复组件”按钮的暗黑防护界面。
