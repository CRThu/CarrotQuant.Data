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

### 3.1 视图 1：股票搜索 (`StockListView`)
- 全市场已探查数据表与股票标的快速检索与自选。
- 使用通用 `SearchInput` 组件，支持代码 (`600000`)、名称 (`浦发银行`) 或拼音首字母 (`pfyh`) 模糊检索与键盘导航 (`ArrowUp`/`ArrowDown`/`Enter`)。
- 顶部预留「我的自选」与「导入自选」卡片，下方为高密度紧凑数据列表。

### 3.2 视图 2：板块概念 (`ConceptIndustryView`)
- 基于东方财富 `ashare.concept.eastmoney` / `ashare.industry.eastmoney` 数据表。
- **极速穿透算法**：初始化一次性全量拉取，前端内存按 `board_code` 分组索引，提供 **0 延时** 概念板块与成分股实时联动。
- 集成 `SearchInput` 拼音首字母搜索，点击任意成分股卡片，可无缝跳转至 K 线详情视图。

### 3.3 视图 3：行情 K 线 (`StockDetailView`)
- **3-Pane 单屏无滚动紧凑布局**：包含主图 (K线+均线+买卖点 Marker)、副图 1 (成交量 VOL)、副图 2 (MACD/RSI)，高度自适应单屏展示，消除页面垂直滚动条。
- **时间轴强同步**：三窗格通过 `subscribeVisibleLogicalRangeChange` 双向强绑定，滚轮缩放与拖拽无缝联动。
- **数据矩阵跳转**：点击顶部“数据矩阵”按钮，可快捷打开全屏二维数据切片矩阵视图。

### 3.4 视图 4：数据矩阵 (`DataMatrixView`)
- **独立数据矩阵切片**：提供全屏 Polars 二维 List 矩阵表格展示、动态搜表与数据切片透视。

### 3.5 视图 5：数据中心 (`DataManagementView`)
- **独立格式数据范围表格 (`TableManagementGrid`)**：展开数据表后，独立显示 `Parquet` 与 `CSV` 在物理磁盘上的数据范围起止时间、总记录行数。
- **右侧实时日志终端卡片 (`LogTerminal`)**：集成暗黑极客风日志视窗，基于 `/api/v1/logs/stream` SSE 接口实时推送全局 Loguru 系统与引擎日志。
- **快捷“同步至最新”**：默认自动以当前时间为终点增量补全数据，支持自定义起止日期与“强制全量刷新”开关。
- **命令行式动态提示**：表格行与右下角 **悬浮 Widget (`FloatingSyncWidget`)** 实时展示动态、可读的命令行式提示信息（如：`🟡 45% - 正在抓取 sh.600000 (45/100)`）。
- **右下角悬浮 Widget**：切去查看 K 线或股票列表时，后台同步不受中断， Widget 会在右下角提供精简下载进度，支持一键缩小为 `⏳ 45%` 的环形呼吸灯图标。

---

## 4. 防白屏防护 (ErrorBoundary)

在 `App.tsx` 与 `StockDetailView.tsx` 中嵌入了全局与组件级 `ErrorBoundary` 异常边界：
- 运行时捕捉崩溃异常，避免整个页面白屏。
- 自动呈现包含错误栈与“重试恢复组件”按钮的暗黑防护界面。
