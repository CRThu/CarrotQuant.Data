# CarrotQuant.Data REST API 使用指南

本文档为 **CarrotQuant.Data (cqdata)** FastAPI RESTful HTTP API 服务端的完整接口规范与参考指南。

---

## 1. 概述与服务启动

`cqdata` 提供基于 FastAPI 的 HTTP 数据服务，全面暴露底层时序/事件数据查询、元数据探查及后台增量数据同步能力。

### 1.1 服务启动命令

可以通过 CLI 命令轻松启动 HTTP 服务：

```bash
# 启动 HTTP 服务 (默认监听 http://0.0.0.0:8000)
cqdata serve

# 指定监听地址与端口
cqdata serve --host 127.0.0.1 --port 8000 --no-reload
```

服务启动后，交互式 Swagger API 文档可在线访问：`http://127.0.0.1:8000/docs`。

---

## 2. API 规范总览

基准 URL (Base URL): `http://<host>:<port>/api/v1`

| 分类 | 路径 | 方法 | 说明 |
| :--- | :--- | :---: | :--- |
| **元数据探查** | `/tables/series` | `GET` | 列出本地所有时间序列表 ID |
| | `/tables/events` | `GET` | 列出本地所有事件静态表 ID |
| | `/tables/{table_id}/formats` | `GET` | 获取指定表在本地拥有的存储格式 (`parquet`, `csv`) |
| | `/tables/{table_id}/symbols` | `GET` | 获取指定表包含的所有证券代码列表 |
| | `/tables/{table_id}/time_range` | `GET` | 获取指定表的时间跨度 (`start_datetime`, `end_datetime`) |
| | `/tables/{table_id}/schema` | `GET` | 获取指定表的字段列定义与数据类型字典 |
| | `/tables/{table_id}/row_count` | `GET` | 获取指定表在物理存储中的记录总行数 |
| **数据切片查询** | `/query/series` | `GET` | **【HTTP GET】** 时间序列数据切片查询 (K线/分笔)，支持物理分页 |
| | `/query/events` | `GET` | **【HTTP GET】** 事件/静态数据切片查询 (板块/龙虎榜等)，支持物理分页 |
| **数据同步** | `/sync` | `POST` | 触发后台数据全自动增量同步任务 |
| | `/tasks` | `GET` | 查询当前正在运行的后台同步任务列表 |

---

## 3. 端点详细说明

### 3.1 时间序列数据查询 (`GET /api/v1/query/series`)

切片查询日 K 线、分钟 K 线等时间序列数据，支持按代码、时间区间、列字段及页码物理切片过滤。

#### 请求参数 (Query Parameters)

| 参数名 | 类型 | 是否必填 | 默认值 | 示例 / 说明 |
| :--- | :--- | :---: | :--- | :--- |
| `table_id` | String | **是** | - | 表 ID，例如 `ashare.kline.1d.raw.baostock` |
| `symbols` | String | 否 | `None` | 证券代码，支持逗号分隔多个，例如 `sh.600000,sz.000001` |
| `start_date` | String | 否 | `None` | 起始日期，例如 `2024-01-01` |
| `end_date` | String | 否 | `None` | 结束日期，例如 `2024-06-30` |
| `columns` | String | 否 | `None` | 选挑字段清单，逗号分隔，例如 `timestamp,open,close,volume` |
| `format` | String | 否 | `"auto"` | 指定存储格式 (`auto`, `parquet`, `csv`) |
| `page` | Integer | 否 | `1` | 当前页码，从 `1` 开始 |
| `page_size` | Integer | 否 | `5000` | 每页记录条数 |

#### 响应 JSON 结构

```json
{
  "table_id": "ashare.kline.1d.raw.baostock",
  "total": 12500,
  "page": 1,
  "page_size": 5000,
  "total_pages": 3,
  "count": 5000,
  "data": [
    {
      "timestamp": 1704092400000,
      "datetime": "2024-01-01T15:00:00.000+08:00",
      "symbol": "sh.600000",
      "open": 6.62,
      "high": 6.68,
      "low": 6.61,
      "close": 6.65,
      "volume": 12500000.0
    }
  ]
}
```

#### cURL 调用示例

```bash
curl -X GET "http://127.0.0.1:8000/api/v1/query/series?table_id=ashare.kline.1d.raw.baostock&symbols=sh.600000,sz.000001&start_date=2024-01-01&page=1&page_size=100"
```

---

### 3.2 事件/静态数据查询 (`GET /api/v1/query/events`)

切片查询概念板块成分股、龙虎榜、机构交易等事件型数据。

#### 请求参数 (Query Parameters)

与 `/api/v1/query/series` 参数完全一致。

#### cURL 调用示例

```bash
curl -X GET "http://127.0.0.1:8000/api/v1/query/events?table_id=ashare.dragon_tiger.eastmoney&page=1&page_size=50"
```

---

### 3.3 异步触发数据同步 (`POST /api/v1/sync`)

异步提交增量同步任务。服务端内置锁机制防止同一表并发重复同步。

#### 请求体 (Request Body)

```json
{
  "table_ids": ["ashare.kline.1d.raw.baostock"],
  "formats": ["parquet", "csv"],
  "start_date": "2024-01-01",
  "end_date": "2024-06-30",
  "force_refresh": false,
  "batch_size": 100,
  "symbol_limit": null
}
```

#### 响应 JSON 示例

```json
{
  "status": "accepted",
  "started_tasks": ["ashare.kline.1d.raw.baostock"],
  "ignored_tasks": [],
  "message": "Sync tasks started in background."
}
```

---

### 3.4 查询活动同步任务 (`GET /api/v1/tasks`)

#### 响应 JSON 示例

```json
{
  "active_tasks": ["ashare.kline.1d.raw.baostock"]
}
```
