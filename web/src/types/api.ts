/**
 * CarrotQuant.Data Web 前端 TypeScript 类型契约
 */

// 物理存储表支持的数据源分类与模板表 ID
export interface DataSourceOption {
  id: string;
  name: string;
  table_id: string;
  category: 'ashare' | 'aindex' | 'concept' | 'dragon_tiger';
  source: 'baostock' | 'eastmoney' | 'tdx';
  description: string;
}

// 预定义常用的金融终端数据源选项
export const DATA_SOURCE_OPTIONS: DataSourceOption[] = [
  {
    id: 'ashare_kline_1d_raw_baostock',
    name: 'Baostock A股日线 (不复权)',
    table_id: 'ashare.kline.1d.raw.baostock',
    category: 'ashare',
    source: 'baostock',
    description: '个股日线 OHLCV 数据，按 [symbol, year] CSV/Parquet 分片'
  },
  {
    id: 'ashare_kline_1d_adj_baostock',
    name: 'Baostock A股日线 (后复权)',
    table_id: 'ashare.kline.1d.adj.baostock',
    category: 'ashare',
    source: 'baostock',
    description: '个股后复权 K 线数据'
  },
  {
    id: 'ashare_kline_5m_raw_baostock',
    name: 'Baostock A股5分钟线',
    table_id: 'ashare.kline.5m.raw.baostock',
    category: 'ashare',
    source: 'baostock',
    description: '个股高频 5 分钟 K 线数据'
  },
  {
    id: 'ashare_concept_eastmoney',
    name: '东方财富 概念板块与成分股',
    table_id: 'ashare.concept.eastmoney',
    category: 'concept',
    source: 'eastmoney',
    description: '东财概念板块代码与成分股映射 (Event 表)'
  },
  {
    id: 'ashare_industry_eastmoney',
    name: '东方财富 行业板块与成分股',
    table_id: 'ashare.industry.eastmoney',
    category: 'concept',
    source: 'eastmoney',
    description: '东财行业板块成分股映射'
  },
  {
    id: 'ashare_dragon_tiger_eastmoney',
    name: '东方财富 龙虎榜每日统计',
    table_id: 'ashare.dragon_tiger.eastmoney',
    category: 'dragon_tiger',
    source: 'eastmoney',
    description: '机构与营业部每日上榜明细'
  },
  {
    id: 'ashare_kline_1d_raw_tdx',
    name: '通达信 A股日线 (TDX)',
    table_id: 'ashare.kline.1d.raw.tdx',
    category: 'ashare',
    source: 'tdx',
    description: '通达信本地 vipdoc 或在线日线数据'
  }
];

// /api/v1/tables 接口返回的数据结构
export interface TableMeta {
  table_id: string;
  category: string;
}

export interface TablesResponse {
  tables: string[];
  total: number;
}

// /api/v1/query 端点返回的 2D 切片矩阵响应结构
export interface QueryMatrixResponse {
  table_id: string;
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
  count: number;
  columns: string[];
  data: (string | number | boolean | null)[][];
}

// TradingView Lightweight Charts 蜡烛图数据点契约
export interface OHLCBar {
  time: string; // YYYY-MM-DD 或 timestamp 秒/毫秒字符串
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount?: number;
}

// TradingView 柱状图 (成交量/MACD) 数据点
export interface HistogramBar {
  time: string;
  value: number;
  color?: string;
}

// TradingView 曲线 (MA/DIF/DEA) 数据点
export interface LineDataPoint {
  time: string;
  value: number;
}

// 买卖点/交易信号 Marker 标注定义
export interface BSMarkerItem {
  time: string;
  position: 'aboveBar' | 'belowBar' | 'inBar';
  color: string;
  shape: 'circle' | 'square' | 'arrowUp' | 'arrowDown';
  text: string;
  size?: number;
}

// MACD 计算结果集中契约
export interface MACDResult {
  dif: LineDataPoint[];
  dea: LineDataPoint[];
  macdBar: HistogramBar[];
}

// 均线数据集契约
export interface MovingAverageData {
  ma5: LineDataPoint[];
  ma10: LineDataPoint[];
  ma20: LineDataPoint[];
  ma60: LineDataPoint[];
}

// 方案 A 概念/行业板块在前端聚合的树形与成分股数据结构
export interface ConceptBoardItem {
  board_code: string;
  board_name: string;
  stock_count: number;
  stocks: {
    symbol: string;
    stock_name: string;
  }[];
}

// 同步任务请求体与状态结构
export interface SyncRequestPayload {
  table_ids: string[];
  formats?: string[];
  start_date?: string;
  end_date?: string;
  force_refresh?: boolean;
  batch_size?: number;
  symbol_limit?: number;
}

export interface SyncTaskResponse {
  status: string;
  started_tasks: string[];
  ignored_tasks: string[];
  message: string;
}

// 终端配色偏好: 'redUpGreenDown' (A股红涨绿跌) | 'greenUpRedDown' (美股/国际绿涨红跌)
export type ColorMode = 'redUpGreenDown' | 'greenUpRedDown';

export interface UpDownColors {
  upColor: string;
  downColor: string;
}

export const getUpDownColors = (mode: ColorMode = 'redUpGreenDown'): UpDownColors => {
  if (mode === 'greenUpRedDown') {
    return { upColor: '#22c55e', downColor: '#ef4444' };
  }
  return { upColor: '#ef4444', downColor: '#22c55e' };
};
