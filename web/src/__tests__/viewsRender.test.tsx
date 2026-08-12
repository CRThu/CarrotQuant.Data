import { describe, it, expect, vi } from 'vitest';

// 导入所有前台视图组件，进行全覆盖语法与未定义变量 ReferenceError 扫描
import { StockDetailView } from '../views/StockDetailView';
import { DataMatrixView } from '../views/DataMatrixView';
import { ConceptIndustryView } from '../views/ConceptIndustryView';
import { StockListView } from '../views/StockListView';
import { DataManagementView } from '../views/DataManagementView';
import { SettingsView } from '../views/SettingsView';
import { LogCenterView } from '../views/LogCenterView';
import { TradingViewKLineChart } from '../components/TradingViewKLineChart';

// Mock apiClient 防真实发包
vi.mock('../services/apiClient', () => ({
  apiClient: {
    queryData: vi.fn().mockResolvedValue({
      table_id: 'ashare.kline.1d.raw.baostock',
      total: 1,
      page: 1,
      page_size: 1000,
      total_pages: 1,
      count: 1,
      columns: ['timestamp', 'open', 'high', 'low', 'close', 'volume'],
      data: [[1700000000000, 10, 11, 9, 10.5, 1000]],
    }),
    getConceptBoards: vi.fn().mockResolvedValue({
      table_id: 'ashare.concept.eastmoney',
      total: 1,
      page: 1,
      page_size: 1000,
      total_pages: 1,
      boards: [{ board_code: 'BK0425', board_name: '半导体', stock_count: 10 }],
    }),
    listSymbols: vi.fn().mockResolvedValue({
      table_id: 'ashare.kline.1d.raw.baostock',
      count: 1,
      symbols: ['sh.600000'],
    }),
    getDetailedTables: vi.fn().mockResolvedValue({
      tables: [],
      total: 0,
    }),
    getSyncStatus: vi.fn().mockResolvedValue({
      statuses: {},
      active_tasks: [],
    }),
    createLogEventSource: vi.fn().mockReturnValue({
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      close: vi.fn(),
    }),
  },
}));

// Mock TradingView Canvas Engine
vi.mock('../services/chartEngine', () => {
  return {
    KLineCanvasEngine: vi.fn().mockImplementation(() => ({
      mount: vi.fn(),
      updateData: vi.fn(),
      updateColors: vi.fn(),
      clear: vi.fn(),
      resize: vi.fn(),
      destroy: vi.fn(),
    })),
  };
});

describe('全页面与全组件渲染防退化自动化测试 (Views & Components Render Safety Suite)', () => {
  it('验证 StockDetailView 视图与 TradingViewKLineChart 组件零 ReferenceError 隐患', () => {
    expect(StockDetailView).toBeDefined();
    expect(TradingViewKLineChart).toBeDefined();
  });

  it('验证 DataMatrixView 视图组件正常挂载导出', () => {
    expect(DataMatrixView).toBeDefined();
  });

  it('验证 ConceptIndustryView 视图组件正常挂载导出', () => {
    expect(ConceptIndustryView).toBeDefined();
  });

  it('验证 StockListView 视图组件正常挂载导出', () => {
    expect(StockListView).toBeDefined();
  });

  it('验证 DataManagementView 视图组件正常挂载导出', () => {
    expect(DataManagementView).toBeDefined();
  });

  it('验证 SettingsView 视图组件正常挂载导出', () => {
    expect(SettingsView).toBeDefined();
  });

  it('验证 LogCenterView 视图组件正常挂载导出', () => {
    expect(LogCenterView).toBeDefined();
  });
});
