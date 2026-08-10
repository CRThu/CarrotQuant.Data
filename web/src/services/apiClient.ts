import axios from 'axios';
import type {
  QueryMatrixResponse,
  TablesResponse,
  SyncRequestPayload,
  SyncTaskResponse,
} from '../types/api';

/**
 * Axios API 客户端，封装与 CarrotQuant.Data FastAPI 后端的交互
 */
const api = axios.create({
  baseURL: typeof window !== 'undefined' ? '/api/v1' : 'http://localhost:8888/api/v1',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const apiClient = {
  /**
   * 健康检查
   */
  async getHealth() {
    const res = await api.get('/health');
    return res.data;
  },

  /**
   * 获取所有本地已建立的数据表总览
   */
  async listTables(format: string = 'auto'): Promise<TablesResponse> {
    const res = await api.get('/tables', { params: { format } });
    return res.data;
  },

  /**
   * 获取某数据表包含的 Symbol 代码清单
   */
  async listSymbols(tableId: string, format: string = 'auto') {
    const res = await api.get(`/tables/${tableId}/symbols`, { params: { format } });
    return res.data;
  },

  /**
   * 获取某数据表的时间跨度
   */
  async getTimeRange(tableId: string, format: string = 'auto') {
    const res = await api.get(`/tables/${tableId}/time_range`, { params: { format } });
    return res.data;
  },

  /**
   * 统一切片查询接口 (GET /api/v1/query)
   */
  async queryData(params: {
    table_id: string;
    symbols?: string;
    start_date?: string;
    end_date?: string;
    columns?: string;
    format?: string;
    page?: number;
    page_size?: number;
  }): Promise<QueryMatrixResponse> {
    const res = await api.get('/query', { params });
    return res.data;
  },

  /**
   * 异步触发后台同步任务
   */
  async triggerSync(payload: SyncRequestPayload): Promise<SyncTaskResponse> {
    const res = await api.post('/sync', payload);
    return res.data;
  },

  /**
   * 获取正在运行的后台同步任务列表
   */
  async getActiveTasks() {
    const res = await api.get('/tasks');
    return res.data;
  },
};
