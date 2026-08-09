import { useState, useEffect, useCallback } from 'react';
import { apiClient } from '../services/apiClient';

export interface UseTablesReturn {
  tables: string[];
  loading: boolean;
  error: string | null;
  serverOnline: boolean;
  healthInfo: any;
  refreshTables: () => void;
}

export const useTables = (): UseTablesReturn => {
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [serverOnline, setServerOnline] = useState<boolean>(false);
  const [healthInfo, setHealthInfo] = useState<any>(null);

  const fetchTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // 探查服务器状态
      const health = await apiClient.getHealth();
      setHealthInfo(health);
      setServerOnline(health?.status === 'ok');

      // 获取数据表列表 (兼容解析字符串列表与对象列表)
      const res = await apiClient.listTables();
      const tableList = (res.tables || []).map((t: any) =>
        typeof t === 'string' ? t : t.table_id
      );
      setTables(tableList);
    } catch (err: any) {
      console.error('Failed to connect to REST API backend:', err);
      setServerOnline(false);
      setError('无法连接后端 API 服务，请确认 rest_api.py 已启动');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTables();
    // 定时探查服务器健康状态
    const timer = setInterval(fetchTables, 15000);
    return () => clearInterval(timer);
  }, [fetchTables]);

  return {
    tables,
    loading,
    error,
    serverOnline,
    healthInfo,
    refreshTables: fetchTables,
  };
};
