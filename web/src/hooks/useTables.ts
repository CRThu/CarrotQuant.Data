import { useState, useEffect, useCallback } from 'react';
import { apiClient } from '../services/apiClient';

export interface UseTablesReturn {
  tables: string[];
  loading: boolean;
  error: string | null;
  serverOnline: boolean;
  healthInfo: any;
  latency: number | null;
  version: string;
  refreshTables: () => void;
}

export const useTables = (): UseTablesReturn => {
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [serverOnline, setServerOnline] = useState<boolean>(false);
  const [healthInfo, setHealthInfo] = useState<any>(null);
  const [latency, setLatency] = useState<number | null>(null);

  // 单独轻量测量 HTTP 往返 Ping 延迟与健康状态
  const checkHealth = useCallback(async () => {
    const startTime = Date.now();
    try {
      const health = await apiClient.getHealth();
      const endTime = Date.now();
      setLatency(endTime - startTime);
      setHealthInfo(health);
      setServerOnline(health?.status === 'ok');
    } catch (err) {
      setServerOnline(false);
      setLatency(null);
    }
  }, []);

  const fetchTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      await checkHealth();
      const res = await apiClient.listTables();
      const tableList = (res.tables || []).map((t: any) =>
        typeof t === 'string' ? t : t.table_id
      );
      setTables(tableList);
    } catch (err: any) {
      console.error('Failed to connect to REST API backend:', err);
      setError('无法连接后端 API 服务，请确认 rest_api.py 已启动');
    } finally {
      setLoading(false);
    }
  }, [checkHealth]);

  useEffect(() => {
    fetchTables();
    // 每 3 秒实时轮询探查 Ping 延迟与健康状态，实现 HeaderBar 与 SettingsView 强同频实时刷新
    const timer = setInterval(checkHealth, 3000);
    return () => clearInterval(timer);
  }, [fetchTables, checkHealth]);

  return {
    tables,
    loading,
    error,
    serverOnline,
    healthInfo,
    latency,
    version: healthInfo?.version || '1.1.0',
    refreshTables: fetchTables,
  };
};


