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

  // 单独轻量测量 HTTP 往返 Ping 延迟与健康状态 (带引用比对，避免无脑重绘)
  const checkHealth = useCallback(async () => {
    const startTime = Date.now();
    try {
      const health = await apiClient.getHealth();
      const endTime = Date.now();
      const newLatency = endTime - startTime;
      const isOk = health?.status === 'ok';

      setLatency((prev) => (prev !== null && Math.abs(newLatency - prev) < 40 ? prev : newLatency));
      setHealthInfo((prev: any) => (prev?.status === health?.status && prev?.version === health?.version ? prev : health));
      setServerOnline((prev) => (prev === isOk ? prev : isOk));
    } catch {
      setServerOnline((prev) => (prev === false ? prev : false));
      setLatency((prev) => (prev === null ? prev : null));
    }
  }, []);

  const fetchTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [, res] = await Promise.all([checkHealth(), apiClient.listTables()]);
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
    // 每 5 秒轻量探查 Ping 延迟
    const timer = setInterval(checkHealth, 5000);
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
