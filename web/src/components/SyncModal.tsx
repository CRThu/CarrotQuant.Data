import React, { useState, useEffect } from 'react';
import { DATA_SOURCE_OPTIONS } from '../types/api';
import { apiClient } from '../services/apiClient';
import { X, RefreshCw, CheckCircle2, AlertTriangle, Clock } from 'lucide-react';

interface SyncModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export const SyncModal: React.FC<SyncModalProps> = ({ isOpen, onClose }) => {
  const [selectedTables, setSelectedTables] = useState<string[]>([DATA_SOURCE_OPTIONS[0].table_id]);
  const [startDate, setStartDate] = useState<string>('2024-01-01');
  const [endDate, setEndDate] = useState<string>('');
  const [forceRefresh, setForceRefresh] = useState<boolean>(false);
  const [activeTasks, setActiveTasks] = useState<string[]>([]);
  const [syncing, setSyncing] = useState<boolean>(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // 轮询活动中的同步任务
  const checkActiveTasks = async () => {
    try {
      const res = await apiClient.getActiveTasks();
      setActiveTasks(res.active_tasks || []);
    } catch (e) {
      // 静默吞掉
    }
  };

  useEffect(() => {
    if (isOpen) {
      checkActiveTasks();
      const timer = setInterval(checkActiveTasks, 3000);
      return () => clearInterval(timer);
    }
  }, [isOpen]);

  if (!isOpen) return null;

  const toggleTableSelect = (tableId: string) => {
    if (selectedTables.includes(tableId)) {
      setSelectedTables(selectedTables.filter((t) => t !== tableId));
    } else {
      setSelectedTables([...selectedTables, tableId]);
    }
  };

  const handleStartSync = async () => {
    if (selectedTables.length === 0) {
      setError('请至少选择一个数据表');
      return;
    }

    setSyncing(true);
    setMessage(null);
    setError(null);

    try {
      const res = await apiClient.triggerSync({
        table_ids: selectedTables,
        formats: ['parquet', 'csv'],
        start_date: startDate || undefined,
        end_date: endDate || undefined,
        force_refresh: forceRefresh,
      });

      setMessage(`后台增量同步任务已启动：${res.started_tasks.join(', ')}`);
      checkActiveTasks();
    } catch (err: any) {
      console.error('Failed to trigger sync task:', err);
      setError(err?.response?.data?.detail || err?.message || '启动同步失败');
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-md flex items-center justify-center p-4 animate-in fade-in duration-200">
      <div className="bg-slate-900 border border-slate-800 rounded-2xl w-full max-w-xl shadow-2xl overflow-hidden flex flex-col">
        {/* 弹窗 Header */}
        <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between bg-slate-950/50">
          <div className="flex items-center space-x-2">
            <RefreshCw className="w-5 h-5 text-cyan-400" />
            <h3 className="text-sm font-bold text-slate-100">后台数据增量同步与补全控制台</h3>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-200 p-1 rounded-lg hover:bg-slate-800 transition-colors cursor-pointer"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* 弹窗 Body */}
        <div className="p-6 space-y-5 flex-1 overflow-y-auto max-h-[70vh]">
          {/* 正在运行中的后台任务卡片 */}
          {activeTasks.length > 0 && (
            <div className="p-3.5 bg-cyan-950/40 border border-cyan-800/60 rounded-xl flex items-center justify-between">
              <div className="flex items-center space-x-2.5">
                <Clock className="w-4 h-4 text-cyan-400 animate-spin" />
                <div>
                  <div className="text-xs font-semibold text-cyan-300">后台增量同步正在运行中...</div>
                  <div className="text-[10px] font-mono text-cyan-400/80">{activeTasks.join(', ')}</div>
                </div>
              </div>
            </div>
          )}

          {message && (
            <div className="p-3 bg-emerald-950/40 border border-emerald-800/60 rounded-xl text-xs text-emerald-300 flex items-center space-x-2">
              <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
              <span>{message}</span>
            </div>
          )}

          {error && (
            <div className="p-3 bg-red-950/40 border border-red-800/60 rounded-xl text-xs text-red-300 flex items-center space-x-2">
              <AlertTriangle className="w-4 h-4 text-red-400 shrink-0" />
              <span>{error}</span>
            </div>
          )}

          {/* 1. 目标数据表多选 */}
          <div>
            <label className="block text-xs font-medium text-slate-300 mb-2">
              选择目标数据表 (支持多选):
            </label>
            <div className="space-y-2 max-h-48 overflow-y-auto pr-1">
              {DATA_SOURCE_OPTIONS.map((opt) => {
                const isSelected = selectedTables.includes(opt.table_id);
                return (
                  <div
                    key={opt.id}
                    onClick={() => toggleTableSelect(opt.table_id)}
                    className={`p-2.5 rounded-xl border text-xs cursor-pointer transition-all flex items-center justify-between ${
                      isSelected
                        ? 'bg-cyan-950/30 border-cyan-500/50 text-cyan-200'
                        : 'bg-slate-950/50 border-slate-800 text-slate-400 hover:border-slate-700'
                    }`}
                  >
                    <div>
                      <div className="font-semibold text-slate-200">{opt.name}</div>
                      <div className="text-[10px] font-mono text-slate-500">{opt.table_id}</div>
                    </div>
                    <div
                      className={`w-4 h-4 rounded border flex items-center justify-center ${
                        isSelected ? 'bg-cyan-500 border-cyan-400 text-slate-950' : 'border-slate-700'
                      }`}
                    >
                      {isSelected && '✓'}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* 2. 起止时间范围 */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label htmlFor="startDateInput" className="block text-xs font-medium text-slate-300 mb-1.5">起始日期</label>
              <input
                id="startDateInput"
                name="startDateInput"
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-cyan-500"
              />
            </div>
            <div>
              <label htmlFor="endDateInput" className="block text-xs font-medium text-slate-300 mb-1.5">结束日期 (留空至今)</label>
              <input
                id="endDateInput"
                name="endDateInput"
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-cyan-500"
              />
            </div>
          </div>

          {/* 3. 选项 */}
          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="forceRefresh"
              checked={forceRefresh}
              onChange={(e) => setForceRefresh(e.target.checked)}
              className="rounded bg-slate-950 border-slate-800 text-cyan-500 focus:ring-cyan-500 cursor-pointer"
            />
            <label htmlFor="forceRefresh" className="text-xs text-slate-300 cursor-pointer select-none">
              强制刷新全量重新拉取 (突破增量水位线)
            </label>
          </div>
        </div>

        {/* 弹窗 Footer */}
        <div className="px-6 py-4 border-t border-slate-800 bg-slate-950/50 flex justify-end space-x-3">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl text-xs font-medium transition-colors cursor-pointer"
          >
            取消
          </button>
          <button
            onClick={handleStartSync}
            disabled={syncing}
            className="flex items-center space-x-1.5 px-4 py-2 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white rounded-xl text-xs font-medium shadow-lg shadow-cyan-900/30 transition-all cursor-pointer disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${syncing ? 'animate-spin' : ''}`} />
            <span>{syncing ? '启动中...' : '立即启动同步'}</span>
          </button>
        </div>
      </div>
    </div>
  );
};
