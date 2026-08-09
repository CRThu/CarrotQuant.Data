import React from 'react';
import { DATA_SOURCE_OPTIONS, type DataSourceOption, type ColorMode } from '../types/api';
import { Database, RefreshCw, Activity, Search, Palette } from 'lucide-react';

interface HeaderBarProps {
  currentTableId: string;
  onTableChange: (tableId: string) => void;
  serverOnline: boolean;
  onOpenSyncModal: () => void;
  searchQuery: string;
  onSearchChange: (q: string) => void;
  colorMode: ColorMode;
  onColorModeChange: (mode: ColorMode) => void;
}

export const HeaderBar: React.FC<HeaderBarProps> = ({
  currentTableId,
  onTableChange,
  serverOnline,
  onOpenSyncModal,
  searchQuery,
  onSearchChange,
  colorMode,
  onColorModeChange,
}) => {
  const searchInputRef = React.useRef<HTMLInputElement>(null);

  // 绑定 Ctrl+K / Cmd+K 全局快捷键自动聚焦搜索框
  React.useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        searchInputRef.current?.focus();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  return (
    <header className="h-14 border-b border-slate-800 bg-slate-900/80 backdrop-blur-md px-4 flex items-center justify-between sticky top-0 z-40">
      {/* 1. Logo 标志 */}
      <div className="flex items-center space-x-3">
        <div className="w-8 h-8 rounded-lg bg-gradient-to-tr from-amber-500 to-orange-600 flex items-center justify-center shadow-lg shadow-orange-500/20">
          <Activity className="w-5 h-5 text-slate-950 font-bold" />
        </div>
        <div>
          <span className="font-extrabold text-base tracking-wide bg-gradient-to-r from-amber-400 via-orange-300 to-cyan-400 bg-clip-text text-transparent">
            CarrotQuant
          </span>
          <span className="text-xs text-slate-400 ml-1 font-mono">Data Web v1.1</span>
        </div>
      </div>

      {/* 2. 中央控制区: 数据源选择器 & 搜索框 (含 Ctrl+K Chip) */}
      <div className="flex items-center space-x-4 flex-1 max-w-2xl mx-8">
        {/* 数据源选择器 (Data Source Selector) */}
        <div className="relative flex items-center bg-slate-950/80 border border-slate-800 rounded-lg px-3 py-1.5 focus-within:border-cyan-500 transition-colors">
          <Database className="w-4 h-4 text-cyan-400 mr-2 shrink-0" />
          <select
            id="dataSourceSelect"
            name="dataSourceSelect"
            aria-label="选择数据源驱动与数据表"
            value={currentTableId}
            onChange={(e) => onTableChange(e.target.value)}
            className="bg-transparent text-xs text-slate-200 focus:outline-none cursor-pointer pr-4 font-medium"
            title="选择数据源驱动与数据表"
          >
            {DATA_SOURCE_OPTIONS.map((opt: DataSourceOption) => (
              <option key={opt.id} value={opt.table_id} className="bg-slate-900 text-slate-200">
                [{opt.source.toUpperCase()}] {opt.name}
              </option>
            ))}
          </select>
        </div>

        {/* 快捷搜索框 (带精致 kbd Chip) */}
        <div className="relative flex-1 flex items-center">
          <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            ref={searchInputRef}
            id="globalSearchInput"
            name="globalSearchInput"
            type="text"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="搜索股票代码 / 名称 / 拼音..."
            aria-label="搜索股票代码 / 名称 / 拼音"
            className="w-full bg-slate-950/60 border border-slate-800 rounded-lg pl-9 pr-14 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition-colors"
          />
          <kbd className="absolute right-2.5 top-1/2 -translate-y-1/2 pointer-events-none text-[10px] font-mono bg-slate-900 text-slate-400 border border-slate-700/80 px-1.5 py-0.5 rounded shadow-inner">
            Ctrl+K
          </kbd>
        </div>
      </div>

      {/* 3. 右侧状态指示、配色设置与增量同步控台 */}
      <div className="flex items-center space-x-3">
        {/* 红涨绿跌 / 绿涨红跌 终端配色偏好切换 */}
        <div className="flex items-center space-x-1.5 bg-slate-950 px-2.5 py-1 rounded-lg border border-slate-800" title="终端涨跌配色主题设置">
          <Palette className="w-3.5 h-3.5 text-cyan-400" />
          <select
            id="colorModeSelect"
            name="colorModeSelect"
            aria-label="设置涨跌颜色模式"
            value={colorMode}
            onChange={(e) => onColorModeChange(e.target.value as ColorMode)}
            className="bg-transparent text-[11px] text-slate-200 focus:outline-none cursor-pointer font-medium"
          >
            <option value="redUpGreenDown" className="bg-slate-900">A股模式: 红涨绿跌</option>
            <option value="greenUpRedDown" className="bg-slate-900">美股模式: 绿涨红跌</option>
          </select>
        </div>

        {/* 服务健康红绿灯指示 */}
        <div className="flex items-center space-x-1.5 px-2.5 py-1 rounded-full bg-slate-950 border border-slate-800">
          <span className={`w-2 h-2 rounded-full ${serverOnline ? 'bg-emerald-400 animate-pulse' : 'bg-red-500'}`} />
          <span className="text-[11px] font-mono text-slate-300">
            {serverOnline ? 'REST API 在线' : 'API 未连接'}
          </span>
        </div>

        {/* 同步控台触发按钮 */}
        <button
          onClick={onOpenSyncModal}
          className="flex items-center space-x-1.5 px-3 py-1.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white rounded-lg text-xs font-medium shadow-md shadow-cyan-900/30 transition-all active:scale-95 cursor-pointer"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          <span>增量同步</span>
        </button>
      </div>
    </header>
  );
};
