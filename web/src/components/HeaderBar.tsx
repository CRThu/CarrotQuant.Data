import React from 'react';
import { Activity } from 'lucide-react';

interface HeaderBarProps {
  serverOnline: boolean;
  latency?: number | null;
  version?: string;
}

export const HeaderBar: React.FC<HeaderBarProps> = ({
  serverOnline,
  latency,
  version = '1.1.0',
}) => {
  return (
    <header className="h-12 border-b border-slate-800 bg-slate-900/90 backdrop-blur-md px-4 flex items-center justify-between sticky top-0 z-40">
      {/* 1. Logo 标志与动态版本号 */}
      <div className="flex items-center space-x-2.5">
        <div className="w-7 h-7 rounded-lg bg-gradient-to-tr from-amber-500 to-orange-600 flex items-center justify-center shadow-md shadow-orange-500/20">
          <Activity className="w-4 h-4 text-slate-950 font-bold" />
        </div>
        <div className="flex items-baseline space-x-1.5">
          <span className="font-extrabold text-sm tracking-wide bg-gradient-to-r from-amber-400 via-orange-300 to-cyan-400 bg-clip-text text-transparent">
            CarrotQuant
          </span>
          <span className="text-[11px] text-slate-500 font-mono">Data Web v{version}</span>
        </div>
      </div>

      {/* 2. 右侧极致精干的真实连接状态 Indicator */}
      <div className="flex items-center space-x-2 px-2.5 py-1 rounded-full bg-slate-950/80 border border-slate-800/80 text-[11px] font-mono">
        <span className={`w-2 h-2 rounded-full ${serverOnline ? 'bg-emerald-400 animate-pulse' : 'bg-red-500'}`} />
        <span className={serverOnline ? 'text-slate-300' : 'text-red-400 font-semibold'}>
          {serverOnline ? (latency !== null && latency !== undefined ? `${latency}ms` : '在线') : '离线'}
        </span>
      </div>
    </header>
  );
};


