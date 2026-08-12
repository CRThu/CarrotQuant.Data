import React, { useState, useEffect } from 'react';
import { Activity, Zap, Loader2 } from 'lucide-react';

interface HeaderBarProps {
  serverOnline: boolean;
  latency?: number | null;
  version?: string;
  isGlobalLoading?: boolean;
}

// 实时 60 FPS 帧率测量 Hook
const useFPS = () => {
  const [fps, setFps] = useState<number>(60);
  useEffect(() => {
    let frameCount = 0;
    let lastTime = performance.now();
    let animId: number;

    const tick = (now: number) => {
      frameCount++;
      if (now - lastTime >= 1000) {
        setFps(Math.min(120, Math.max(1, Math.round((frameCount * 1000) / (now - lastTime)))));
        frameCount = 0;
        lastTime = now;
      }
      animId = requestAnimationFrame(tick);
    };

    animId = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(animId);
  }, []);

  return fps;
};

export const HeaderBar: React.FC<HeaderBarProps> = ({
  serverOnline,
  latency,
  version = '1.1.0',
  isGlobalLoading = false,
}) => {
  const fps = useFPS();

  return (
    <header className="h-12 border-b border-slate-800 bg-slate-900/98 px-4 flex items-center justify-between sticky top-0 z-40">
      {/* 1. Logo 标志与动态版本号 */}
      <div className="flex items-center space-x-2.5">
        <div className="w-7 h-7 rounded-lg bg-gradient-to-tr from-amber-500 to-orange-600 flex items-center justify-center border border-amber-500/40">
          <Activity className="w-4 h-4 text-slate-950 font-bold" />
        </div>
        <div className="flex items-baseline space-x-1.5">
          <span className="font-extrabold text-sm tracking-wide text-cyan-400 font-mono">
            CarrotQuant
          </span>
          <span className="text-[11px] text-slate-500 font-mono">Data Web v{version}</span>
        </div>
      </div>

      {/* 2. 右侧极致精干的 HUD: 实时 FPS + 网络 Latency (ms) + 服务器状态与全局请求指示器 */}
      <div className="flex items-center space-x-2.5">
        {/* 全局请求中 Loading Indicator 提示 */}
        {isGlobalLoading && (
          <div className="flex items-center space-x-1.5 px-2 py-0.5 rounded-full bg-cyan-950/80 border border-cyan-800 text-cyan-400 text-[10px] font-mono animate-pulse">
            <Loader2 className="w-3 h-3 animate-spin text-cyan-400" />
            <span>请求数据中...</span>
          </div>
        )}

        {/* 实时 FPS + Ping 延迟组合 HUD */}
        <div className="flex items-center space-x-2 px-3 py-1 rounded-full bg-slate-950/80 border border-slate-800/80 text-[11px] font-mono">
          {/* FPS 指示 */}
          <div className="flex items-center space-x-1 text-slate-400 border-r border-slate-800 pr-2">
            <Zap className="w-3 h-3 text-amber-400" />
            <span className="text-amber-400 font-bold">{fps}</span>
            <span className="text-[10px] text-slate-500">FPS</span>
          </div>

          {/* Latency 延迟指示 */}
          <div className="flex items-center space-x-1">
            <span className={`w-2 h-2 rounded-full ${serverOnline ? 'bg-emerald-400 animate-pulse shadow-sm shadow-emerald-400/50' : 'bg-red-500'}`} />
            <span className={serverOnline ? 'text-slate-300 font-semibold' : 'text-red-400 font-semibold'}>
              {serverOnline ? (latency !== null && latency !== undefined ? `${latency}ms` : '在线') : '离线'}
            </span>
          </div>
        </div>
      </div>
    </header>
  );
};


