import React, { useState, useEffect, useRef } from 'react';
import type { LogMessage } from '../types/api';
import { apiClient } from '../services/apiClient';
import { Terminal, Trash2, Copy, Check, Filter, ArrowDown } from 'lucide-react';

interface LogTerminalProps {
  title?: string;
  className?: string;
  maxLogs?: number;
}

export const LogTerminal: React.FC<LogTerminalProps> = ({
  title = "全局系统与引擎日志",
  className = "",
  maxLogs = 0, // 0 表示全量无上限加载
}) => {
  const [logs, setLogs] = useState<LogMessage[]>([]);

  const [filterLevel, setFilterLevel] = useState<string>('ALL');
  const [autoScroll, setAutoScroll] = useState<boolean>(true);
  const [connected, setConnected] = useState<boolean>(false);
  const [copied, setCopied] = useState<boolean>(false);

  const containerRef = useRef<HTMLDivElement | null>(null);

  // 1. 初始化 SSE 连接，广播与推送信道
  useEffect(() => {
    const es = apiClient.createLogEventSource();

    es.onopen = () => {
      setConnected(true);
    };

    es.onmessage = (event) => {
      try {
        const data: LogMessage = JSON.parse(event.data);
        setLogs((prev) => {
          const next = [...prev, data];
          return maxLogs > 0 && next.length > maxLogs ? next.slice(next.length - maxLogs) : next;
        });
      } catch (e) {
        // 静默 ping 心跳
      }
    };

    es.onerror = () => {
      setConnected(false);
    };

    return () => {
      es.close();
    };
  }, [maxLogs]);

  // 2. 自动滚动控制
  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [logs, autoScroll, filterLevel]);

  // 3. 过滤逻辑
  const filteredLogs = logs.filter((log) => {
    if (filterLevel === 'ALL') return true;
    const levelUpper = (log.level || '').toUpperCase();
    if (filterLevel === 'WARN') {
      return levelUpper === 'WARN' || levelUpper === 'WARNING';
    }
    return levelUpper === filterLevel;
  });

  // 清空日志
  const handleClear = () => {
    setLogs([]);
  };

  // 复制日志
  const handleCopy = () => {
    const text = filteredLogs
      .map((l) => `[${l.timestamp}] [${l.level}] ${l.name ? l.name + ':' + l.line : ''} - ${l.message}`)
      .join('\n');
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  // 根据日志等级匹配 Badge 样式
  const renderLevelBadge = (level: string) => {
    const lvl = (level || 'INFO').toUpperCase();
    if (lvl === 'ERROR') {
      return <span className="px-1.5 py-0.5 rounded bg-red-950/80 border border-red-800 text-red-400 font-mono text-[10px] font-bold">ERR</span>;
    }
    if (lvl === 'WARN' || lvl === 'WARNING') {
      return <span className="px-1.5 py-0.5 rounded bg-amber-950/80 border border-amber-800 text-amber-400 font-mono text-[10px] font-bold">WARN</span>;
    }
    if (lvl === 'DEBUG') {
      return <span className="px-1.5 py-0.5 rounded bg-slate-800 border border-slate-700 text-slate-400 font-mono text-[10px]">DBG</span>;
    }
    if (lvl === 'SUCCESS') {
      return <span className="px-1.5 py-0.5 rounded bg-emerald-950/80 border border-emerald-800 text-emerald-400 font-mono text-[10px] font-bold">OK</span>;
    }
    return <span className="px-1.5 py-0.5 rounded bg-cyan-950/80 border border-cyan-800 text-cyan-400 font-mono text-[10px]">INFO</span>;
  };

  // 语法与标签高亮
  const renderLogMessage = (msg: string) => {
    if (msg.includes('[PROGRESS]')) {
      return <span className="text-cyan-300 font-semibold">{msg}</span>;
    }
    if (msg.includes('[BATCH]')) {
      return <span className="text-purple-300 font-semibold">{msg}</span>;
    }
    if (msg.includes('[Sync]') || msg.includes('[+]')) {
      return <span className="text-emerald-300 font-semibold">{msg}</span>;
    }
    if (msg.includes('[REST API]') || msg.includes('Failed') || msg.includes('Error')) {
      return <span className="text-red-300 font-semibold">{msg}</span>;
    }
    return <span className="text-slate-300">{msg}</span>;
  };

  return (
    <div className={`bg-slate-900/90 border border-slate-800 rounded-2xl flex flex-col overflow-hidden shadow-xl ${className}`}>
      {/* 顶部控制栏 Header */}
      <div className="px-4 py-3 bg-slate-900 border-b border-slate-800 flex items-center justify-between gap-2 shrink-0">
        <div className="flex items-center space-x-2.5 overflow-hidden">
          <Terminal className="w-4 h-4 text-cyan-400 shrink-0" />
          <span className="text-xs font-bold text-slate-200 truncate">{title}</span>
          <span className="hidden sm:inline-flex items-center space-x-1 px-2 py-0.5 rounded-full text-[10px] bg-slate-950 border border-slate-800">
            {connected ? (
              <>
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                <span className="text-emerald-400 font-medium">Live</span>
              </>
            ) : (
              <>
                <span className="w-1.5 h-1.5 rounded-full bg-red-400" />
                <span className="text-red-400 font-medium">Disconnected</span>
              </>
            )}
          </span>
        </div>

        {/* 右侧交互控制按钮组 */}
        <div className="flex items-center space-x-2 shrink-0">
          {/* 日志等级 Filter */}
          <div className="flex items-center space-x-1 bg-slate-950 px-2 py-1 rounded-lg border border-slate-800 text-xs">
            <Filter className="w-3.5 h-3.5 text-slate-400 shrink-0" />
            <select
              value={filterLevel}
              onChange={(e) => setFilterLevel(e.target.value)}
              className="bg-transparent text-slate-300 font-sans text-[11px] focus:outline-none cursor-pointer"
            >
              <option value="ALL" className="bg-slate-900">全部等级</option>
              <option value="INFO" className="bg-slate-900">INFO</option>
              <option value="DEBUG" className="bg-slate-900">DEBUG</option>
              <option value="WARN" className="bg-slate-900">WARN</option>
              <option value="ERROR" className="bg-slate-900">ERROR</option>
            </select>
          </div>

          {/* 自动滚动切换开关 */}
          <button
            onClick={() => setAutoScroll(!autoScroll)}
            title={autoScroll ? "已开启自动滚动" : "已暂停自动滚动"}
            className={`px-2 py-1 rounded-lg border text-[11px] font-sans flex items-center space-x-1 transition-colors cursor-pointer ${
              autoScroll
                ? 'bg-cyan-950/60 border-cyan-800 text-cyan-300'
                : 'bg-slate-950 border-slate-800 text-slate-400 hover:text-slate-200'
            }`}
          >
            <ArrowDown className={`w-3 h-3 ${autoScroll ? 'text-cyan-400' : 'text-slate-500'}`} />
            <span className="hidden md:inline">滚动</span>
          </button>

          {/* 复制 */}
          <button
            onClick={handleCopy}
            disabled={filteredLogs.length === 0}
            title="复制所选日志"
            className="p-1.5 bg-slate-950 hover:bg-slate-800 border border-slate-800 text-slate-400 hover:text-slate-200 rounded-lg text-xs transition-colors cursor-pointer disabled:opacity-40"
          >
            {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
          </button>

          {/* 清空 */}
          <button
            onClick={handleClear}
            disabled={logs.length === 0}
            title="清空所有日志"
            className="p-1.5 bg-slate-950 hover:bg-red-950/40 border border-slate-800 hover:border-red-900 text-slate-400 hover:text-red-300 rounded-lg text-xs transition-colors cursor-pointer disabled:opacity-40"
          >
            <Trash2 className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* 主日志视窗 Console Body */}
      <div
        ref={containerRef}
        className="flex-1 p-3.5 bg-slate-950 font-mono text-[11px] overflow-y-auto space-y-1.5 min-h-[280px] max-h-[600px] select-text border-t border-slate-950"
      >
        {filteredLogs.length === 0 ? (
          <div className="h-full min-h-[220px] flex flex-col items-center justify-center text-slate-600 space-y-2 py-12">
            <Terminal className="w-8 h-8 opacity-30" />
            <div className="text-xs">暂无符合条件的日志记录</div>
            <div className="text-[10px] text-slate-600">后台触发数据读取、抓取或同步时此处将实时涌现 Loguru 日志</div>
          </div>
        ) : (
          filteredLogs.map((log, index) => (
            <div
              key={index}
              className="flex items-start space-x-2 hover:bg-slate-900/60 p-1 rounded transition-colors group leading-relaxed"
            >
              <span className="text-slate-500 shrink-0 text-[10px] select-none font-sans pt-0.5">{log.timestamp}</span>
              <div className="shrink-0 pt-0.5">{renderLevelBadge(log.level)}</div>
              {log.name && (
                <span className="text-slate-500 shrink-0 text-[10px] hidden md:inline truncate max-w-[120px]">
                  [{log.name}:{log.line}]
                </span>
              )}
              <div className="flex-1 break-all">{renderLogMessage(log.message)}</div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};
