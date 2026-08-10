import React, { useState, useEffect } from 'react';
import type { FileSystemItem, FileSystemListResponse } from '../types/api';
import { apiClient } from '../services/apiClient';
import {
  Folder,
  FileText,
  ChevronRight,
  ArrowUp,
  FolderOpen,
  X,
  Check,
  RefreshCcw,
  HardDrive,
  AlertCircle
} from 'lucide-react';

interface FileExplorerModalProps {
  isOpen: boolean;
  initialPath?: string;
  onClose: () => void;
  onSelectDirectory: (selectedPath: string) => void;
}

export const FileExplorerModal: React.FC<FileExplorerModalProps> = ({
  isOpen,
  initialPath = 'C:\\new_tdx\\vipdoc',
  onClose,
  onSelectDirectory,
}) => {
  const [currentPath, setCurrentPath] = useState<string>(initialPath);
  const [inputPath, setInputPath] = useState<string>(initialPath);
  const [data, setData] = useState<FileSystemListResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // 加载指定路径的内容
  const fetchDirectory = async (targetPath: string) => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiClient.getDirectoryContents(targetPath);
      setData(res);
      setCurrentPath(res.path);
      setInputPath(res.path);
    } catch (e: any) {
      console.error('Failed to load directory contents', e);
      setError(e?.response?.data?.detail || e?.message || '无法读取指定路径内容');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (isOpen) {
      fetchDirectory(initialPath || currentPath);
    }
  }, [isOpen, initialPath]);

  if (!isOpen) return null;

  // 辅助函数：格式化字节大小
  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
  };

  // 导航至上一级目录
  const handleGoUp = () => {
    if (!currentPath) return;
    const cleanPath = currentPath.replace(/[/\\]+$/, '');
    const lastSlash = Math.max(cleanPath.lastIndexOf('/'), cleanPath.lastIndexOf('\\'));
    if (lastSlash > 0) {
      const parent = cleanPath.substring(0, lastSlash);
      fetchDirectory(parent);
    } else if (lastSlash === 0) {
      fetchDirectory(cleanPath.substring(0, 1) + ':\\');
    }
  };

  // 双击或点击文件夹
  const handleItemClick = (item: FileSystemItem) => {
    if (item.is_dir) {
      fetchDirectory(item.path);
    }
  };

  // 提交手动输入的路径
  const handleInputSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (inputPath.trim()) {
      fetchDirectory(inputPath.trim());
    }
  };

  // 确认选择当前目录并关闭 modal
  const handleConfirmSelect = () => {
    onSelectDirectory(currentPath);
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-md animate-in fade-in duration-200">
      <div className="w-full max-w-3xl bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl overflow-hidden flex flex-col max-h-[85vh]">
        {/* Modal 顶部 Header */}
        <div className="px-5 py-3.5 bg-slate-950/80 border-b border-slate-800 flex items-center justify-between">
          <div className="flex items-center space-x-2.5">
            <div className="p-1.5 rounded-lg bg-cyan-950/80 border border-cyan-800/80 text-cyan-400">
              <FolderOpen className="w-5 h-5" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-slate-100 flex items-center space-x-2 font-sans">
                <span>本地目录与文件探查器</span>
                <span className="text-[10px] px-2 py-0.5 rounded-full bg-cyan-950 text-cyan-300 border border-cyan-800/60 font-mono">
                  FS Explorer
                </span>
              </h3>
              <p className="text-[11px] text-slate-400">穿透查看本地文件与文件夹，一键设置数据源路径</p>
            </div>
          </div>

          <button
            onClick={onClose}
            className="p-1.5 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded-lg transition-colors cursor-pointer"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* 路径控制栏 */}
        <div className="p-3 bg-slate-950/40 border-b border-slate-800/80 space-y-2">
          <form onSubmit={handleInputSubmit} className="flex items-center space-x-2">
            <button
              type="button"
              onClick={handleGoUp}
              title="返回上一级目录"
              className="p-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl border border-slate-700 transition-colors shrink-0 cursor-pointer"
            >
              <ArrowUp className="w-4 h-4" />
            </button>

            <div className="relative flex-1">
              <input
                type="text"
                value={inputPath}
                onChange={(e) => setInputPath(e.target.value)}
                placeholder="请输入绝对路径..."
                className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-slate-200 font-mono focus:outline-none focus:border-cyan-500 pr-8"
              />
              <button
                type="submit"
                className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-cyan-400 cursor-pointer"
                title="前往此路径"
              >
                <ChevronRight className="w-4 h-4" />
              </button>
            </div>

            <button
              type="button"
              onClick={() => fetchDirectory(currentPath)}
              disabled={loading}
              className="p-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl border border-slate-700 transition-colors shrink-0 cursor-pointer"
              title="刷新目录"
            >
              <RefreshCcw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            </button>
          </form>

          {/* Breadcrumbs 路径展示 */}
          <div className="flex items-center space-x-1 text-[11px] font-mono text-slate-400 truncate px-1">
            <HardDrive className="w-3.5 h-3.5 text-slate-500 shrink-0" />
            <span className="truncate">{currentPath}</span>
          </div>
        </div>

        {/* 目录文件列表主区域 */}
        <div className="flex-1 overflow-y-auto p-3 min-h-[300px] max-h-[50vh] bg-slate-950/20">
          {error ? (
            <div className="p-6 text-center text-xs text-red-400 bg-red-950/20 rounded-2xl border border-red-900/30 flex flex-col items-center justify-center space-y-2">
              <AlertCircle className="w-8 h-8 text-red-400" />
              <div className="font-semibold">{error}</div>
              <div className="text-[10px] text-slate-400 font-mono">请检查输入的路径是否存在或是否具备读取权限</div>
            </div>
          ) : loading ? (
            <div className="py-12 flex flex-col items-center justify-center text-xs text-slate-400 space-y-2">
              <RefreshCcw className="w-6 h-6 animate-spin text-cyan-400" />
              <span>正在读取本地文件系统...</span>
            </div>
          ) : !data || data.items.length === 0 ? (
            <div className="py-12 flex flex-col items-center justify-center text-xs text-slate-500 space-y-2">
              <FolderOpen className="w-8 h-8 text-slate-600" />
              <span>此目录为空文件夹</span>
            </div>
          ) : (
            <div className="grid grid-cols-1 divide-y divide-slate-800/40">
              {data.items.map((item) => (
                <div
                  key={item.path}
                  onClick={() => handleItemClick(item)}
                  className={`flex items-center justify-between px-3 py-2 rounded-xl transition-colors cursor-pointer group hover:bg-slate-800/60 ${
                    item.is_dir ? 'hover:border-cyan-900/30' : ''
                  }`}
                >
                  <div className="flex items-center space-x-3 min-w-0 flex-1">
                    {item.is_dir ? (
                      <Folder className="w-4 h-4 text-cyan-400 shrink-0 group-hover:scale-110 transition-transform" />
                    ) : (
                      <FileText className="w-4 h-4 text-slate-400 shrink-0" />
                    )}
                    <span
                      className={`text-xs font-mono truncate ${
                        item.is_dir ? 'text-slate-100 font-medium' : 'text-slate-300'
                      }`}
                    >
                      {item.name}
                    </span>
                  </div>

                  <div className="flex items-center space-x-4 text-[11px] font-mono text-slate-500 shrink-0">
                    <span>{item.is_dir ? '文件夹' : formatBytes(item.size)}</span>
                    <span className="hidden sm:inline text-slate-600">
                      {item.updated_at ? item.updated_at.split('T')[0] : '-'}
                    </span>
                    {item.is_dir && <ChevronRight className="w-3.5 h-3.5 text-slate-500 group-hover:text-cyan-400" />}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Modal 底部 Footer */}
        <div className="p-3.5 bg-slate-950/80 border-t border-slate-800 flex items-center justify-between gap-3">
          <div className="text-xs text-slate-400 truncate max-w-md font-mono">
            已选择: <span className="text-cyan-300 font-semibold">{currentPath}</span>
          </div>

          <div className="flex items-center space-x-2">
            <button
              onClick={onClose}
              className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl text-xs font-medium transition-colors cursor-pointer"
            >
              取消
            </button>
            <button
              onClick={handleConfirmSelect}
              className="px-4 py-1.5 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white rounded-xl text-xs font-semibold shadow-md shadow-cyan-950/50 flex items-center space-x-1.5 cursor-pointer transition-all hover:scale-105"
            >
              <Check className="w-4 h-4" />
              <span>选择并填入此目录</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
