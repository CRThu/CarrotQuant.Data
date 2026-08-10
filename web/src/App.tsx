import React, { useState, useEffect } from 'react';
import { DATA_SOURCE_OPTIONS, type ColorMode, type SyncStatusItem } from './types/api';
import { useTables } from './hooks/useTables';
import { apiClient } from './services/apiClient';
import { HeaderBar } from './components/HeaderBar';
import { SidebarNav, type ViewType } from './components/SidebarNav';
import { StockListView } from './views/StockListView';
import { ConceptIndustryView } from './views/ConceptIndustryView';
import { StockDetailView } from './views/StockDetailView';
import { DataManagementView } from './views/DataManagementView';
import { SettingsView } from './views/SettingsView';
import { FloatingSyncWidget } from './components/FloatingSyncWidget';
import { SyncModal } from './components/SyncModal';
import { ErrorBoundary } from './components/ErrorBoundary';

export const App: React.FC = () => {
  const [currentView, setCurrentView] = useState<ViewType>('stock_detail');
  const [currentTableId, setCurrentTableId] = useState<string>(DATA_SOURCE_OPTIONS[0].table_id);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('sh.600000');
  const [searchQuery] = useState<string>('');
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(false);
  const [isSyncModalOpen, setIsSyncModalOpen] = useState<boolean>(false);
  const [activeTaskCount, setActiveTaskCount] = useState<number>(0);
  const [runningStatus, setRunningStatus] = useState<SyncStatusItem | null>(null);

  // 终端配色偏好: 'redUpGreenDown' (A股红涨绿跌) | 'greenUpRedDown' (美股绿涨红跌)，持久化至 localStorage
  const [colorMode, setColorMode] = useState<ColorMode>(() => {
    const saved = localStorage.getItem('cqdata_color_mode');
    return saved === 'greenUpRedDown' ? 'greenUpRedDown' : 'redUpGreenDown';
  });

  useEffect(() => {
    localStorage.setItem('cqdata_color_mode', colorMode);
  }, [colorMode]);

  // 全局轮询获取活动中的任务状态，供悬浮 Widget 使用
  useEffect(() => {
    const pollStatus = async () => {
      try {
        const res = await apiClient.getSyncStatus();
        setActiveTaskCount((res.active_tasks || []).length);

        const allStatuses = Object.values(res.statuses || {});
        // 1. 优先选 status === 'running' 的任务 (若有多个，按 start_time 倒序选最新发起的)
        const runningTask = allStatuses
          .filter((s) => s.status === 'running')
          .sort((a, b) => (b.start_time || 0) - (a.start_time || 0))[0];

        if (runningTask) {
          setRunningStatus(runningTask);
        } else {
          // 2. 无正在运行的任务时，选最近完成或失败的任务 (按 end_time/start_time 倒序)
          const finishedTask = allStatuses
            .filter((s) => s.status === 'success' || s.status === 'failed')
            .sort((a, b) => (b.end_time || b.start_time || 0) - (a.end_time || a.start_time || 0))[0];
          setRunningStatus(finishedTask || null);
        }
      } catch (e) {
        // 静默
      }
    };

    pollStatus();
    const timer = setInterval(pollStatus, 2000);
    return () => clearInterval(timer);
  }, []);

  const { serverOnline, latency, version, healthInfo } = useTables();

  // 从股票列表或板块成分股中选择某个代码，自动打通跳转到 3-Pane K 线详情页
  const handleSelectStock = (symbol: string) => {
    setSelectedSymbol(symbol);
    setCurrentView('stock_detail');
  };

  return (
    <ErrorBoundary fallbackTitle="应用顶层组件渲染拦截">
      <div className="h-screen max-h-screen flex flex-col bg-slate-950 text-slate-100 font-sans overflow-hidden relative">
        {/* 顶部 HeaderBar (极简 HUD: Logo、版本号与在线延迟) */}
        <HeaderBar serverOnline={serverOnline} latency={latency} version={version} />

        {/* 主界面: 左侧侧边栏 + 右侧中央工作区 */}
        <div className="flex-1 flex overflow-hidden">
          {/* 左侧可折叠 Sidebar (含左下角 ⚙️ 系统设置按钮) */}
          <SidebarNav
            currentView={currentView}
            onViewChange={setCurrentView}
            collapsed={sidebarCollapsed}
            onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
            activeTaskCount={activeTaskCount}
          />

          {/* 中央工作区 (带 ErrorBoundary 防白屏) */}
          <main className="flex-1 p-4 lg:p-6 overflow-y-auto max-w-7xl mx-auto w-full">
            <ErrorBoundary fallbackTitle="视图区域渲染拦截">
              {currentView === 'stock_list' && (
                <StockListView
                  currentTableId={currentTableId}
                  onSelectStock={handleSelectStock}
                  searchQuery={searchQuery}
                />
              )}

              {currentView === 'concept_industry' && (
                <ConceptIndustryView
                  onSelectStock={handleSelectStock}
                  globalSearchQuery={searchQuery}
                />
              )}

              {currentView === 'stock_detail' && (
                <StockDetailView
                  currentTableId={currentTableId}
                  selectedSymbol={selectedSymbol}
                  onSymbolChange={setSelectedSymbol}
                  colorMode={colorMode}
                />
              )}

              {currentView === 'data_management' && (
                <DataManagementView onSyncStatusChange={setActiveTaskCount} />
              )}

              {currentView === 'settings' && (
                <SettingsView
                  currentTableId={currentTableId}
                  onTableChange={setCurrentTableId}
                  colorMode={colorMode}
                  onColorModeChange={setColorMode}
                  serverOnline={serverOnline}
                  latency={latency}
                  healthInfo={healthInfo}
                />
              )}
            </ErrorBoundary>
          </main>
        </div>

        {/* 弹窗备份 */}
        <SyncModal isOpen={isSyncModalOpen} onClose={() => setIsSyncModalOpen(false)} />

        {/* 全局右下角悬浮同步 Widget */}
        <FloatingSyncWidget
          activeStatus={runningStatus}
          onNavigateToManagement={() => setCurrentView('data_management')}
        />
      </div>
    </ErrorBoundary>
  );
};

export default App;

