import React, { useState, useEffect } from 'react';
import { DATA_SOURCE_OPTIONS, type ColorMode } from './types/api';
import { useTables } from './hooks/useTables';
import { HeaderBar } from './components/HeaderBar';
import { SidebarNav, type ViewType } from './components/SidebarNav';
import { StockListView } from './views/StockListView';
import { ConceptIndustryView } from './views/ConceptIndustryView';
import { StockDetailView } from './views/StockDetailView';
import { SyncModal } from './components/SyncModal';
import { ErrorBoundary } from './components/ErrorBoundary';

export const App: React.FC = () => {
  const [currentView, setCurrentView] = useState<ViewType>('stock_detail');
  const [currentTableId, setCurrentTableId] = useState<string>(DATA_SOURCE_OPTIONS[0].table_id);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('sh.600000');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(false);
  const [isSyncModalOpen, setIsSyncModalOpen] = useState<boolean>(false);

  // 终端配色偏好: 'redUpGreenDown' (A股红涨绿跌) | 'greenUpRedDown' (美股绿涨红跌)，持久化至 localStorage
  const [colorMode, setColorMode] = useState<ColorMode>(() => {
    const saved = localStorage.getItem('cqdata_color_mode');
    return saved === 'greenUpRedDown' ? 'greenUpRedDown' : 'redUpGreenDown';
  });

  useEffect(() => {
    localStorage.setItem('cqdata_color_mode', colorMode);
  }, [colorMode]);

  const { serverOnline } = useTables();

  // 从股票列表或板块成分股中选择某个代码，自动打通跳转到 3-Pane K 线详情页
  const handleSelectStock = (symbol: string) => {
    setSelectedSymbol(symbol);
    setCurrentView('stock_detail');
  };

  return (
    <ErrorBoundary fallbackTitle="应用顶层组件渲染拦截">
      <div className="min-h-screen flex flex-col bg-slate-950 text-slate-100 font-sans">
        {/* 顶部 HeaderBar (Logo、数据源切换器、状态指示、搜索、配色设置与同步按钮) */}
        <HeaderBar
          currentTableId={currentTableId}
          onTableChange={setCurrentTableId}
          serverOnline={serverOnline}
          onOpenSyncModal={() => setIsSyncModalOpen(true)}
          searchQuery={searchQuery}
          onSearchChange={setSearchQuery}
          colorMode={colorMode}
          onColorModeChange={setColorMode}
        />

        {/* 主界面: 左侧侧边栏 + 右侧中央工作区 */}
        <div className="flex-1 flex overflow-hidden">
          {/* 左侧可折叠 Sidebar */}
          <SidebarNav
            currentView={currentView}
            onViewChange={setCurrentView}
            collapsed={sidebarCollapsed}
            onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
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
            </ErrorBoundary>
          </main>
        </div>

        {/* 增量同步控制台弹窗 */}
        <SyncModal isOpen={isSyncModalOpen} onClose={() => setIsSyncModalOpen(false)} />
      </div>
    </ErrorBoundary>
  );
};

export default App;
