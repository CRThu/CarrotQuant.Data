import React from 'react';
import { TrendingUp, Layers, BarChart2, ChevronLeft, ChevronRight, Database, Settings, Terminal } from 'lucide-react';

export type ViewType = 'stock_list' | 'concept_industry' | 'stock_detail' | 'data_management' | 'log_center' | 'settings';

interface SidebarNavProps {
  currentView: ViewType;
  onViewChange: (view: ViewType) => void;
  collapsed: boolean;
  onToggleCollapse: () => void;
  activeTaskCount?: number;
}

export const SidebarNav: React.FC<SidebarNavProps> = ({
  currentView,
  onViewChange,
  collapsed,
  onToggleCollapse,
  activeTaskCount = 0,
}) => {
  const navItems = [
    {
      id: 'stock_list' as ViewType,
      label: '股票市场',
      icon: TrendingUp,
      desc: '全市场股票搜索与自选',
    },
    {
      id: 'concept_industry' as ViewType,
      label: '板块概念穿透',
      icon: Layers,
      desc: '东财概念/行业成分股探查',
    },
    {
      id: 'stock_detail' as ViewType,
      label: 'K线三窗格分析',
      icon: BarChart2,
      desc: 'TradingView 3-Pane 极速图表',
    },
    {
      id: 'data_management' as ViewType,
      label: '数据管理中心',
      icon: Database,
      desc: '物理格式、水位线与数据增量同步',
      badge: activeTaskCount,
    },
    {
      id: 'log_center' as ViewType,
      label: '系统日志中心',
      icon: Terminal,
      desc: '全局 Loguru 架构与引擎实时日志流',
    },
  ];

  const isSettingsActive = currentView === 'settings';

  return (
    <aside
      className={`bg-slate-900/60 border-r border-slate-800 flex flex-col transition-all duration-300 relative z-30 ${
        collapsed ? 'w-16' : 'w-56'
      }`}
    >
      {/* 导航菜单列表 */}
      <div className="flex-1 py-4 px-2 space-y-1.5">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = currentView === item.id;
          return (
            <button
              key={item.id}
              onClick={() => onViewChange(item.id)}
              title={collapsed ? `${item.label} - ${item.desc}` : undefined}
              className={`w-full flex items-center px-3 py-2.5 rounded-xl transition-all group cursor-pointer ${
                isActive
                  ? 'bg-cyan-500/15 border border-cyan-500/30 text-cyan-400 font-semibold shadow-lg shadow-cyan-950/40'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Icon className={`w-5 h-5 shrink-0 ${isActive ? 'text-cyan-400' : 'group-hover:text-cyan-300'}`} />
              {!collapsed && (
                <div className="ml-3 text-left overflow-hidden flex-1 flex items-center justify-between">
                  <div className="overflow-hidden">
                    <div className="text-xs tracking-wide truncate">{item.label}</div>
                    <div className="text-[10px] text-slate-500 truncate">{item.desc}</div>
                  </div>
                  {item.badge && item.badge > 0 ? (
                    <span className="ml-1 px-1.5 py-0.2 text-[10px] font-mono bg-cyan-400 text-slate-950 rounded-full font-bold animate-pulse">
                      {item.badge}
                    </span>
                  ) : null}
                </div>
              )}
            </button>
          );
        })}
      </div>

      {/* 底部系统设置与折叠收起按钮区 (左下角 ⚙️ 系统设置) */}
      <div className="p-2 border-t border-slate-800 flex items-center justify-between gap-1">
        <button
          onClick={() => onViewChange('settings')}
          title={collapsed ? '系统设置' : undefined}
          className={`flex items-center space-x-2 px-2.5 py-1.5 rounded-lg text-xs transition-all cursor-pointer ${
            collapsed ? 'justify-center w-full' : 'flex-1'
          } ${
            isSettingsActive
              ? 'bg-cyan-500/20 text-cyan-400 font-semibold border border-cyan-500/30'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
          }`}
        >
          <Settings className={`w-4 h-4 shrink-0 ${isSettingsActive ? 'text-cyan-400' : ''}`} />
          {!collapsed && <span>系统设置</span>}
        </button>

        {!collapsed && (
          <button
            onClick={onToggleCollapse}
            className="p-1.5 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded-lg transition-colors cursor-pointer shrink-0"
            title="收起导航栏"
          >
            <ChevronLeft className="w-4 h-4" />
          </button>
        )}

        {collapsed && (
          <button
            onClick={onToggleCollapse}
            className="p-1.5 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded-lg transition-colors cursor-pointer shrink-0 mt-1"
            title="展开导航栏"
          >
            <ChevronRight className="w-4 h-4" />
          </button>
        )}
      </div>
    </aside>
  );
};

