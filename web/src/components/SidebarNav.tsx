import React from 'react';
import { TrendingUp, Layers, BarChart2, ChevronLeft, ChevronRight } from 'lucide-react';

export type ViewType = 'stock_list' | 'concept_industry' | 'stock_detail';

interface SidebarNavProps {
  currentView: ViewType;
  onViewChange: (view: ViewType) => void;
  collapsed: boolean;
  onToggleCollapse: () => void;
}

export const SidebarNav: React.FC<SidebarNavProps> = ({
  currentView,
  onViewChange,
  collapsed,
  onToggleCollapse,
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
      desc: '东财概念/行业成分股探查 (方案A)',
    },
    {
      id: 'stock_detail' as ViewType,
      label: 'K线三窗格分析',
      icon: BarChart2,
      desc: 'TradingView 3-Pane 极速图表',
    },
  ];

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
              className={`w-full flex items-center px-3 py-2.5 rounded-xl transition-all group ${
                isActive
                  ? 'bg-cyan-500/15 border border-cyan-500/30 text-cyan-400 font-semibold shadow-lg shadow-cyan-950/40'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Icon className={`w-5 h-5 shrink-0 ${isActive ? 'text-cyan-400' : 'group-hover:text-cyan-300'}`} />
              {!collapsed && (
                <div className="ml-3 text-left overflow-hidden">
                  <div className="text-xs tracking-wide">{item.label}</div>
                  <div className="text-[10px] text-slate-500 truncate">{item.desc}</div>
                </div>
              )}
            </button>
          );
        })}
      </div>

      {/* 底部折叠收起按钮 */}
      <div className="p-2 border-t border-slate-800 flex justify-end">
        <button
          onClick={onToggleCollapse}
          className="p-2 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded-lg transition-colors cursor-pointer"
          title={collapsed ? '展开导航栏' : '收起导航栏'}
        >
          {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
        </button>
      </div>
    </aside>
  );
};
