import React from 'react';
import type { QueryMatrixResponse, ColorMode } from '../types/api';
import { Table, ArrowUpDown, Download, ChevronLeft, ChevronRight } from 'lucide-react';

interface DataTableProps {
  matrix: QueryMatrixResponse | null;
  loading: boolean;
  colorMode?: ColorMode;
  onPageChange?: (page: number) => void;
}

export const DataTable: React.FC<DataTableProps> = ({
  matrix,
  loading,
  colorMode = 'redUpGreenDown',
  onPageChange,
}) => {
  // 一键导出 2D 切片矩阵数据为 CSV 文件
  const handleExportCSV = () => {
    if (!matrix || !matrix.columns || !matrix.data) return;

    const headers = matrix.columns.join(',');
    const rows = matrix.data.map((row) =>
      row
        .map((cell) => {
          if (cell === null || cell === undefined) return '';
          const str = String(cell);
          return str.includes(',') ? `"${str}"` : str;
        })
        .join(',')
    );

    const csvContent = [headers, ...rows].join('\n');
    const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `cqdata_${matrix.table_id}_slice.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  if (loading) {
    return (
      <div className="p-8 text-center text-xs text-slate-400 bg-slate-900/40 rounded-2xl border border-slate-800 animate-pulse">
        <Table className="w-6 h-6 text-cyan-400 mx-auto mb-2 animate-spin" />
        正在读取 Polars 切片 2D 矩阵数据...
      </div>
    );
  }

  if (!matrix || !matrix.columns || matrix.columns.length === 0 || !matrix.data) {
    return (
      <div className="p-8 text-center text-xs text-slate-500 bg-slate-900/40 rounded-2xl border border-slate-800">
        暂无数据切片明细，请选择有效的数据源与股票代码。
      </div>
    );
  }

  // 双色配色样式
  const isRedUp = colorMode === 'redUpGreenDown';
  const posColorClass = isRedUp ? 'text-red-400 font-semibold' : 'text-emerald-400 font-semibold';
  const negColorClass = isRedUp ? 'text-emerald-400 font-semibold' : 'text-red-400 font-semibold';

  const currentPage = matrix.page || 1;
  const totalPages = matrix.total_pages || Math.ceil((matrix.total || 0) / (matrix.page_size || 500)) || 1;

  return (
    <div className="bg-slate-900/80 rounded-2xl border border-slate-800 overflow-hidden flex flex-col shadow-xl h-full font-mono">
      {/* 表格标题、导出按钮与统计 */}
      <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between bg-slate-950/60 flex-wrap gap-2 shrink-0">
        <div className="flex items-center space-x-2">
          <Table className="w-4 h-4 text-cyan-400" />
          <span className="text-xs font-semibold text-slate-200">
            数据切片明细
          </span>
          <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-cyan-950 text-cyan-400 border border-cyan-800/40">
            {matrix.table_id}
          </span>
        </div>
        <div className="flex items-center space-x-4">
          <div className="text-[11px] font-mono text-slate-400">
            共 <span className="text-cyan-400 font-bold">{matrix.total}</span> 条记录 | 当前页展现 <span className="text-amber-400">{matrix.count}</span> 行
          </div>
          <button
            onClick={handleExportCSV}
            className="flex items-center space-x-1 px-2.5 py-1 bg-slate-950 hover:bg-slate-800 border border-slate-800 text-[11px] text-cyan-400 rounded-lg transition-colors cursor-pointer"
            title="导出为 CSV 文件"
          >
            <Download className="w-3.5 h-3.5" />
            <span>导出 CSV</span>
          </button>
        </div>
      </div>

      {/* 2D 矩阵表格容器 (完美垂直伸展填充屏幕最下方) */}
      <div className="overflow-x-auto overflow-y-auto flex-1 min-h-0">
        <table className="w-full text-left text-xs font-mono">
          <thead className="sticky top-0 bg-slate-950 text-slate-400 border-b border-slate-800 shadow-md">
            <tr>
              <th className="px-3 py-2 text-slate-500 font-normal border-r border-slate-800/40">#</th>
              {matrix.columns.map((col, i) => (
                <th key={i} className="px-3 py-2 font-medium text-slate-300 whitespace-nowrap">
                  <div className="flex items-center space-x-1">
                    <span>{col}</span>
                    <ArrowUpDown className="w-3 h-3 text-slate-600" />
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/40 text-slate-300">
            {matrix.data.map((row, rowIdx) => (
              <tr key={rowIdx} className="hover:bg-slate-800/40 transition-colors">
                <td className="px-3 py-1.5 text-slate-500 border-r border-slate-800/40 text-[10px]">
                  {(currentPage - 1) * (matrix.page_size || 500) + rowIdx + 1}
                </td>
                {row.map((val, colIdx) => (
                  <td key={colIdx} className="px-3 py-1.5 whitespace-nowrap">
                    {val === null || val === undefined ? (
                      <span className="text-slate-600 italic">null</span>
                    ) : typeof val === 'number' ? (
                      <span className={val > 0 ? posColorClass : val < 0 ? negColorClass : 'text-slate-300'}>
                        {val}
                      </span>
                    ) : (
                      String(val)
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* 极速物理分页 Bar */}
      <div className="px-4 py-2 border-t border-slate-800 bg-slate-950/80 flex items-center justify-between text-xs font-mono shrink-0">
        <div className="text-slate-400 text-[11px]">
          第 <span className="text-cyan-400 font-bold">{currentPage}</span> / <span className="text-slate-300 font-bold">{totalPages}</span> 页
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={() => onPageChange && onPageChange(currentPage - 1)}
            disabled={currentPage <= 1}
            className="px-2.5 py-1 bg-slate-900 hover:bg-slate-800 disabled:opacity-30 border border-slate-800 rounded-lg text-slate-300 transition-colors flex items-center space-x-1 cursor-pointer disabled:cursor-not-allowed"
          >
            <ChevronLeft className="w-3.5 h-3.5" />
            <span>上一页</span>
          </button>
          <button
            onClick={() => onPageChange && onPageChange(currentPage + 1)}
            disabled={currentPage >= totalPages}
            className="px-2.5 py-1 bg-slate-900 hover:bg-slate-800 disabled:opacity-30 border border-slate-800 rounded-lg text-slate-300 transition-colors flex items-center space-x-1 cursor-pointer disabled:cursor-not-allowed"
          >
            <span>下一页</span>
            <ChevronRight className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </div>
  );
};
