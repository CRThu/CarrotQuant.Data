import React, { useEffect, useRef, useState } from 'react';
import {
  createChart,
  ColorType,
  CrosshairMode,
} from 'lightweight-charts';
import type {
  IChartApi,
  SeriesMarker,
  Time,
} from 'lightweight-charts';
import type {
  OHLCBar,
  HistogramBar,
  MovingAverageData,
  MACDResult,
  BSMarkerItem,
  LineDataPoint,
  ColorMode,
} from '../types/api';
import { getUpDownColors } from '../types/api';
import { Sliders, Activity, Target } from 'lucide-react';

interface TradingViewKLineChartProps {
  ohlcBars: OHLCBar[];
  volumeBars: HistogramBar[];
  maData: MovingAverageData;
  macdData: MACDResult;
  rsiData?: LineDataPoint[];
  markers?: BSMarkerItem[];
  selectedIndicator?: string;
  onIndicatorChange?: (ind: string) => void;
  barLimit: number;
  onBarLimitChange: (limit: number) => void;
  colorMode?: ColorMode;
}

export const TradingViewKLineChart: React.FC<TradingViewKLineChartProps> = ({
  ohlcBars,
  volumeBars,
  maData,
  macdData,
  rsiData = [],
  markers = [],
  selectedIndicator = 'MACD',
  onIndicatorChange,
  barLimit,
  onBarLimitChange,
  colorMode = 'redUpGreenDown',
}) => {
  const containerMainRef = useRef<HTMLDivElement>(null);
  const containerVolRef = useRef<HTMLDivElement>(null);
  const containerIndRef = useRef<HTMLDivElement>(null);

  const chartMainRef = useRef<IChartApi | null>(null);
  const chartVolRef = useRef<IChartApi | null>(null);
  const chartIndRef = useRef<IChartApi | null>(null);

  // 实时 Crosshair 光敏探针 Legend
  const [hoverInfo, setHoverInfo] = useState<OHLCBar | null>(null);

  const { upColor, downColor } = getUpDownColors(colorMode);

  // 初始化 3-Pane 图表与手势/时间轴强同步 (Lightweight Charts v4 Native)
  useEffect(() => {
    if (!containerMainRef.current || !containerVolRef.current || !containerIndRef.current) return;

    // 通用外观样式选项
    const commonChartOptions = {
      layout: {
        background: { type: ColorType.Solid, color: '#090d16' },
        textColor: '#94a3b8',
        fontSize: 11,
        fontFamily: 'Roboto, -apple-system, sans-serif',
      },
      grid: {
        vertLines: { color: 'rgba(30, 41, 59, 0.5)' },
        horzLines: { color: 'rgba(30, 41, 59, 0.5)' },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
      },
      rightPriceScale: {
        borderColor: '#1e293b',
      },
      timeScale: {
        borderColor: '#1e293b',
        timeVisible: true,
        secondsVisible: false,
      },
      handleScale: {
        mouseWheel: true,
        pinch: true,
        axisPressedMouseMove: true,
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
      },
    };

    // 1. 创建 Pane 1 (主图: K线 + 均线 + 买卖点)
    const chartMain = createChart(containerMainRef.current, {
      ...commonChartOptions,
      height: 320,
    });
    chartMainRef.current = chartMain;

    const candlestickSeries = chartMain.addCandlestickSeries({
      upColor,
      downColor,
      borderVisible: false,
      wickUpColor: upColor,
      wickDownColor: downColor,
    });

    // 绑定十字光标移动探针，更新 Hover 数据
    chartMain.subscribeCrosshairMove((param) => {
      if (!param || !param.time || param.point === undefined || param.point.x < 0 || param.point.y < 0) {
        setHoverInfo(null);
        return;
      }
      const data = param.seriesData.get(candlestickSeries) as any;
      if (data && typeof data.close === 'number') {
        setHoverInfo({
          time: String(param.time),
          open: data.open,
          high: data.high,
          low: data.low,
          close: data.close,
          volume: data.volume ?? 0,
        });
      }
    });

    // 均线 Overlay 系列
    const ma5Series = chartMain.addLineSeries({ color: '#eab308', lineWidth: 1, title: 'MA5' });
    const ma10Series = chartMain.addLineSeries({ color: '#a855f7', lineWidth: 1, title: 'MA10' });
    const ma20Series = chartMain.addLineSeries({ color: '#06b6d4', lineWidth: 1, title: 'MA20' });
    const ma60Series = chartMain.addLineSeries({ color: '#64748b', lineWidth: 1, title: 'MA60' });

    // 2. 创建 Pane 2 (副图 1: 成交量 VOL)
    const chartVol = createChart(containerVolRef.current, {
      ...commonChartOptions,
      height: 120,
    });
    chartVolRef.current = chartVol;

    const volumeSeries = chartVol.addHistogramSeries({
      priceFormat: { type: 'volume' },
    });

    // 3. 创建 Pane 3 (副图 2: 技术指标 MACD / RSI)
    const chartInd = createChart(containerIndRef.current, {
      ...commonChartOptions,
      height: 130,
    });
    chartIndRef.current = chartInd;

    const difSeries = chartInd.addLineSeries({ color: '#38bdf8', lineWidth: 1, title: 'DIF' });
    const deaSeries = chartInd.addLineSeries({ color: '#f59e0b', lineWidth: 1, title: 'DEA' });
    const macdBarSeries = chartInd.addHistogramSeries({ title: 'MACD' });
    const rsiSeries = chartInd.addLineSeries({ color: '#ec4899', lineWidth: 1, title: 'RSI(14)' });

    // 三窗格时间轴强同步机制 (TimeScale Sync Guard)
    let isSyncing = false;

    const syncLogicalRange = (sourceChart: IChartApi, targets: IChartApi[]) => {
      sourceChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (isSyncing || !range) return;
        isSyncing = true;
        targets.forEach((target) => {
          target.timeScale().setVisibleLogicalRange(range);
        });
        isSyncing = false;
      });
    };

    syncLogicalRange(chartMain, [chartVol, chartInd]);
    syncLogicalRange(chartVol, [chartMain, chartInd]);
    syncLogicalRange(chartInd, [chartMain, chartVol]);

    // 自动响应窗口大小变动 (ResizeObserver)
    const handleResize = () => {
      if (containerMainRef.current) chartMain.applyOptions({ width: containerMainRef.current.clientWidth });
      if (containerVolRef.current) chartVol.applyOptions({ width: containerVolRef.current.clientWidth });
      if (containerIndRef.current) chartInd.applyOptions({ width: containerIndRef.current.clientWidth });
    };

    const resizeObserver = new ResizeObserver(handleResize);
    if (containerMainRef.current) resizeObserver.observe(containerMainRef.current);
    if (containerVolRef.current) resizeObserver.observe(containerVolRef.current);
    if (containerIndRef.current) resizeObserver.observe(containerIndRef.current);

    // 存入 refs
    (chartMainRef as any).currentSeries = {
      candlestick: candlestickSeries,
      ma5: ma5Series,
      ma10: ma10Series,
      ma20: ma20Series,
      ma60: ma60Series,
    };
    (chartVolRef as any).currentSeries = { volume: volumeSeries };
    (chartIndRef as any).currentSeries = {
      dif: difSeries,
      dea: deaSeries,
      macdBar: macdBarSeries,
      rsi: rsiSeries,
    };

    return () => {
      resizeObserver.disconnect();
      chartMain.remove();
      chartVol.remove();
      chartInd.remove();
    };
  }, []);

  // 响应 colorMode 更新蜡烛图颜色属性
  useEffect(() => {
    const mainSeries = (chartMainRef as any).currentSeries;
    if (mainSeries && mainSeries.candlestick) {
      mainSeries.candlestick.applyOptions({
        upColor,
        downColor,
        wickUpColor: upColor,
        wickDownColor: downColor,
      });
    }
  }, [colorMode, upColor, downColor]);

  // 更新图表数据系列
  useEffect(() => {
    const mainSeries = (chartMainRef as any).currentSeries;
    const volSeries = (chartVolRef as any).currentSeries;
    const indSeries = (chartIndRef as any).currentSeries;

    if (!mainSeries || !ohlcBars || ohlcBars.length === 0) return;

    // 1. 设置主图蜡烛图与均线
    mainSeries.candlestick.setData(
      ohlcBars.map((b) => ({
        time: b.time as Time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      }))
    );

    mainSeries.ma5.setData(maData.ma5.map((d) => ({ time: d.time as Time, value: d.value })));
    mainSeries.ma10.setData(maData.ma10.map((d) => ({ time: d.time as Time, value: d.value })));
    mainSeries.ma20.setData(maData.ma20.map((d) => ({ time: d.time as Time, value: d.value })));
    mainSeries.ma60.setData(maData.ma60.map((d) => ({ time: d.time as Time, value: d.value })));

    // 2. 设置原生 setMarkers 买卖点标记 (必须按 time 升序排序防 Crash)
    if (markers && markers.length > 0) {
      const tvMarkers: SeriesMarker<Time>[] = markers.map((m) => ({
        time: m.time as Time,
        position: m.position,
        color: m.color,
        shape: m.shape,
        text: m.text,
        size: m.size || 1,
      }));
      tvMarkers.sort((a, b) => (String(a.time) > String(b.time) ? 1 : -1));
      mainSeries.candlestick.setMarkers(tvMarkers);
    } else {
      mainSeries.candlestick.setMarkers([]);
    }

    // 3. 设置成交量 VOL
    if (volSeries && volumeBars) {
      volSeries.volume.setData(
        volumeBars.map((v) => ({
          time: v.time as Time,
          value: v.value,
          color: v.color,
        }))
      );
    }

    // 4. 设置 Pane 3 技术指标 (MACD 或 RSI(14))
    if (indSeries) {
      if (selectedIndicator === 'RSI') {
        indSeries.dif.setData([]);
        indSeries.dea.setData([]);
        indSeries.macdBar.setData([]);

        indSeries.rsi.setData(rsiData.map((d) => ({ time: d.time as Time, value: d.value })));
      } else {
        indSeries.rsi.setData([]);

        if (macdData) {
          indSeries.dif.setData(macdData.dif.map((d) => ({ time: d.time as Time, value: d.value })));
          indSeries.dea.setData(macdData.dea.map((d) => ({ time: d.time as Time, value: d.value })));
          indSeries.macdBar.setData(
            macdData.macdBar.map((m) => ({
              time: m.time as Time,
              value: m.value,
              color: m.color,
            }))
          );
        }
      }
    }

    // 自动强同步自适应 3 窗格视口数据范围
    if (chartMainRef.current) chartMainRef.current.timeScale().fitContent();
    if (chartVolRef.current) chartVolRef.current.timeScale().fitContent();
    if (chartIndRef.current) chartIndRef.current.timeScale().fitContent();
  }, [ohlcBars, volumeBars, maData, macdData, rsiData, markers, selectedIndicator]);

  // 最新 Bar 数据 (默认展现)
  const lastBar = ohlcBars.length > 0 ? ohlcBars[ohlcBars.length - 1] : null;
  const activeBar = hoverInfo || lastBar;
  const changePct = activeBar ? (((activeBar.close - activeBar.open) / activeBar.open) * 100).toFixed(2) : '0.00';
  const isUp = activeBar ? activeBar.close >= activeBar.open : true;
  const activeColor = isUp ? upColor : downColor;

  return (
    <div className="flex flex-col space-y-2 w-full bg-slate-950 p-3 rounded-2xl border border-slate-800/80 shadow-2xl">
      {/* 图表顶栏控制条: Bar 数量选择 & 副图指标切换 */}
      <div className="flex items-center justify-between px-3 py-1.5 bg-slate-900/90 rounded-xl border border-slate-800 text-xs">
        {/* 左侧: 均线与买卖点标记说明 */}
        <div className="flex items-center space-x-4">
          <span className="flex items-center font-medium text-slate-300">
            <Activity className="w-3.5 h-3.5 text-cyan-400 mr-1.5" />
            三窗格强同步 K 线图
          </span>
          <div className="flex items-center space-x-3 text-[11px] font-mono">
            <span className="text-amber-400">● MA5</span>
            <span className="text-purple-400">● MA10</span>
            <span className="text-cyan-400">● MA20</span>
            <span className="text-slate-400">● MA60</span>
            <span className="font-bold" style={{ color: upColor }}>▲ 买(B)</span>
            <span className="font-bold" style={{ color: downColor }}>▼ 卖(S)</span>
          </div>
        </div>

        {/* 右侧: Bar 数控制与指标下拉 */}
        <div className="flex items-center space-x-3">
          {/* Bar 数快捷按钮 */}
          <div className="flex items-center bg-slate-950 rounded-lg p-0.5 border border-slate-800 font-mono">
            {[250, 500, 1000, 0].map((limit) => (
              <button
                key={limit}
                onClick={() => onBarLimitChange(limit)}
                className={`px-2 py-0.5 rounded text-[10px] transition-colors cursor-pointer ${
                  barLimit === limit
                    ? 'bg-cyan-500 text-slate-950 font-bold'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {limit === 0 ? 'ALL' : `${limit}Bars`}
              </button>
            ))}
          </div>

          {/* 窗格 3 技术指标切换菜单 */}
          <div className="flex items-center space-x-1 bg-slate-950 px-2 py-1 rounded-lg border border-slate-800">
            <Sliders className="w-3 h-3 text-cyan-400" />
            <select
              id="indicatorSelect"
              name="indicatorSelect"
              aria-label="选择窗格3技术指标"
              value={selectedIndicator}
              onChange={(e) => onIndicatorChange && onIndicatorChange(e.target.value)}
              className="bg-transparent text-[11px] text-slate-200 focus:outline-none cursor-pointer"
            >
              <option value="MACD" className="bg-slate-900">窗格3: MACD 指标</option>
              <option value="RSI" className="bg-slate-900">窗格3: RSI (14) 相对强弱</option>
            </select>
          </div>
        </div>
      </div>

      {/* 1. 主图 Pane (K线 + 悬浮探针 Status Overlay) */}
      <div className="relative rounded-xl overflow-hidden border border-slate-800/60">
        {/* Crosshair Hover 光敏探针 Legend */}
        {activeBar && (
          <div className="absolute top-2 left-3 z-10 text-[11px] font-mono bg-slate-950/80 px-2.5 py-1 rounded-lg border border-slate-800/80 backdrop-blur-sm pointer-events-none flex items-center space-x-3 text-slate-300">
            <span className="flex items-center text-slate-400">
              <Target className="w-3 h-3 text-cyan-400 mr-1" />
              {activeBar.time}
            </span>
            <span>开: <span style={{ color: activeColor }}>{activeBar.open}</span></span>
            <span>高: <span style={{ color: activeColor }}>{activeBar.high}</span></span>
            <span>低: <span style={{ color: activeColor }}>{activeBar.low}</span></span>
            <span>收: <span style={{ color: activeColor }}>{activeBar.close}</span></span>
            <span>幅: <span style={{ color: activeColor }}>{Number(changePct) >= 0 ? `+${changePct}%` : `${changePct}%`}</span></span>
            <span className="text-slate-400">VOL: <span className="text-cyan-300">{activeBar.volume.toLocaleString()}</span></span>
          </div>
        )}
        <div ref={containerMainRef} className="w-full" />
      </div>

      {/* 2. 成交量 VOL Pane */}
      <div className="relative rounded-xl overflow-hidden border border-slate-800/60">
        <div className="absolute top-1.5 left-3 z-10 text-[10px] font-mono text-slate-400 pointer-events-none">
          副图1: 成交量 VOL (柱状图)
        </div>
        <div ref={containerVolRef} className="w-full" />
      </div>

      {/* 3. 技术指标 MACD / RSI Pane */}
      <div className="relative rounded-xl overflow-hidden border border-slate-800/60">
        <div className="absolute top-1.5 left-3 z-10 text-[10px] font-mono text-slate-400 pointer-events-none">
          副图2: {selectedIndicator === 'RSI' ? 'RSI (14) 相对强弱指标' : 'MACD (DIF 快线 / DEA 慢线 / MACD 能量柱)'}
        </div>
        <div ref={containerIndRef} className="w-full" />
      </div>
    </div>
  );
};
