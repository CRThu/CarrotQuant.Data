import { createChart, ColorType, CrosshairMode } from 'lightweight-charts';
import type { IChartApi, ISeriesApi, Time } from 'lightweight-charts';
import type { OHLCBar, HistogramBar, MovingAverageData, ColorMode, BSMarkerItem } from '../types/api';
import { getUpDownColors } from '../types/api';

export interface ChartEngineMountOptions {
  colorMode?: ColorMode;
  onCrosshairMove?: (bar: OHLCBar | null) => void;
}

/**
 * 封装 TradingView Lightweight Charts 的轻量 2D/WebGL 画布引擎
 * 职责：负责图表实例挂载、手势交互强同步、数据增量 setData、配色切换与平滑 resize。
 * 优势：与 React 解耦，绝不上网重新销毁/创建 Canvas，性能维持 60 FPS 满帧。
 */
export class KLineCanvasEngine {
  private chartMain: IChartApi | null = null;
  private chartVol: IChartApi | null = null;

  private candlestickSeries: ISeriesApi<'Candlestick'> | null = null;
  private ma5Series: ISeriesApi<'Line'> | null = null;
  private ma10Series: ISeriesApi<'Line'> | null = null;
  private ma20Series: ISeriesApi<'Line'> | null = null;
  private volumeSeries: ISeriesApi<'Histogram'> | null = null;

  private colorMode: ColorMode = 'redUpGreenDown';
  private onCrosshairMoveCb?: (bar: OHLCBar | null) => void;
  private lastDataKey: string = '';

  /**
   * 挂载画布并建立主副图强同步
   */
  public mount(
    mainContainer: HTMLDivElement,
    volContainer: HTMLDivElement,
    options: ChartEngineMountOptions = {}
  ): void {
    this.colorMode = options.colorMode || 'redUpGreenDown';
    this.onCrosshairMoveCb = options.onCrosshairMove;

    const { upColor, downColor } = getUpDownColors(this.colorMode);

    const commonOptions = {
      layout: {
        background: { type: ColorType.Solid, color: '#090d16' },
        textColor: '#94a3b8',
        fontSize: 11,
        fontFamily: 'sans-serif',
      },
      watermark: { visible: false },
      grid: {
        vertLines: { color: '#162032' },
        horzLines: { color: '#162032' },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: '#1e293b' },
      timeScale: { borderColor: '#1e293b', timeVisible: true, secondsVisible: false },
    };

    // 1. 主图 K 线 (开启全套缩放与滚轮手势)
    this.chartMain = createChart(mainContainer, {
      ...commonOptions,
      height: 320,
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
    });

    this.candlestickSeries = this.chartMain.addCandlestickSeries({
      upColor,
      downColor,
      borderVisible: false,
      wickUpColor: upColor,
      wickDownColor: downColor,
    });

    this.ma5Series = this.chartMain.addLineSeries({ color: '#eab308', lineWidth: 1, title: 'MA5' });
    this.ma10Series = this.chartMain.addLineSeries({ color: '#a855f7', lineWidth: 1, title: 'MA10' });
    this.ma20Series = this.chartMain.addLineSeries({ color: '#06b6d4', lineWidth: 1, title: 'MA20' });

    // 十字光标悬浮监听
    this.chartMain.subscribeCrosshairMove((param) => {
      if (!this.onCrosshairMoveCb) return;

      if (!param || !param.time || param.point === undefined || param.point.x < 0 || param.point.y < 0) {
        this.onCrosshairMoveCb(null);
        return;
      }

      if (this.candlestickSeries) {
        const data = param.seriesData.get(this.candlestickSeries) as any;
        if (data && typeof data.close === 'number') {
          this.onCrosshairMoveCb({
            time: String(param.time),
            open: data.open,
            high: data.high,
            low: data.low,
            close: data.close,
            volume: data.volume ?? 0,
          });
        }
      }
    });

    // 2. 附图 成交量 VOL (由主图单向指挥)
    this.chartVol = createChart(volContainer, {
      ...commonOptions,
      height: 120,
      handleScale: false,
      handleScroll: false,
    });

    this.volumeSeries = this.chartVol.addHistogramSeries({
      priceFormat: { type: 'volume' },
    });

    // 单向 Master -> Follower 零冲突强同步
    this.chartMain.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (range && this.chartVol) {
        try {
          this.chartVol.timeScale().setVisibleLogicalRange(range);
        } catch {
          // 静默
        }
      }
    });
  }

  /**
   * 增量更新 K 线、均线与成交量数据 (绝不上网重新销毁/新建 Canvas)
   */
  public updateData(
    ohlcBars: OHLCBar[],
    volumeBars: HistogramBar[],
    maData: MovingAverageData,
    markers?: BSMarkerItem[]
  ): void {
    if (!this.candlestickSeries) return;

    if (!ohlcBars || ohlcBars.length === 0) {
      this.clear();
      return;
    }

    // 1. 设置 K 线
    this.candlestickSeries.setData(
      ohlcBars.map((b) => ({
        time: b.time as Time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      }))
    );

    // 如果有买卖点 Markers 标注，自动上图
    if (markers && markers.length > 0) {
      this.candlestickSeries.setMarkers(
        markers.map((m) => ({
          time: m.time as Time,
          position: m.position,
          color: m.color,
          shape: m.shape,
          text: m.text,
        }))
      );
    }

    // 2. 设置 MA 均线
    if (this.ma5Series && maData.ma5) {
      this.ma5Series.setData(maData.ma5.map((d) => ({ time: d.time as Time, value: d.value })));
    }
    if (this.ma10Series && maData.ma10) {
      this.ma10Series.setData(maData.ma10.map((d) => ({ time: d.time as Time, value: d.value })));
    }
    if (this.ma20Series && maData.ma20) {
      this.ma20Series.setData(maData.ma20.map((d) => ({ time: d.time as Time, value: d.value })));
    }

    // 3. 设置成交量 VOL
    if (this.volumeSeries && volumeBars) {
      this.volumeSeries.setData(
        volumeBars.map((v) => ({
          time: v.time as Time,
          value: v.value,
          color: v.color,
        }))
      );
    }

    // 仅在数据 key 发生改变时自适应视口
    const currentDataKey = `${ohlcBars[0]?.time}-${ohlcBars[ohlcBars.length - 1]?.time}-${ohlcBars.length}`;
    if (this.lastDataKey !== currentDataKey && this.chartMain) {
      this.lastDataKey = currentDataKey;
      this.chartMain.timeScale().fitContent();
    }
  }

  /**
   * 动态切换红涨绿跌 / 绿涨红跌双色
   */
  public updateColors(colorMode: ColorMode): void {
    this.colorMode = colorMode;
    const { upColor, downColor } = getUpDownColors(colorMode);

    if (this.candlestickSeries) {
      this.candlestickSeries.applyOptions({
        upColor,
        downColor,
        wickUpColor: upColor,
        wickDownColor: downColor,
      });
    }
  }

  /**
   * 清空 Series 数据 (切换股票时极速置空，保留 Canvas)
   */
  public clear(): void {
    if (this.candlestickSeries) this.candlestickSeries.setData([]);
    if (this.ma5Series) this.ma5Series.setData([]);
    if (this.ma10Series) this.ma10Series.setData([]);
    if (this.ma20Series) this.ma20Series.setData([]);
    if (this.volumeSeries) this.volumeSeries.setData([]);
    this.lastDataKey = '';
  }

  /**
   * 平滑 resize 画布宽度与高度
   */
  public resize(width: number, height: number): void {
    if (width <= 0 || height <= 0) return;
    const availableH = Math.max(200, height - 42); // 扣除 Header 工具栏
    const mainH = Math.floor(availableH * 0.7);
    const volH = Math.max(60, availableH - mainH - 6);

    if (this.chartMain) this.chartMain.applyOptions({ width, height: mainH });
    if (this.chartVol) this.chartVol.applyOptions({ width, height: volH });
  }

  /**
   * 销毁并释放实例
   */
  public destroy(): void {
    this.clear();
    if (this.chartMain) {
      this.chartMain.remove();
      this.chartMain = null;
    }
    if (this.chartVol) {
      this.chartVol.remove();
      this.chartVol = null;
    }
    this.candlestickSeries = null;
    this.ma5Series = null;
    this.ma10Series = null;
    this.ma20Series = null;
    this.volumeSeries = null;
  }
}
