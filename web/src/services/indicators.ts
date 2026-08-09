import {
  type OHLCBar,
  type LineDataPoint,
  type MovingAverageData,
  type MACDResult,
  type HistogramBar,
  type BSMarkerItem,
  type ColorMode,
  getUpDownColors,
} from '../types/api';

/**
 * 计算简单移动平均线 (Simple Moving Average)
 */
export const calculateMA = (bars: OHLCBar[], period: number): LineDataPoint[] => {
  const result: LineDataPoint[] = [];
  if (bars.length < period) return result;

  let sum = 0;
  for (let i = 0; i < bars.length; i++) {
    sum += bars[i].close;
    if (i >= period) {
      sum -= bars[i - period].close;
    }
    if (i >= period - 1) {
      result.push({
        time: bars[i].time,
        value: Number((sum / period).toFixed(3)),
      });
    }
  }
  return result;
};

/**
 * 批量计算 MA5, MA10, MA20, MA60 均线集合
 */
export const calculateMAMulti = (bars: OHLCBar[]): MovingAverageData => {
  return {
    ma5: calculateMA(bars, 5),
    ma10: calculateMA(bars, 10),
    ma20: calculateMA(bars, 20),
    ma60: calculateMA(bars, 60),
  };
};

/**
 * 计算指数移动平均线 (EMA)
 */
const calculateEMA = (prices: number[], period: number): number[] => {
  const ema: number[] = [];
  if (prices.length === 0) return ema;

  const k = 2 / (period + 1);
  ema[0] = prices[0]; // 初始 EMA 取第 1 个价格

  for (let i = 1; i < prices.length; i++) {
    ema[i] = prices[i] * k + ema[i - 1] * (1 - k);
  }
  return ema;
};

/**
 * 计算 MACD 指标 (DIF 快线, DEA 慢线, MACD 能量柱)，支持 ColorMode 动态配色
 */
export const calculateMACD = (
  bars: OHLCBar[],
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9,
  colorMode: ColorMode = 'redUpGreenDown'
): MACDResult => {
  const prices = bars.map((b) => b.close);
  const emaFast = calculateEMA(prices, fastPeriod);
  const emaSlow = calculateEMA(prices, slowPeriod);
  const { upColor, downColor } = getUpDownColors(colorMode);

  // 计算 DIF (快线) = EMA(12) - EMA(26)
  const difValues: number[] = prices.map((_, i) => emaFast[i] - emaSlow[i]);

  // 计算 DEA (慢线) = EMA(DIF, 9)
  const deaValues = calculateEMA(difValues, signalPeriod);

  const difPoints: LineDataPoint[] = [];
  const deaPoints: LineDataPoint[] = [];
  const macdBars: HistogramBar[] = [];

  for (let i = 0; i < bars.length; i++) {
    const difVal = Number(difValues[i].toFixed(3));
    const deaVal = Number(deaValues[i].toFixed(3));
    // A股常规 MACD 柱 = 2 * (DIF - DEA)
    const macdVal = Number(((difVal - deaVal) * 2).toFixed(3));

    difPoints.push({ time: bars[i].time, value: difVal });
    deaPoints.push({ time: bars[i].time, value: deaVal });
    macdBars.push({
      time: bars[i].time,
      value: macdVal,
      color: macdVal >= 0 ? upColor : downColor,
    });
  }

  return {
    dif: difPoints,
    dea: deaPoints,
    macdBar: macdBars,
  };
};

/**
 * 计算 RSI 相对强弱指标 (Relative Strength Index)
 * 默认周期 period = 14
 */
export const calculateRSI = (bars: OHLCBar[], period = 14): LineDataPoint[] => {
  const result: LineDataPoint[] = [];
  if (bars.length <= period) return result;

  let gains = 0;
  let losses = 0;

  // 初始 14 天的累计 Gain 和 Loss
  for (let i = 1; i <= period; i++) {
    const diff = bars[i].close - bars[i - 1].close;
    if (diff >= 0) {
      gains += diff;
    } else {
      losses -= diff;
    }
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  const calculateRSIValue = (g: number, l: number): number => {
    if (l === 0) return 100;
    const rs = g / l;
    return Number((100 - 100 / (1 + rs)).toFixed(2));
  };

  result.push({
    time: bars[period].time,
    value: calculateRSIValue(avgGain, avgLoss),
  });

  // Wilder 平滑递推算法
  for (let i = period + 1; i < bars.length; i++) {
    const diff = bars[i].close - bars[i - 1].close;
    const currentGain = diff >= 0 ? diff : 0;
    const currentLoss = diff < 0 ? -diff : 0;

    avgGain = (avgGain * (period - 1) + currentGain) / period;
    avgLoss = (avgLoss * (period - 1) + currentLoss) / period;

    result.push({
      time: bars[i].time,
      value: calculateRSIValue(avgGain, avgLoss),
    });
  }

  return result;
};

/**
 * 基于 MA5 与 MA20 交叉算法派生买卖点标记 (Golden / Death Cross Markers)，支持 ColorMode
 */
export const deriveBSMarkers = (
  _bars: OHLCBar[],
  ma5: LineDataPoint[],
  ma20: LineDataPoint[],
  colorMode: ColorMode = 'redUpGreenDown'
): BSMarkerItem[] => {
  const markers: BSMarkerItem[] = [];
  if (ma5.length === 0 || ma20.length === 0) return markers;

  const { upColor, downColor } = getUpDownColors(colorMode);

  // 将 MA20 映射为以 time 为 Key 的字典
  const ma20Map = new Map<string, number>();
  ma20.forEach((item) => ma20Map.set(item.time, item.value));

  for (let i = 1; i < ma5.length; i++) {
    const prevTime = ma5[i - 1].time;
    const currTime = ma5[i].time;

    const prevMA5 = ma5[i - 1].value;
    const currMA5 = ma5[i].value;

    const prevMA20 = ma20Map.get(prevTime);
    const currMA20 = ma20Map.get(currTime);

    if (prevMA20 !== undefined && currMA20 !== undefined) {
      // 金叉：MA5 从下方穿过 MA20 (买入信号)
      if (prevMA5 <= prevMA20 && currMA5 > currMA20) {
        markers.push({
          time: currTime,
          position: 'belowBar',
          color: upColor,
          shape: 'arrowUp',
          text: '买 B',
          size: 1.5,
        });
      }
      // 死叉：MA5 从上方穿过 MA20 (卖出信号)
      else if (prevMA5 >= prevMA20 && currMA5 < currMA20) {
        markers.push({
          time: currTime,
          position: 'aboveBar',
          color: downColor,
          shape: 'arrowDown',
          text: '卖 S',
          size: 1.5,
        });
      }
    }
  }

  return markers;
};
