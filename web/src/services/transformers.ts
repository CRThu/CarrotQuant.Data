import type { OHLCBar, HistogramBar, QueryMatrixResponse } from '../types/api';

/**
 * 将后端 /api/v1/query 返回的 2D 矩阵转换为 TradingView Lightweight Charts 适配格式
 */
export const matrixToOHLC = (matrix: QueryMatrixResponse, maxBars?: number): OHLCBar[] => {
  if (!matrix || !matrix.columns || !matrix.data || matrix.data.length === 0) {
    return [];
  }

  const colMap = new Map<string, number>();
  matrix.columns.forEach((col, idx) => colMap.set(col.toLowerCase(), idx));

  const idxOpen = colMap.get('open') ?? -1;
  const idxHigh = colMap.get('high') ?? -1;
  const idxLow = colMap.get('low') ?? -1;
  const idxClose = colMap.get('close') ?? -1;
  const idxVolume = colMap.get('volume') ?? -1;
  const idxAmount = colMap.get('amount') ?? -1;
  const idxDatetime = colMap.get('datetime') ?? -1;
  const idxTimestamp = colMap.get('timestamp') ?? -1;

  if (idxClose === -1) {
    return [];
  }

  const bars: OHLCBar[] = [];

  for (const row of matrix.data) {
    let dateStr = '';

    if (idxDatetime !== -1 && row[idxDatetime]) {
      // 兼容 ISO8601 日线 (YYYY-MM-DD) 与高频/分钟线 (YYYY-MM-DD HH:mm)
      const rawDt = String(row[idxDatetime]);
      if (rawDt.includes('T') && rawDt.length >= 16) {
        const datePart = rawDt.substring(0, 10);
        const timePart = rawDt.substring(11, 16);
        const isIntraday =
          matrix.table_id?.includes('.5m.') ||
          matrix.table_id?.includes('.1m.') ||
          (timePart !== '15:00' && timePart !== '00:00');
        dateStr = isIntraday ? `${datePart} ${timePart}` : datePart;
      } else {
        dateStr = rawDt.substring(0, 10);
      }
    } else if (idxTimestamp !== -1 && row[idxTimestamp]) {
      // 毫秒时间戳转换为 ISO/UTC 时间
      const ts = Number(row[idxTimestamp]);
      const d = new Date(ts);
      if (!isNaN(d.getTime())) {
        const yyyy = d.getUTCFullYear();
        const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
        const dd = String(d.getUTCDate()).padStart(2, '0');
        const isIntraday = matrix.table_id?.includes('.5m.') || matrix.table_id?.includes('.1m.');
        if (isIntraday) {
          const hh = String(d.getUTCHours()).padStart(2, '0');
          const min = String(d.getUTCMinutes()).padStart(2, '0');
          dateStr = `${yyyy}-${mm}-${dd} ${hh}:${min}`;
        } else {
          dateStr = `${yyyy}-${mm}-${dd}`;
        }
      }
    }

    if (!dateStr) continue;

    const open = idxOpen !== -1 ? Number(row[idxOpen]) : Number(row[idxClose]);
    const high = idxHigh !== -1 ? Number(row[idxHigh]) : Number(row[idxClose]);
    const low = idxLow !== -1 ? Number(row[idxLow]) : Number(row[idxClose]);
    const close = Number(row[idxClose]);
    const volume = idxVolume !== -1 ? Number(row[idxVolume]) : 0;
    const amount = idxAmount !== -1 ? Number(row[idxAmount]) : 0;

    bars.push({
      time: dateStr,
      open,
      high,
      low,
      close,
      volume,
      amount
    });
  }

  // 按时间升序排序并去重
  const uniqueMap = new Map<string, OHLCBar>();
  bars.forEach(bar => uniqueMap.set(bar.time, bar));
  const sortedBars = Array.from(uniqueMap.values()).sort((a, b) => (a.time > b.time ? 1 : -1));

  // 如果指定了最大条数 Limit，截取最近的 N 条 Bar
  if (maxBars && maxBars > 0 && sortedBars.length > maxBars) {
    return sortedBars.slice(sortedBars.length - maxBars);
  }

  return sortedBars;
};

import { getUpDownColors, type ColorMode } from '../types/api';

/**
 * 从 OHLC 数据生成带动态配色的成交量 Histogram 数据系列 (支持 A股红涨绿跌 与 美股绿涨红跌)
 */
export const ohlcToVolume = (
  bars: OHLCBar[],
  colorMode: ColorMode = 'redUpGreenDown'
): HistogramBar[] => {
  const { upColor, downColor } = getUpDownColors(colorMode);
  return bars.map((bar) => {
    const isUp = bar.close >= bar.open;
    return {
      time: bar.time,
      value: bar.volume,
      color: isUp ? upColor : downColor,
    };
  });
};
