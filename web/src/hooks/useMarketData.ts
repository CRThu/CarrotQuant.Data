import { useState, useEffect, useCallback, useMemo } from 'react';
import { DATA_SOURCE_OPTIONS } from '../types/api';
import type {
  OHLCBar,
  HistogramBar,
  MovingAverageData,
  MACDResult,
  BSMarkerItem,
  QueryMatrixResponse,
} from '../types/api';
import { apiClient } from '../services/apiClient';
import { matrixToOHLC, ohlcToVolume } from '../services/transformers';
import { calculateMAMulti, calculateMACD, calculateRSI, deriveBSMarkers } from '../services/indicators';
import type { ColorMode, LineDataPoint } from '../types/api';

export interface UseMarketDataReturn {
  tableId: string;
  setTableId: (id: string) => void;
  symbol: string;
  setSymbol: (sym: string) => void;
  barLimit: number;
  setBarLimit: (limit: number) => void;
  selectedIndicator: string;
  setSelectedIndicator: (ind: string) => void;
  loading: boolean;
  error: string | null;
  ohlcBars: OHLCBar[];
  volumeBars: HistogramBar[];
  maData: MovingAverageData;
  macdData: MACDResult;
  rsiData: LineDataPoint[];
  markers: BSMarkerItem[];
  matrixRaw: QueryMatrixResponse | null;
  setExternalMarkers: (markers: BSMarkerItem[]) => void;
  refreshData: () => void;
}

export const useMarketData = (
  initialTableId: string = DATA_SOURCE_OPTIONS[0].table_id,
  initialSymbol: string = 'sh.600000',
  colorMode: ColorMode = 'redUpGreenDown'
): UseMarketDataReturn => {
  const [tableId, setTableId] = useState<string>(initialTableId);
  const [symbol, setSymbol] = useState<string>(initialSymbol);
  const [barLimit, setBarLimit] = useState<number>(250); // 默认加载 250 条 Bars (1年)
  const [selectedIndicator, setSelectedIndicator] = useState<string>('MACD');
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const [ohlcBars, setOhlcBars] = useState<OHLCBar[]>([]);
  const [volumeBars, setVolumeBars] = useState<HistogramBar[]>([]);
  const [maData, setMaData] = useState<MovingAverageData>({ ma5: [], ma10: [], ma20: [], ma60: [] });
  const [macdData, setMacdData] = useState<MACDResult>({ dif: [], dea: [], macdBar: [] });
  const [rsiData, setRsiData] = useState<LineDataPoint[]>([]);
  const [derivedMarkers, setDerivedMarkers] = useState<BSMarkerItem[]>([]);
  const [externalMarkers, setExternalMarkers] = useState<BSMarkerItem[]>([]);
  const [matrixRaw, setMatrixRaw] = useState<QueryMatrixResponse | null>(null);

  // 在 Render 阶段同步 Prop 变动，彻底消除切换/加载标的时产生的二次串行 HTTP 请求 (1.6s -> 0.2s)
  const [prevInitialTableId, setPrevInitialTableId] = useState(initialTableId);
  const [prevInitialSymbol, setPrevInitialSymbol] = useState(initialSymbol);

  if (initialTableId !== prevInitialTableId) {
    setPrevInitialTableId(initialTableId);
    setTableId(initialTableId);
  }

  if (initialSymbol !== prevInitialSymbol) {
    setPrevInitialSymbol(initialSymbol);
    setSymbol(initialSymbol);
  }

  const fetchData = useCallback(async () => {
    if (!tableId || !symbol) return;
    setLoading(true);
    setError(null);

    try {
      const res = await apiClient.queryData({
        table_id: tableId,
        symbols: symbol,
        page: 1,
        page_size: 5000,
      });

      setMatrixRaw(res);

      // 1. 先用全量历史数据 (无 barLimit 截断) 转换为 OHLC Bars，保证 EMA/MACD/RSI 长期记忆指标计算无失真
      const fullBars = matrixToOHLC(res);
      const fullVols = ohlcToVolume(fullBars, colorMode);
      const fullMas = calculateMAMulti(fullBars);
      const fullMacd = calculateMACD(fullBars, 12, 26, 9, colorMode);
      const fullRsi = calculateRSI(fullBars, 14);
      const fullMarkers = deriveBSMarkers(fullBars, fullMas.ma5, fullMas.ma20, colorMode);

      // 2. 算完完整指标后，统一按 barLimit 进行显示视口切片
      const sliceLimit = <T>(arr: T[]): T[] =>
        barLimit > 0 && arr.length > barLimit ? arr.slice(arr.length - barLimit) : arr;

      setOhlcBars(sliceLimit(fullBars));
      setVolumeBars(sliceLimit(fullVols));
      setMaData({
        ma5: sliceLimit(fullMas.ma5),
        ma10: sliceLimit(fullMas.ma10),
        ma20: sliceLimit(fullMas.ma20),
        ma60: sliceLimit(fullMas.ma60),
      });
      setMacdData({
        dif: sliceLimit(fullMacd.dif),
        dea: sliceLimit(fullMacd.dea),
        macdBar: sliceLimit(fullMacd.macdBar),
      });
      setRsiData(sliceLimit(fullRsi));
      setDerivedMarkers(sliceLimit(fullMarkers));
    } catch (err: any) {
      console.error('Failed to fetch market data:', err);
      setError(err?.response?.data?.detail || err?.message || '获取数据失败');
      setOhlcBars([]);
      setVolumeBars([]);
      setMatrixRaw(null);
    } finally {
      setLoading(false);
    }
  }, [tableId, symbol, barLimit, colorMode]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // 合并派生的金叉死叉 Marker 与外部扩展注入的 Marker (使用 useMemo 缓存 Array 引用防重绘)
  const combinedMarkers = useMemo(
    () => [...derivedMarkers, ...externalMarkers],
    [derivedMarkers, externalMarkers]
  );

  return {
    tableId,
    setTableId,
    symbol,
    setSymbol,
    barLimit,
    setBarLimit,
    selectedIndicator,
    setSelectedIndicator,
    loading,
    error,
    ohlcBars,
    volumeBars,
    maData,
    macdData,
    rsiData,
    markers: combinedMarkers,
    matrixRaw,
    setExternalMarkers,
    refreshData: fetchData,
  };
};
