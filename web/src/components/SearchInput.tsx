import React, { useState, useEffect, useRef, useDeferredValue } from 'react';
import { Search, ArrowRight, X } from 'lucide-react';
import type { SearchableItem } from '../services/pinyin';
import { stockDictionary } from '../services/stockDictionary';

interface SearchInputProps<T extends SearchableItem> {
  items?: T[];
  onSelect: (item: T) => void;
  placeholder?: string;
  className?: string;
  inputClassName?: string;
  autoFocus?: boolean;
  value?: string;
  onChange?: (val: string) => void;
}

export function SearchInput<T extends SearchableItem>({
  items,
  onSelect,
  placeholder = '搜索代码/名称/拼音 (如 00 / 600000 / 浦发 / pfyh)...',
  className = '',
  inputClassName = '',
  autoFocus = false,
  value: externalValue,
  onChange: externalOnChange,
}: SearchInputProps<T>) {
  const [query, setQuery] = useState<string>(externalValue || '');
  const deferredQuery = useDeferredValue(query);
  const [isOpen, setIsOpen] = useState<boolean>(false);
  const [selectedIndex, setSelectedIndex] = useState<number>(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const dropdownRef = useRef<HTMLDivElement | null>(null);

  // 同步外部受控状态
  useEffect(() => {
    if (externalValue !== undefined) {
      setQuery(externalValue);
    }
  }, [externalValue]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    setQuery(val);
    setSelectedIndex(0);
    setIsOpen(true);
    if (externalOnChange) {
      externalOnChange(val);
    }
  };

  // 极速匹配候选建议 (耗时 < 1ms，支持 pure 00/600/拼音/名称搜索)
  const matchedItems = React.useMemo(() => {
    if (!deferredQuery.trim()) return [];

    const q = deferredQuery.trim().toLowerCase();

    // 如果未传入 custom items，默认直接走高性能 stockDictionary 全量字典
    if (!items || items.length === 0) {
      const dictResults = stockDictionary.search(q, 20);
      return dictResults.map(
        (d) =>
          ({
            code: d.symbol,
            name: d.name,
            subText: d.market,
          }) as T
      );
    }

    // 针对传入的 items 列表进行无阻塞、非 localeCompare 极速字符串过滤
    const results: T[] = [];
    const rawQ = q.replace(/^(sh|sz|bj)\./, '');

    // 1. 优先前缀精确匹配 (如 输入 "00" 匹配 sz.000001, sz.000002)
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const codeLower = (item.code || '').toLowerCase();
      const nameLower = (item.name || '').toLowerCase();
      const rawCode = codeLower.replace(/^(sh|sz|bj)\./, '');
      const pinyinLower = ((item as any).pinyin || '').toLowerCase();

      if (
        rawCode.startsWith(rawQ) ||
        codeLower.startsWith(q) ||
        nameLower.startsWith(q) ||
        (pinyinLower && pinyinLower.startsWith(q))
      ) {
        results.push(item);
        if (results.length >= 20) return results;
      }
    }

    // 2. 包含子串匹配
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (results.includes(item)) continue;

      const codeLower = (item.code || '').toLowerCase();
      const nameLower = (item.name || '').toLowerCase();
      const rawCode = codeLower.replace(/^(sh|sz|bj)\./, '');
      const pinyinLower = ((item as any).pinyin || '').toLowerCase();

      if (
        rawCode.includes(rawQ) ||
        codeLower.includes(q) ||
        nameLower.includes(q) ||
        (pinyinLower && pinyinLower.includes(q))
      ) {
        results.push(item);
        if (results.length >= 20) return results;
      }
    }

    return results;
  }, [items, deferredQuery]);

  // 键盘导航 (Up, Down, Enter, Esc)
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!isOpen || matchedItems.length === 0) {
      if (e.key === 'Enter' && query.trim()) {
        const q = query.trim().toLowerCase();
        // 尝试从字典或候选集精确定位
        const dictItem = stockDictionary.get(q) || stockDictionary.search(q, 1)[0];
        if (dictItem) {
          onSelect({
            code: dictItem.symbol,
            name: dictItem.name,
            subText: dictItem.market,
          } as T);
          setIsOpen(false);
        } else if (items && items.length > 0) {
          const matched = items.find((i) => i.code.toLowerCase().includes(q));
          if (matched) {
            onSelect(matched);
            setIsOpen(false);
          }
        }
      }
      return;
    }

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIndex((prev) => (prev + 1) % matchedItems.length);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIndex((prev) => (prev - 1 + matchedItems.length) % matchedItems.length);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const target = matchedItems[selectedIndex] || matchedItems[0];
      if (target) {
        onSelect(target);
        setIsOpen(false);
      }
    } else if (e.key === 'Escape') {
      setIsOpen(false);
    }
  };

  // 点击外部收起下拉框
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        inputRef.current &&
        !inputRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleClear = () => {
    setQuery('');
    setIsOpen(false);
    if (externalOnChange) externalOnChange('');
  };

  return (
    <div className={`relative ${className}`}>
      <div className="relative flex items-center">
        <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={handleChange}
          onFocus={() => query.trim() && setIsOpen(true)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          autoFocus={autoFocus}
          className={`bg-slate-950/90 border border-slate-800 focus:border-cyan-500 rounded-xl pl-9 pr-8 py-1.5 text-xs text-slate-100 placeholder-slate-500 font-sans focus:outline-none transition-all shadow-inner w-full ${inputClassName}`}
        />
        {query && (
          <button
            onClick={handleClear}
            className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300 p-0.5 cursor-pointer"
          >
            <X className="w-3.5 h-3.5" />
          </button>
        )}
      </div>

      {/* 搜索下拉建议面板 Autocomplete Popover */}
      {isOpen && matchedItems.length > 0 && (
        <div
          ref={dropdownRef}
          className="absolute z-50 left-0 right-0 mt-1 bg-slate-900/95 border border-slate-800 rounded-xl shadow-2xl overflow-hidden backdrop-blur-md max-h-64 overflow-y-auto divide-y divide-slate-800/40"
        >
          {matchedItems.map((item, index) => {
            const isFocused = index === selectedIndex;
            return (
              <div
                key={item.code}
                onClick={() => {
                  onSelect(item);
                  setIsOpen(false);
                }}
                onMouseEnter={() => setSelectedIndex(index)}
                className={`px-3 py-2 flex items-center justify-between cursor-pointer transition-colors ${
                  isFocused ? 'bg-cyan-950/60 text-cyan-300 border-l-2 border-cyan-400' : 'text-slate-300 hover:bg-slate-800/50'
                }`}
              >
                <div className="flex items-center space-x-2.5 overflow-hidden">
                  <span className="font-bold text-xs font-sans truncate">{item.name}</span>
                  <span className="font-mono text-[10px] text-slate-400 bg-slate-950 px-1.5 py-0.5 rounded border border-slate-800 shrink-0">
                    {item.code}
                  </span>
                </div>
                {item.subText && (
                  <span className="text-[10px] text-slate-500 font-mono truncate ml-2">{item.subText}</span>
                )}
                {isFocused && <ArrowRight className="w-3 h-3 text-cyan-400 shrink-0 ml-1" />}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
