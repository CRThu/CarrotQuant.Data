/**
 * web/src/services/pinyin.ts
 *
 * 极速拼音首字母提取与股票/板块多条件模糊匹配工具。
 * 支持按股票代码 (如 sh.600000 / 600000)、中文名称 (如 浦发银行) 和拼音首字母 (如 pfyh) 进行高效搜索。
 */

// 常见金融词汇与 A 股中文汉字首字母映射对照表 (高频汉字覆盖)
const PINYIN_DICT: Record<string, string> = {
  // A
  '平安': 'PA', '安': 'A', '爱': 'A', '奥': 'A', '阿': 'A',
  // B
  '百': 'B', '北': 'B', '宝': 'B', '保': 'B', '博': 'B', '波': 'B', '奔': 'B', '邦': 'B', '半': 'B', '备': 'B', '变': 'B',
  // C
  '长': 'C', '城': 'C', '中': 'Z', '创': 'C', '重': 'Z', '成': 'C', '超': 'C', '春': 'C', '财': 'C', '招': 'Z', '车': 'C',
  // D
  '大': 'D', '德': 'D', '东': 'D', '达': 'D', '地': 'D', '电': 'D', '鼎': 'D', '低': 'D', '多': 'D', '汽': 'Q', '网': 'W',
  // E
  '恩': 'E', '尔': 'E',
  // F
  '发': 'F', '方': 'F', '富': 'F', '丰': 'F', '复': 'F', '飞': 'F', '风': 'F', '福': 'F', '辅': 'F',
  // G
  '国': 'G', '工': 'G', '广': 'G', '光': 'G', '高': 'G', '格': 'G', '港': 'G', '贵': 'G', '古': 'G', '感': 'G',
  // H
  '华': 'H', '海': 'H', '航': 'H', '和': 'H', '宏': 'H', '恒': 'H', '合': 'H', '红': 'H', '环': 'H', '汇': 'H', '黑': 'H',
  // J
  '金': 'J', '建': 'J', '江': 'J', '京': 'J', '佳': 'J', '精': 'J', '九': 'J', '吉': 'J', '技': 'J', '交': 'J', '军': 'J',
  // K
  '科': 'K', '康': 'K', '开': 'K', '快': 'K', '昆': 'K',
  // L
  '立': 'L', '联': 'L', '隆': 'L', '利': 'L', '鲁': 'L', '龙': 'L', '领': 'L', '绿': 'L', '雷': 'L', '兰': 'L', '蓝': 'L',
  // M
  '明': 'M', '美': 'M', '迈': 'M', '蒙': 'M', '木': 'M', '铭': 'M', '茂': 'M',
  // N
  '南': 'N', '农': 'N', '宁': 'N', '能': 'N', '纽': 'N', '纳': 'N',
  // O
  '欧': 'O',
  // P
  '浦': 'P', '普': 'P', '平': 'P', '鹏': 'P', '蓬': 'P',
  // Q
  '青': 'Q', '齐': 'Q', '企': 'Q', '全': 'Q', '强': 'Q', '奇': 'Q', '群': 'Q', '轻': 'Q',
  // R
  '人': 'R', '融': 'R', '瑞': 'R', '日': 'R', '荣': 'R', '润': 'R', '仁': 'R',
  // S
  '上': 'S', '三': 'S', '神': 'S', '顺': 'S', '申': 'S', '双': 'S', '胜': 'S', '圣': 'S', '盛': 'S', '赛': 'S', '山': 'S', '数': 'S',
  // T
  '通': 'T', '天': 'T', '太': 'T', '同': 'T', '泰': 'T', '唐': 'T', '铁': 'T', '拓': 'T', '特': 'T',
  // W
  '万': 'W', '潍': 'W', '微': 'W', '威': 'W', '伟': 'W', '维': 'W', '无': 'W',
  // X
  '新': 'X', '信': 'X', '兴': 'X', '西': 'X', '香': 'X', '夏': 'X', '协': 'X', '星': 'X', '智': 'Z', '先': 'X', '心': 'X',
  // Y
  '银': 'Y', '行': 'H', '药': 'Y', '亚': 'Y', '洋': 'Y', '益': 'Y', '元': 'Y', '一': 'Y', '易': 'Y', '亿': 'Y', '源': 'Y', '英': 'Y', '永': 'Y',
  // Z
  '浙': 'Z', '紫': 'Z', '正': 'Z', '振': 'Z', '众': 'Z', '卓': 'Z', '资': 'Z',
};

// 汉字首字母 Unicode 规范分界算法 (通用拼音索引)
export function getCharPinyinInitial(char: string): string {
  if (!char) return '';
  if (/[a-zA-Z0-9]/.test(char)) return char.toUpperCase();
  if (PINYIN_DICT[char]) return PINYIN_DICT[char];

  // Unicode 汉字转拼音首字母 GB2312 编码估计
  const unicode = char.charCodeAt(0);
  if (unicode >= 19968 && unicode <= 40869) {
    return getUnicodeInitialFallback(char);
  }
  return '';
}

function getUnicodeInitialFallback(char: string): string {
  const boundaryLetters = 'ABCDEFGHJKLMNOPQRSTWXYZ';
  const boundaryChars = '妸簸澈妸发妸妸讥咔垃妸拏噢<ctrl42>七亽仨他妸妸妸夕丫帀';
  for (let i = 0; i < boundaryChars.length; i++) {
    if (boundaryChars.charAt(i).localeCompare(char, 'zh-CN-u-co-pinyin') >= 0) {
      return boundaryLetters.charAt(i);
    }
  }
  return '';
}

/**
 * 将中文字符串转换为拼音首字母大写串 (例如: "浦发银行" -> "PFYH")
 */
export function toPinyinInitials(text: string): string {
  if (!text) return '';
  let result = '';
  for (let i = 0; i < text.length; i++) {
    const char = text.charAt(i);
    if (/[a-zA-Z0-9\.\-\_]/.test(char)) {
      result += char.toUpperCase();
    } else {
      result += getCharPinyinInitial(char);
    }
  }
  return result;
}

export interface SearchableItem {
  code: string;       // 代码 (如 sh.600000 或 600000)
  name: string;       // 名称 (如 浦发银行)
  subText?: string;   // 辅助说明 (如 概念/行业/代码)
}

/**
 * 校验条目是否匹配 Query（同时匹配代码、名称、拼音首字母）
 */
export function matchItem<T extends SearchableItem>(item: T, query: string): boolean {
  if (!query || !query.trim()) return true;
  const q = query.trim().toLowerCase();

  // 1. 匹配代码 (包含去缀纯数字代码匹配, 如 sh.600000 匹配 600)
  const codeLower = (item.code || '').toLowerCase();
  const rawNumCode = codeLower.replace(/^(sh|sz|bj)\./, '');
  if (codeLower.includes(q) || rawNumCode.includes(q)) {
    return true;
  }

  // 2. 匹配中文名称
  const nameLower = (item.name || '').toLowerCase();
  if (nameLower.includes(q)) {
    return true;
  }

  // 3. 匹配拼音首字母
  const pinyinInitials = toPinyinInitials(item.name).toLowerCase();
  if (pinyinInitials.includes(q)) {
    return true;
  }

  return false;
}
