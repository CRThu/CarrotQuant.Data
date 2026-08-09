/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        dark: {
          bg: '#0f172a',
          card: '#1e293b',
          border: '#334155',
          hover: '#1e293b',
          muted: '#64748b',
        },
        stock: {
          up: '#ef4444',      // A股涨 - 红色
          down: '#22c55e',    // A股跌 - 绿色
          flat: '#94a3b8',    // 平盘 - 灰色
        }
      }
    },
  },
  plugins: [],
}
