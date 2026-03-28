# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "akshare>=1.18.39",
#     "baostock>=0.8.9",
#     "fastapi>=0.135.1",
#     "httpx>=0.28.1",
#     "loguru>=0.7.3",
#     "polars>=1.39.0",
#     "pydantic>=2.12.5",
#     "pydantic-settings>=2.13.1",
#     "pyyaml>=6.0.3",
#     "typer>=0.24.1",
#     "uvicorn>=0.41.0",
# ]
# ///

import subprocess
import sys
import os
import importlib
import inspect
import io
from pathlib import Path

if sys.platform == "win32":
    # 强制当前进程输出为 UTF-8
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    # 强制后续所有 subprocess 默认也使用 UTF-8
    os.environ["PYTHONUTF8"] = "1"
    
# 添加对项目路径的引用以便导入 app 模块
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from app.utils.logger_utils import setup_logger

# 初始化日志，每次运行生成独立的时间戳日志文件
setup_logger(log_level="INFO", log_file_prefix="wizard")

def get_project_root():
    """获取项目根目录"""
    return Path(__file__).parent.parent.absolute()

def discover_supported_tables():
    """
    动态扫描 app/provider 下的驱动，获取所有支持的 table_id。
    """
    project_root = get_project_root()
    provider_path = project_root / "app" / "provider"
    
    # 确保 PYTHONPATH 包含项目根目录，以便导入
    sys.path.insert(0, str(project_root))
    
    all_tables = []
    try:
        for file in provider_path.glob("*_provider.py"):
            module_name = f"app.provider.{file.stem}"
            module = importlib.import_module(module_name)
            
            # 寻找继承自 BaseProvider 的类
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and name != "BaseProvider" and name.endswith("Provider"):
                    if hasattr(obj, "_SUPPORTED_TABLE_MAP"):
                        tables = list(obj._SUPPORTED_TABLE_MAP.keys())
                        all_tables.extend(tables)
                    elif hasattr(obj, "get_supported_tables"):
                        try:
                            instance = obj()
                            all_tables.extend(instance.get_supported_tables())
                        except Exception:
                            pass
    except Exception as e:
        print(f"[!] 扫描驱动时出错: {e}")
        return ["ashare.kline.1d.adj.baostock", "ashare.adj_factor.baostock"]
        
    return sorted(list(set(all_tables)))

def start_wizard():
    """启动数据同步向导"""
    print("="*60)
    print("      CarrotQuant.Data 数据同步向导 (Wizard V2.1)")
    print("="*60)
    print("\n[+] 正在加载可用数据表...")
    
    available_tables = discover_supported_tables()
    
    if not available_tables:
        print("[!] 未发现数据表驱动。请检查 app/provider 目录。")
        return

    print("\n[1/5] 选择同步对象: 请通过编号选择或切换")
    
    selected_indices = set()
    
    while True:
        print("\n" + "-"*40)
        for i, table in enumerate(available_tables, 1):
            mark = "[*]" if i in selected_indices else "[ ]"
            print(f"  {mark} {i:2}. {table}")
        print("-" * 40)
        print("指令: [编号] 切换状态 | [*] 全选 | [-] 全不选 | [0] 确认并继续")
        
        selection_input = input("请选择 (直接回车默认选中第1项): ").strip()
        
        if not selection_input:
            if not selected_indices:
                selected_indices.add(1)
            break
            
        if selection_input == '0':
            if not selected_indices:
                print("[!] 请至少选择一个同步对象！")
                continue
            break
            
        if selection_input == '*':
            selected_indices = set(range(1, len(available_tables) + 1))
            continue
            
        if selection_input == '-':
            selected_indices.clear()
            continue
            
        try:
            indices = [int(i.strip()) for i in selection_input.split(',') if i.strip()]
            for idx in indices:
                if 1 <= idx <= len(available_tables):
                    if idx in selected_indices:
                        selected_indices.remove(idx)
                    else:
                        selected_indices.add(idx)
                else:
                    print(f"[!] 编号 {idx} 超出范围。")
        except ValueError:
            print("[!] 输入无效，请重新输入。")

    target_tables = [available_tables[i - 1] for i in sorted(selected_indices)]

    # 2. 设置时间范围
    print("\n[2/5] 设置同步时间范围 (可选):")
    print("      提示: 留空则代表【增量同步】，自动续接断点数据。")
    start_date = input("起始日期 (YYYY-MM-DD) [默认: 自动续接]: ").strip()
    end_date = input("结束日期 (YYYY-MM-DD) [默认: 至今]: ").strip()

    # 3. 存储格式
    print("\n[3/5] 选择存储格式:")
    print("      可用选项: csv, parquet")
    formats_input = input("请输入格式 [默认: csv,parquet]: ").strip()
    formats = formats_input if formats_input else "csv,parquet"

    # 4. 其它同步策略
    print("\n[4/5] 同步策略配置:")
    force_refresh = input("是否强制全量覆盖更新? (y/N): ").strip().lower() == 'y'
    batch_size = input("批处理大小 (Batch Size) [默认: 100]: ").strip()
    batch_size = batch_size if batch_size else "100"

    # 5. 任务清单确认
    tables_to_sync = ",".join(target_tables)
    print("\n" + "="*60)
    print(" 任务清单确认:")
    print(f"  - 目标数量: {len(target_tables)} 个数据表")
    print(f"  - 数据列表: {tables_to_sync}")
    print(f"  - 同步模式: {'全量覆盖' if start_date or force_refresh else '增量同步'}")
    print(f"  - 时间区间: {start_date if start_date else '自动水位线'} -> {end_date if end_date else '至今'}")
    print(f"  - 存储格式: {formats}")
    print(f"  - 任务负荷: {batch_size}")
    print("="*60)
    
    confirm = input("\n[5/5] 确认启动同步任务? (Y/n): ").strip().lower()
    if confirm not in ['', 'y', 'yes']:
        print("\n[!] 任务已取消。")
        return

    # 构造命令
    cmd = [
        sys.executable, "-m", "app.gateway.cli", "sync",
        "--tables", tables_to_sync,
        "--formats", formats,
        "--batch", batch_size
    ]
    
    if start_date:
        cmd.extend(["--start", start_date])
    if end_date:
        cmd.extend(["--end", end_date])
    if force_refresh:
        cmd.append("--force")

    print(f"\n[*] 正在启动同步流程...")
    print("-" * 60)
    
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(get_project_root())
        env["PYTHONUTF8"] = "1"  # 强制子进程开启 UTF-8 模式
        
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",    # 明确指定编码为 UTF-8
            bufsize=1,
            universal_newlines=True
        )
        
        for line in process.stdout:
            print(line, end='')
            
        process.wait()
        
        if process.returncode == 0:
            print("\n[+] 同步任务执行完毕！")
        else:
            print(f"\n[!] 同步任务执行失败，退出码: {process.returncode}")
            
    except Exception as e:
        print(f"\n[!] 发生异常: {e}")

    input("\n任务已结束。按回车键退出...")

if __name__ == "__main__":
    start_wizard()
