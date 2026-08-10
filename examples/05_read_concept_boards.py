"""
示例 05: 获取概念/行业板块列表与极速按需成分股查询
"""

import cqdata

def main():
    table_id = "ashare.concept.eastmoney"
    
    print(f"=== 1. 查询 {table_id} 板块列表 ===")
    boards = cqdata.list_boards(table_id)
    print(f"共发现 {len(boards)} 个板块:")
    for b in boards[:5]:
        print(f"  - [{b['board_code']}] {b['board_name']}: {b['stock_count']} 只成分股")
        
    if boards:
        target_code = boards[0]['board_code']
        print(f"\n=== 2. 按需读取板块 [{target_code}] 的成分股明细 ===")
        df = cqdata.read(table_id)
        filtered_df = df.filter(df["board_code"] == target_code)
        print(f"成分股全量记录: {filtered_df.height} 条")
        print(filtered_df.head(5))

if __name__ == "__main__":
    main()
