#!/usr/bin/env python3
"""
完整資料庫架構與內容展示腳本
展示所有表的 DDL、索引、約束、資料內容等
"""

import os
import sys
import duckdb
from datetime import datetime

# 添加專案根目錄到 Python 路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import DUCKDB_PATH

def show_table_ddl(conn, table_name):
    """顯示表的 DDL"""
    try:
        # 獲取表的創建語句
        result = conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone()
        if result:
            return result[0]
        else:
            # DuckDB 方式獲取表結構
            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
            ddl = f"CREATE TABLE {table_name} (\n"
            for i, (col_name, col_type, nullable, key, default, extra) in enumerate(columns):
                nullable_str = "NOT NULL" if nullable == "NO" else ""
                key_str = "PRIMARY KEY" if key == "PRI" else ""
                
                line = f"  {col_name} {col_type}"
                if nullable_str:
                    line += f" {nullable_str}"
                if key_str:
                    line += f" {key_str}"
                if default and default != "NULL":
                    line += f" DEFAULT {default}"
                    
                if i < len(columns) - 1:
                    line += ","
                ddl += line + "\n"
            ddl += ");"
            return ddl
    except Exception as e:
        return f"無法獲取 {table_name} 的 DDL: {e}"

def show_table_indexes(conn, table_name):
    """顯示表的索引"""
    try:
        # 嘗試不同的方式獲取索引信息
        try:
            result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            indexes = []
            for row in result:
                if len(row) >= 6 and row[5]:  # 如果有主鍵
                    indexes.append(f"PRIMARY KEY on {row[1]}")
            return indexes if indexes else ["無索引"]
        except:
            return ["無法查詢索引"]
    except Exception as e:
        return [f"索引查詢錯誤: {e}"]

def show_table_constraints(conn, table_name):
    """顯示表的約束"""
    try:
        # 從表結構中推斷約束
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        constraints = []
        
        for col_name, col_type, nullable, key, default, extra in columns:
            if key == "PRI":
                constraints.append(f"PRIMARY KEY: {col_name}")
            if nullable == "NO":
                constraints.append(f"NOT NULL: {col_name}")
                
        return constraints if constraints else ["無特殊約束"]
    except Exception as e:
        return [f"約束查詢錯誤: {e}"]

def show_table_sample_data(conn, table_name, limit=5):
    """顯示表的樣本資料"""
    try:
        result = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchall()
        columns = [desc[0] for desc in conn.description]
        return columns, result
    except Exception as e:
        return None, f"無法獲取 {table_name} 的資料: {e}"

def show_table_statistics(conn, table_name):
    """顯示表的統計信息"""
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        # 獲取表的列信息
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        
        stats = {
            'row_count': count,
            'column_count': len(columns),
            'size_estimate': 'N/A'  # DuckDB 沒有直接的表大小查詢
        }
        
        # 如果表有時間戳列，獲取時間範圍
        timestamp_cols = ['timestamp', 'created_at', 'updated_at']
        for col in timestamp_cols:
            try:
                time_range = conn.execute(f"""
                    SELECT MIN({col}) as min_time, MAX({col}) as max_time 
                    FROM {table_name} 
                    WHERE {col} IS NOT NULL
                """).fetchone()
                if time_range[0]:
                    stats[f'{col}_range'] = f"{time_range[0]} ~ {time_range[1]}"
                break
            except:
                continue
                
        return stats
    except Exception as e:
        return f"無法獲取 {table_name} 的統計: {e}"

def main():
    """主函數"""
    print("=" * 80)
    print("🗃️  完整資料庫架構與內容展示")
    print("=" * 80)
    
    try:
        # 連接資料庫
        conn = duckdb.connect(str(DUCKDB_PATH))
        print(f"📁 資料庫路徑: {DUCKDB_PATH}")
        print(f"⏰ 檢查時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 獲取所有表
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        
        table_names = [t[0] for t in tables]
        
        # 獲取所有視圖
        views = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_type = 'VIEW'
            ORDER BY table_name
        """).fetchall()
        
        view_names = [v[0] for v in views]
        
        print(f"📊 發現 {len(table_names)} 個資料表，{len(view_names)} 個視圖")
        print()
        
        # 顯示每個表的完整信息
        for table_name in table_names:
            print("=" * 60)
            print(f"📋 資料表: {table_name}")
            print("=" * 60)
            
            # 1. DDL
            print("\n🏗️  表結構 (DDL):")
            print("-" * 40)
            ddl = show_table_ddl(conn, table_name)
            print(ddl)
            
            # 2. 索引
            print("\n🔍 索引:")
            print("-" * 40)
            indexes = show_table_indexes(conn, table_name)
            if indexes:
                for idx in indexes:
                    print(f"  • {idx}")
            else:
                print("  無索引")
            
            # 3. 約束
            print("\n🔒 約束:")
            print("-" * 40)
            constraints = show_table_constraints(conn, table_name)
            if constraints:
                for const in constraints:
                    print(f"  • {const}")
            else:
                print("  無特殊約束")
            
            # 4. 統計信息
            print("\n📈 統計信息:")
            print("-" * 40)
            stats = show_table_statistics(conn, table_name)
            if isinstance(stats, dict):
                for key, value in stats.items():
                    print(f"  • {key}: {value}")
            else:
                print(f"  {stats}")
            
            # 5. 樣本資料
            print("\n📄 樣本資料 (前5筆):")
            print("-" * 40)
            columns, sample_data = show_table_sample_data(conn, table_name)
            if columns:
                # 顯示列標題
                col_str = " | ".join([f"{col[:15]:15}" for col in columns[:6]])  # 只顯示前6列
                print(f"  {col_str}")
                print("  " + "-" * len(col_str))
                
                # 顯示資料
                for row in sample_data:
                    row_str = " | ".join([f"{str(val)[:15]:15}" for val in row[:6]])  # 只顯示前6列
                    print(f"  {row_str}")
            else:
                print(f"  {sample_data}")
            
            print("\n")
        
        # 顯示視圖信息
        if view_names:
            print("=" * 60)
            print("👁️  資料視圖")
            print("=" * 60)
            
            for view_name in view_names:
                print(f"\n📋 視圖: {view_name}")
                print("-" * 40)
                
                # 視圖統計
                stats = show_table_statistics(conn, view_name)
                if isinstance(stats, dict):
                    for key, value in stats.items():
                        print(f"  • {key}: {value}")
                
                # 視圖樣本資料
                columns, sample_data = show_table_sample_data(conn, view_name, 3)
                if columns:
                    print(f"\n  樣本資料:")
                    col_str = " | ".join([f"{col[:12]:12}" for col in columns[:5]])
                    print(f"    {col_str}")
                    for row in sample_data:
                        row_str = " | ".join([f"{str(val)[:12]:12}" for val in row[:5]])
                        print(f"    {row_str}")
        
        # 資料庫整體統計
        print("\n" + "=" * 60)
        print("🏢 資料庫整體統計")
        print("=" * 60)
        
        total_rows = 0
        for table_name in table_names:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                total_rows += count
                print(f"  • {table_name}: {count:,} 筆")
            except Exception as e:
                print(f"  • {table_name}: 錯誤 - {e}")
        
        print(f"\n📊 總計: {total_rows:,} 筆資料")
        
        # 資料庫文件大小
        try:
            import os
            db_size = os.path.getsize(str(DUCKDB_PATH)) / (1024 * 1024)
            print(f"💾 資料庫文件大小: {db_size:.2f} MB")
        except:
            print("💾 無法獲取資料庫文件大小")
            
    except Exception as e:
        print(f"❌ 錯誤: {e}")
    finally:
        conn.close()
        print("\n🔒 資料庫連接已關閉")

if __name__ == "__main__":
    main()