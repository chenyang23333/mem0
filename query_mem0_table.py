#!/usr/bin/env python3
"""
查询mem0_memories表的全部内容
"""

import psycopg2
import logging
import json
from typing import List, Dict, Any

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库连接配置
DB_CONFIG = {
    "host": "10.118.1.32",
    "port": 5432,
    "user": "flightbi",
    "password": "CtripBt!@3",
    "database": "fltaivectordb"
}


def connect_to_database():
    """连接到PostgreSQL数据库"""
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"]
        )
        logger.info(f"成功连接到数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        return connection
        
    except psycopg2.Error as e:
        logger.error(f"数据库连接失败: {e}")
        return None


def check_table_exists(connection, table_name: str = "mem0_memories") -> bool:
    """检查表是否存在"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))
        
        exists = cursor.fetchone()[0]
        cursor.close()
        
        if exists:
            logger.info(f"表 '{table_name}' 存在")
        else:
            logger.info(f"表 '{table_name}' 不存在")
            
        return exists
        
    except psycopg2.Error as e:
        logger.error(f"检查表存在性失败: {e}")
        return False


def get_table_structure(connection, table_name: str = "mem0_memories") -> List[Dict]:
    """获取表结构信息"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = cursor.fetchall()
        cursor.close()
        
        column_info = []
        for col in columns:
            column_info.append({
                "name": col[0],
                "type": col[1],
                "nullable": col[2],
                "default": col[3],
                "max_length": col[4]
            })
        
        return column_info
        
    except psycopg2.Error as e:
        logger.error(f"获取表结构失败: {e}")
        return []


def get_table_count(connection, table_name: str = "mem0_memories") -> int:
    """获取表中的记录数"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    except psycopg2.Error as e:
        logger.error(f"获取记录数失败: {e}")
        return 0


def query_all_data(connection, table_name: str = "mem0_memories", limit: int = None) -> List[Dict]:
    """查询表中的所有数据"""
    try:
        cursor = connection.cursor()
        
        # 构建查询语句
        if limit:
            query = f"SELECT * FROM {table_name} LIMIT %s;"
            cursor.execute(query, (limit,))
        else:
            query = f"SELECT * FROM {table_name};"
            cursor.execute(query)
        
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 获取数据
        rows = cursor.fetchall()
        cursor.close()
        
        # 转换为字典列表
        data = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                column_name = column_names[i]
                
                # 处理JSON字段
                if column_name == 'payload' and value:
                    try:
                        if isinstance(value, str):
                            row_dict[column_name] = json.loads(value)
                        else:
                            row_dict[column_name] = value
                    except json.JSONDecodeError:
                        row_dict[column_name] = value
                else:
                    row_dict[column_name] = value
            
            data.append(row_dict)
        
        return data
        
    except psycopg2.Error as e:
        logger.error(f"查询数据失败: {e}")
        return []


def display_table_info(connection, table_name: str = "mem0_memories"):
    """显示表的基本信息"""
    print(f"\n📊 表 '{table_name}' 信息:")
    print("=" * 50)
    
    # 检查表是否存在
    if not check_table_exists(connection, table_name):
        print(f"❌ 表 '{table_name}' 不存在")
        return
    
    # 获取表结构
    columns = get_table_structure(connection, table_name)
    if columns:
        print(f"📋 表结构 ({len(columns)} 列):")
        for i, col in enumerate(columns, 1):
            print(f"  {i}. {col['name']} ({col['type']}) - {'可空' if col['nullable'] == 'YES' else '非空'}")
    
    # 获取记录数
    count = get_table_count(connection, table_name)
    print(f"📈 记录数: {count}")


def display_data(data: List[Dict], max_display: int = 10):
    """显示数据"""
    if not data:
        print("❌ 没有数据")
        return
    
    print(f"\n📄 数据内容 (显示前 {min(len(data), max_display)} 条):")
    print("=" * 80)
    
    for i, row in enumerate(data[:max_display], 1):
        print(f"\n🔸 记录 {i}:")
        for key, value in row.items():
            if key == 'payload' and isinstance(value, dict):
                print(f"  {key}: {json.dumps(value, ensure_ascii=False, indent=2)}")
            elif key == 'vector' and value:
                # 向量数据只显示前几个维度
                if isinstance(value, list):
                    vector_preview = value[:5] if len(value) > 5 else value
                    print(f"  {key}: {vector_preview}... (维度: {len(value)})")
                else:
                    print(f"  {key}: {str(value)[:100]}...")
            else:
                # 限制字符串长度
                if isinstance(value, str) and len(value) > 100:
                    print(f"  {key}: {value[:100]}...")
                else:
                    print(f"  {key}: {value}")


def save_data_to_file(data: List[Dict], filename: str = "mem0_memories_data.json"):
    """保存数据到文件"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        print(f"✅ 数据已保存到文件: {filename}")
    except Exception as e:
        print(f"❌ 保存文件失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("🔍 查询mem0_memories表内容")
    print("=" * 60)
    
    # 连接数据库
    connection = connect_to_database()
    if not connection:
        print("❌ 数据库连接失败，程序退出")
        return
    
    try:
        table_name = "mem0_memories"
        
        # 显示表信息
        display_table_info(connection, table_name)
        
        # 查询所有数据
        print(f"\n🔍 查询表 '{table_name}' 的所有数据...")
        data = query_all_data(connection, table_name)
        
        if data:
            print(f"✅ 成功查询到 {len(data)} 条记录")
            
            # 显示数据
            display_data(data, max_display=5)
            
            # 询问是否保存到文件
            if len(data) > 0:
                save_choice = input(f"\n是否保存所有 {len(data)} 条记录到文件? (y/n): ").strip().lower()
                if save_choice in ['y', 'yes', '是']:
                    save_data_to_file(data)
            
            # 询问是否显示更多数据
            if len(data) > 5:
                more_choice = input(f"\n是否显示更多数据? (y/n): ").strip().lower()
                if more_choice in ['y', 'yes', '是']:
                    display_data(data, max_display=len(data))
        else:
            print("❌ 没有查询到数据")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        
    finally:
        if connection:
            connection.close()
            logger.info("数据库连接已关闭")


def interactive_query():
    """交互式查询"""
    print("=" * 60)
    print("🔍 交互式查询mem0_memories表")
    print("=" * 60)
    
    connection = connect_to_database()
    if not connection:
        print("❌ 数据库连接失败，程序退出")
        return
    
    try:
        while True:
            print("\n" + "=" * 40)
            print("请选择操作:")
            print("1. 查看表信息")
            print("2. 查询所有数据")
            print("3. 查询指定数量数据")
            print("4. 保存数据到文件")
            print("5. 退出")
            print("=" * 40)
            
            choice = input("请输入选择 (1-5): ").strip()
            
            if choice == "1":
                display_table_info(connection)
                
            elif choice == "2":
                data = query_all_data(connection)
                if data:
                    print(f"✅ 查询到 {len(data)} 条记录")
                    display_data(data, max_display=10)
                else:
                    print("❌ 没有数据")
                    
            elif choice == "3":
                try:
                    limit = int(input("请输入要查询的记录数: "))
                    data = query_all_data(connection, limit=limit)
                    if data:
                        print(f"✅ 查询到 {len(data)} 条记录")
                        display_data(data, max_display=limit)
                    else:
                        print("❌ 没有数据")
                except ValueError:
                    print("❌ 请输入有效的数字")
                    
            elif choice == "4":
                data = query_all_data(connection)
                if data:
                    filename = input("请输入文件名 (默认: mem0_memories_data.json): ").strip()
                    if not filename:
                        filename = "mem0_memories_data.json"
                    save_data_to_file(data, filename)
                else:
                    print("❌ 没有数据可保存")
                    
            elif choice == "5":
                print("👋 退出程序")
                break
                
            else:
                print("❌ 无效选择，请重新输入")
                
    except KeyboardInterrupt:
        print("\n\n👋 程序被中断")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("数据库连接已关闭")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_query()
    else:
        main()
