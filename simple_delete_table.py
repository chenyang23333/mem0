#!/usr/bin/env python3
"""
简单删除mem0_memories表的脚本
"""

import psycopg2
import logging

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


def delete_mem0_table():
    """删除mem0_memories表"""
    
    try:
        # 连接数据库
        print("连接数据库...")
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"]
        )
        
        cursor = connection.cursor()
        
        # 检查表是否存在
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'mem0_memories'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("表 'mem0_memories' 存在，正在删除...")
            
            # 获取表信息
            cursor.execute("SELECT COUNT(*) FROM mem0_memories;")
            row_count = cursor.fetchone()[0]
            print(f"表中有 {row_count} 行数据")
            
            # 删除表
            cursor.execute("DROP TABLE IF EXISTS mem0_memories;")
            connection.commit()
            
            print("✅ 成功删除表 'mem0_memories'")
        else:
            print("表 'mem0_memories' 不存在，无需删除")
        
        cursor.close()
        connection.close()
        
    except psycopg2.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    except Exception as e:
        print(f"❌ 程序执行失败: {e}")


def main():
    """主函数"""
    print("=" * 50)
    print("🗑️  删除mem0_memories表")
    print("=" * 50)
    
    delete_mem0_table()
    
    print("\n操作完成！")


if __name__ == "__main__":
    main()
