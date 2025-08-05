# 首先安装: pip install PyMySQL
import pymysql

try:
    conn = pymysql.connect(
        user='root',
        password='root',
        host='cdh03',
        database='gmall_01'
    )
    print("连接成功")
    conn.close()
except Exception as e:
    print(f"连接失败: {e}")
