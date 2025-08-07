import random
from faker import Faker
from datetime import datetime
import pymysql
import time

# 初始化
fake = Faker('zh_CN')
current_date = datetime.now()
target_date = datetime(2025, 8, 7)  # 固定为2025-08-07

# 数据库配置
db_config = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'gmall_02',
    'charset': 'utf8mb4'
}

def get_connection():
    return pymysql.connect(**db_config)

def batch_insert(table_name, data, batch_size=500):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        print(f"已清空表 {table_name}")

        if not data:
            print(f"警告：{table_name} 无数据可插入")
            return True

        columns = list(data[0].keys())
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"

        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(sql, [tuple(item.values()) for item in batch])
            conn.commit()
            print(f"已插入 {min(i+batch_size, total)}/{total} 条到 {table_name}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"插入失败({table_name}): {str(e)[:200]}")
        return False
    finally:
        cursor.close()
        conn.close()

# 1. 商品基础信息（500条）
def generate_product_info():
    print("生成商品信息（500条）...")
    categories = [('1001','手机数码'), ('1002','家用电器'), ('1003','服装鞋包'),
                  ('1004','美妆个护'), ('1005','食品生鲜'), ('1006','家居家装')]
    brands = [('2001','苹果'), ('2002','华为'), ('2003','小米'), ('2004','海尔'),
              ('2005','美的'), ('2006','耐克'), ('2007','欧莱雅'), ('2008','伊利')]

    return [{
        'product_id': f"P{str(i).zfill(5)}",
        'product_name': f"{random.choice(brands)[1]}{fake.word()}{random.choice(['手机','电视','鞋子','面膜'])}",
        'category_id': random.choice(categories)[0],
        'category_name': random.choice(categories)[1],
        'brand_id': random.choice(brands)[0],
        'brand_name': random.choice(brands)[1],
        'price': round(random.uniform(50, 5000), 2),
        'cost': round(random.uniform(20, 4000), 2),
        'status': random.choices([0,1], weights=[0.1,0.9])[0],
        'create_time': target_date,
        'update_time': target_date,
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for i in range(1, 501)]

# 2. 商品SKU（每个商品3个SKU，共1500条）
def generate_product_sku(product_info):
    print("生成SKU信息（1,500条）...")
    data = []
    for i, product in enumerate(product_info, 1):
        for j in range(1, 4):
            sku_attr = f"颜色:{fake.color_name()}, " + random.choice([
                f"内存:{random.choice(['64G','128G','256G'])}",
                f"尺寸:{random.choice(['S','M','L','XL'])}",
                f"规格:{random.choice(['标准装','家庭装'])}"
            ])
            data.append({
                'sku_id': f"S{str(i).zfill(5)}{j}",
                'product_id': product['product_id'],
                'sku_name': f"{product['product_name']}-变体{j}",
                'sku_attr': sku_attr,
                'price': round(product['price'] * random.uniform(0.8, 1.2), 2),
                'stock': random.randint(50, 1000),
                'status': product['status'],
                'create_time': target_date,
                'update_time': target_date,
                'etl_time': current_date,
                'dt': target_date.strftime('%Y-%m-%d')
            })
    return data

# 3. 商品销售数据（5,000条）
def generate_product_sales(product_info, product_sku):
    print("生成销售数据（5,000条）...")
    data = []
    order_base = int(target_date.timestamp())
    product_sku_map = {p['product_id']: [] for p in product_info}
    for sku in product_sku:
        product_sku_map[sku['product_id']].append(sku)

    for order_num in range(5000):
        order_id = f"O{order_base + order_num}{random.randint(10000,99999)}"
        user_id = f"U{random.randint(1000, 9999)}"
        channel = random.choice(['APP','PC','WX'])
        used_products = set()

        for _ in range(random.randint(1, 3)):
            product = random.choice(product_info)
            while product['product_id'] in used_products:
                product = random.choice(product_info)
            used_products.add(product['product_id'])

            sku = random.choice(product_sku_map.get(product['product_id'], []))
            if not sku: continue

            quantity = random.randint(1, 3)
            data.append({
                'order_id': order_id,
                'product_id': product['product_id'],
                'sku_id': sku['sku_id'],
                'user_id': user_id,
                'sale_amount': round(sku['price'] * quantity, 2),
                'sale_quantity': quantity,
                'payment_time': target_date.replace(
                    hour=random.randint(9, 21),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)),
                'channel_id': channel,
                'channel_name': {'APP':'APP端','PC':'电脑端','WX':'微信'}[channel],
                'etl_time': current_date,
                'dt': target_date.strftime('%Y-%m-%d')
            })
    return data[:5000]

# 4. 商品流量数据（15,000条）
def generate_product_traffic(product_info):
    print("生成流量数据（15,000条）...")
    sources = ['搜索', '推荐', '活动', '直接访问']
    source_details = {
        '搜索': ['品牌词', '品类词', '长尾词'],
        '推荐': ['首页推荐', '猜你喜欢', '同类推荐'],
        '活动': ['双11', '618', '年货节'],
        '直接访问': ['直接访问']
    }

    return [{
        'product_id': product['product_id'],
        'uv': random.randint(50, 500),
        'pv': random.randint(100, 2000),
        'cart_uv': random.randint(5, 100),
        'fav_uv': random.randint(3, 50),
        'pay_uv': random.randint(1, 30),
        'source_type': source,
        'source_detail': random.choice(source_details[source]),
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for product in product_info for source in sources]

# 5. 搜索词数据（1,500条）
def generate_search_keyword(product_info):
    print("生成搜索词数据（1,500条）...")
    keywords = ['手机', '电视', '运动鞋', '面膜', '牛奶', '沙发', '笔记本', '洗衣机']
    return [{
        'keyword': keyword,
        'product_id': product['product_id'],
        'search_uv': random.randint(50, 500),
        'click_uv': random.randint(10, 200),
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for product in product_info for keyword in random.sample(keywords, 3)][:1500]

# 6. 价格力数据（500条）
def generate_price_power(product_info):
    print("生成价格力数据（500条）...")
    return [{
        'product_id': p['product_id'],
        'price_power_level': random.choices([1,2,3], weights=[0.3,0.5,0.2])[0],
        'price_power_star': round(random.uniform(3.0, 5.0), 1),
        'after_coupon_price': round(p['price'] * random.uniform(0.7, 0.95), 2),
        'market_avg_price': round(p['price'] * random.uniform(0.8, 1.2), 2),
        'price_alert_flag': random.choices([0,1], weights=[0.7,0.3])[0],
        'product_power_alert_flag': random.choices([0,1], weights=[0.8,0.2])[0],
        'create_time': target_date,
        'update_time': target_date,
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for p in product_info]

# 7. 商品预警数据（300条）
def generate_product_alert(product_info):
    print("生成预警数据（300条）...")
    reasons = {
        1: ['价格高于竞品', '促销力度不足', '定价过高'],
        2: ['转化率下降', '差评增多', '退货率上升']
    }
    return [{
        'alert_id': f"A{str(i).zfill(5)}",
        'product_id': random.choice(product_info)['product_id'],
        'alert_type': random.choices([1,2], weights=[0.6,0.4])[0],
        'alert_reason': random.choice(reasons[random.choices([1,2], weights=[0.6,0.4])[0]]),
        'alert_level': random.choices([1,2,3], weights=[0.2,0.5,0.3])[0],
        'status': random.choices([0,1], weights=[0.3,0.7])[0],
        'create_time': target_date.replace(
            hour=random.randint(9, 21),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)),
        'update_time': target_date.replace(
            hour=random.randint(9, 21),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)),
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for i in range(1, 301)]

# 8. 商品关注数据（2,000条）
def generate_product_follow(product_info):
    print("生成关注数据（2,000条）...")
    return [{
        'follow_id': f"F{str(i).zfill(6)}",
        'product_id': random.choice(product_info)['product_id'],
        'user_id': f"U{random.randint(1000, 9999)}",
        'follow_time': target_date.replace(
            hour=random.randint(9, 21),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)),
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for i in range(1, 2001)]

# 9. 商品转化率（500条）
def generate_conversion(product_info):
    print("生成转化率数据（500条）...")
    return [{
        'product_id': p['product_id'],
        'conversion_rate': round(random.uniform(0.01, 0.15), 4),
        'market_avg_conversion': round(random.uniform(0.02, 0.1), 4),
        'is_declining': random.choices([0,1], weights=[0.7,0.3])[0],
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for p in product_info]

# 10. 商品库存（1,500条）
def generate_product_stock(product_sku):
    print("生成库存数据（1,500条）...")
    return [{
        'product_id': sku['product_id'],
        'sku_id': sku['sku_id'],
        'stock': sku['stock'],
        'daily_sale_avg': random.randint(1, 20),
        'sale_days': sku['stock'] // max(1, random.randint(1, 20)),
        'etl_time': current_date,
        'dt': target_date.strftime('%Y-%m-%d')
    } for sku in product_sku]

if __name__ == '__main__':
    start_time = time.time()

    print("="*50)
    print(f"开始生成测试数据(仅{target_date.strftime('%Y-%m-%d')})...")

    products = generate_product_info()
    skus = generate_product_sku(products)

    tables_data = [
        ('product_info', products),
        ('product_sku', skus),
        ('product_sales', generate_product_sales(products, skus)),
        ('product_traffic', generate_product_traffic(products)),
        ('search_keyword', generate_search_keyword(products)),
        ('price_power_product', generate_price_power(products)),
        ('product_alert', generate_product_alert(products)),
        ('product_follow', generate_product_follow(products)),
        ('product_conversion', generate_conversion(products)),
        ('product_stock', generate_product_stock(skus))
    ]

    for table_name, data in tables_data:
        print("\n" + "="*50)
        if not batch_insert(table_name, data):
            print(f"!!! {table_name} 数据插入失败，终止执行 !!!")
            exit(1)

    print("\n" + "="*50)
    print(f"所有数据生成完成，总耗时: {time.time()-start_time:.2f}秒")
    print("="*50)
    print("数据量统计:")
    print("-"*50)
    for table_name, data in tables_data:
        print(f"{table_name:<20} | {len(data):>6} 条")
    print("="*50)
    print(f"所有数据日期均为: {target_date.strftime('%Y-%m-%d')}")
    print("="*50)