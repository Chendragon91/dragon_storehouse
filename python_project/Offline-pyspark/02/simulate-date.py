import random
from faker import Faker
from datetime import datetime, timedelta
import pymysql
from pymysql.cursors import DictCursor
import time

# 初始化Faker和数据库连接
fake = Faker('zh_CN')

# 数据库配置
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'yourpassword',
    'database': 'gmall_02',
    'charset': 'utf8mb4'
}

# 创建数据库连接
def get_connection():
    return pymysql.connect(**db_config)

# 批量插入数据
def batch_insert(table_name, data, batch_size=1000):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # 获取列名
        columns = data[0].keys()
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        # 准备SQL
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # 分批插入
        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i+batch_size]
            values = [tuple(item.values()) for item in batch]
            cursor.executemany(sql, values)
            conn.commit()
            print(f"已插入 {min(i+batch_size, total)}/{total} 条数据到 {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"插入数据到 {table_name} 时出错: {e}")
    finally:
        cursor.close()
        conn.close()

# 1. 生成基础商品信息
def generate_product_info():
    print("开始生成商品基础信息...")
    categories = [
        ('1001', '手机数码'), ('1002', '家用电器'), ('1003', '服装鞋包'),
        ('1004', '美妆个护'), ('1005', '食品生鲜'), ('1006', '家居家装')
    ]

    brands = [
        ('2001', '苹果'), ('2002', '华为'), ('2003', '小米'),
        ('2004', '海尔'), ('2005', '美的'), ('2006', '耐克'),
        ('2007', '欧莱雅'), ('2008', '伊利'), ('2009', '宜家')
    ]

    data = []
    for i in range(1, 101):
        product_id = f"P{str(i).zfill(5)}"
        category_id, category_name = random.choice(categories)
        brand_id, brand_name = random.choice(brands)
        price = round(random.uniform(50, 5000), 2)
        cost = round(price * random.uniform(0.4, 0.8), 2)

        data.append({
            'product_id': product_id,
            'product_name': f"{brand_name}{fake.word()}{random.choice(['手机', '电视', '鞋子', '面膜', '牛奶', '沙发'])}",
            'category_id': category_id,
            'category_name': category_name,
            'brand_id': brand_id,
            'brand_name': brand_name,
            'price': price,
            'cost': cost,
            'status': random.choices([0, 1], weights=[0.1, 0.9])[0],
            'create_time': fake.date_time_between(start_date='-1y', end_date='now'),
            'update_time': fake.date_time_between(start_date='-1y', end_date='now'),
            'etl_time': datetime.now(),
            'dt': datetime.now().strftime('%Y-%m-%d')
        })

    batch_insert('ods_product_info', data)
    return data

# 2. 为每个商品生成SKU信息
def generate_product_sku(product_info):
    print("开始生成商品SKU信息...")
    data = []
    sku_counter = 1

    for product in product_info:
        sku_count = random.randint(1, 5)
        for j in range(sku_count):
            sku_id = f"S{str(sku_counter).zfill(6)}"
            sku_counter += 1

            if '手机' in product['product_name']:
                sku_attr = f"颜色:{fake.color_name()}, 内存:{random.choice(['64G','128G','256G','512G'])}"
            elif '电视' in product['product_name']:
                sku_attr = f"尺寸:{random.choice(['55寸','65寸','75寸','85寸'])}, 颜色:{fake.color_name()}"
            elif '鞋子' in product['product_name']:
                sku_attr = f"尺码:{random.choice(['37','38','39','40','41','42','43','44'])}, 颜色:{fake.color_name()}"
            else:
                sku_attr = f"规格:{random.choice(['标准装','家庭装','礼盒装'])}, 颜色:{fake.color_name()}"

            data.append({
                'sku_id': sku_id,
                'product_id': product['product_id'],
                'sku_name': f"{product['product_name']}-{j+1}",
                'sku_attr': sku_attr,
                'price': round(product['price'] * random.uniform(0.9, 1.1), 2),
                'stock': random.randint(0, 1000),
                'status': 1 if product['status'] == 1 else 0,
                'create_time': product['create_time'],
                'update_time': product['update_time'],
                'etl_time': datetime.now(),
                'dt': datetime.now().strftime('%Y-%m-%d')
            })

    batch_insert('ods_product_sku', data)
    return data

# 3. 生成销售数据
def generate_product_sales(product_info, product_sku):
    print("开始生成商品销售数据...")
    data = []
    start_date = datetime.now() - timedelta(days=30)

    for day in range(30):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime('%Y-%m-%d')

        for order_num in range(random.randint(50, 200)):
            order_id = f"O{current_date.strftime('%Y%m%d')}{str(order_num).zfill(5)}"
            user_id = f"U{random.randint(1000, 9999)}"
            channel_id = random.choice(['APP', 'PC', 'WX', 'H5'])
            channel_name = {'APP': '移动应用', 'PC': '电脑网页', 'WX': '微信小程序', 'H5': '手机网页'}[channel_id]

            for item_num in range(random.randint(1, 5)):
                product = random.choice(product_info)
                sku = random.choice([s for s in product_sku if s['product_id'] == product['product_id']])

                quantity = random.randint(1, 3)
                sale_amount = round(sku['price'] * quantity, 2)

                payment_time = current_date.replace(
                    hour=random.randint(9, 22),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                data.append({
                    'order_id': order_id,
                    'product_id': product['product_id'],
                    'sku_id': sku['sku_id'],
                    'user_id': user_id,
                    'sale_amount': sale_amount,
                    'sale_quantity': quantity,
                    'payment_time': payment_time,
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'etl_time': datetime.now(),
                    'dt': date_str
                })

    batch_insert('ods_product_sales', data, batch_size=500)

# 4. 生成商品流量数据
def generate_product_traffic(product_info):
    print("开始生成商品流量数据...")
    data = []
    start_date = datetime.now() - timedelta(days=30)

    for day in range(30):
        date_str = (start_date + timedelta(days=day)).strftime('%Y-%m-%d')
        for product in product_info:
            source_type = random.choice(['搜索', '推荐', '活动', '直接访问'])
            source_detail = {
                '搜索': random.choice(['品牌词', '品类词', '长尾词']),
                '推荐': random.choice(['首页推荐', '猜你喜欢', '同类推荐']),
                '活动': random.choice(['双11', '618', '年货节', '日常促销']),
                '直接访问': '直接输入网址'
            }[source_type]

            uv = random.randint(10, 1000)
            pv = uv * random.randint(1, 5)
            cart_uv = int(uv * random.uniform(0.05, 0.2))
            fav_uv = int(uv * random.uniform(0.03, 0.15))
            pay_uv = int(uv * random.uniform(0.01, 0.1))

            data.append({
                'product_id': product['product_id'],
                'uv': uv,
                'pv': pv,
                'cart_uv': cart_uv,
                'fav_uv': fav_uv,
                'pay_uv': pay_uv,
                'source_type': source_type,
                'source_detail': source_detail,
                'etl_time': datetime.now(),
                'dt': date_str
            })

    batch_insert('ods_product_traffic', data)

# 5. 生成搜索词数据
def generate_search_keyword(product_info):
    print("开始生成搜索词数据...")
    data = []
    popular_keywords = ['手机', '电视', '运动鞋', '面膜', '牛奶', '沙发', '笔记本电脑', '洗衣机']

    for product in product_info:
        for keyword in random.sample(popular_keywords, random.randint(1, 3)):
            search_uv = random.randint(50, 500)
            click_uv = int(search_uv * random.uniform(0.1, 0.3))

            data.append({
                'keyword': keyword,
                'product_id': product['product_id'],
                'search_uv': search_uv,
                'click_uv': click_uv,
                'etl_time': datetime.now(),
                'dt': datetime.now().strftime('%Y-%m-%d')
            })

    batch_insert('ods_search_keyword', data)

# 6. 生成价格力商品数据
def generate_price_power_product(product_info):
    print("开始生成价格力商品数据...")
    data = []

    for product in product_info:
        price_power_level = random.choices([1, 2, 3], weights=[0.2, 0.5, 0.3])[0]
        price_power_star = round(random.uniform(3.0, 5.0), 1)
        after_coupon_price = round(product['price'] * random.uniform(0.8, 0.95), 2)
        market_avg_price = round(product['price'] * random.uniform(0.9, 1.1), 2)

        data.append({
            'product_id': product['product_id'],
            'price_power_level': price_power_level,
            'price_power_star': price_power_star,
            'after_coupon_price': after_coupon_price,
            'market_avg_price': market_avg_price,
            'price_alert_flag': 1 if price_power_level == 3 else 0,
            'product_power_alert_flag': random.choices([0, 1], weights=[0.7, 0.3])[0],
            'create_time': product['create_time'],
            'update_time': product['update_time'],
            'etl_time': datetime.now(),
            'dt': datetime.now().strftime('%Y-%m-%d')
        })

    batch_insert('ods_price_power_product', data)

# 7. 生成商品预警数据
def generate_product_alert():
    print("开始生成商品预警数据...")
    data = []
    alert_types = {1: '价格力预警', 2: '商品力预警'}
    alert_reasons = {
        1: ['价格高于市场均价', '价格竞争力不足', '促销价格设置不合理'],
        2: ['转化率下降', '流量下降', '差评增多', '库存周转率低']
    }

    for i in range(1, 31):
        alert_id = f"A{str(i).zfill(5)}"
        product_id = f"P{str(random.randint(1, 100)).zfill(5)}"
        alert_type = random.choices([1, 2], weights=[0.6, 0.4])[0]

        data.append({
            'alert_id': alert_id,
            'product_id': product_id,
            'alert_type': alert_type,
            'alert_reason': random.choice(alert_reasons[alert_type]),
            'alert_level': random.choices([1, 2, 3], weights=[0.2, 0.5, 0.3])[0],
            'status': random.choices([0, 1], weights=[0.3, 0.7])[0],
            'create_time': fake.date_time_between(start_date='-30d', end_date='now'),
            'update_time': fake.date_time_between(start_date='-30d', end_date='now'),
            'etl_time': datetime.now(),
            'dt': datetime.now().strftime('%Y-%m-%d')
        })

    batch_insert('ods_product_alert', data)

# 8. 生成商品关注数据
def generate_product_follow():
    print("开始生成商品关注数据...")
    data = []

    for i in range(1, 101):
        product_id = f"P{str(random.randint(1, 100)).zfill(5)}"
        user_id = f"M{random.randint(1000, 9999)}"

        data.append({
            'follow_id': f"F{str(i).zfill(5)}",
            'product_id': product_id,
            'user_id': user_id,
            'follow_time': fake.date_time_between(start_date='-1y', end_date='now'),
            'etl_time': datetime.now(),
            'dt': datetime.now().strftime('%Y-%m-%d')
        })

    batch_insert('ods_product_follow', data)

# 9. 生成商品转化率数据
def generate_product_conversion(product_info):
    print("开始生成商品转化率数据...")
    data = []

    for product in product_info:
        conversion_rate = round(random.uniform(0.01, 0.1), 4)
        market_avg_conversion = round(random.uniform(0.02, 0.08), 4)

        data.append({
            'product_id': product['product_id'],
            'conversion_rate': conversion_rate,
            'market_avg_conversion': market_avg_conversion,
            'is_declining': random.choices([0, 1], weights=[0.7, 0.3])[0],
            'etl_time': datetime.now(),
            'dt': datetime.now().strftime('%Y-%m-%d')
        })

    batch_insert('ods_product_conversion', data)

# 10. 生成商品库存数据
def generate_product_stock(product_sku):
    print("开始生成商品库存数据...")
    data = []

    for sku in product_sku:
        daily_sale_avg = random.randint(1, 20)
        sale_days = sku['stock'] // daily_sale_avg if daily_sale_avg > 0 else 999

        data.append({
            'product_id': sku['product_id'],
            'sku_id': sku['sku_id'],
            'stock': sku['stock'],
            'daily_sale_avg': daily_sale_avg,
            'sale_days': sale_days,
            'etl_time': datetime.now(),
            'dt': datetime.now().strftime('%Y-%m-%d')
        })

    batch_insert('ods_product_stock', data)

if __name__ == '__main__':
    start_time = time.time()

    # 按顺序生成数据，确保依赖关系
    product_info = generate_product_info()
    product_sku = generate_product_sku(product_info)
    generate_product_sales(product_info, product_sku)
    generate_product_traffic(product_info)
    generate_search_keyword(product_info)
    generate_price_power_product(product_info)
    generate_product_alert()
    generate_product_follow()
    generate_product_conversion(product_info)
    generate_product_stock(product_sku)

    end_time = time.time()
    print(f"所有数据生成完成，总耗时: {end_time - start_time:.2f}秒")