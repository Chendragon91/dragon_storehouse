import random
from faker import Faker
from datetime import datetime, timedelta
import pymysql
from tqdm import tqdm
import numpy as np
import time

# 初始化Faker
fake = Faker('zh_CN')
random.seed(42)
Faker.seed(42)

# 数据库配置
DB_CONFIG = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'gmall_08',
    'port': 3306,
    'charset': 'utf8mb4'
}

# 业务参数配置（10万条版本）
CONFIG = {
    'customer_service_num': 300,       # 300名客服
    'activities_num': 80,              # 80个活动
    'products_per_activity': (5, 30),  # 每个活动5-30个商品
    'skus_per_product': (2, 8),        # 每个商品2-8个SKU
    'offer_records_num': 100000,       # 10万条发放记录
    'redemption_rate': 0.65,           # 65%核销率
    'batch_size': 2000                 # 批量提交大小
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def batch_insert(cursor, table, columns, data):
    """高性能批量插入"""
    sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})"
    cursor.executemany(sql, data)

def truncate_tables(conn):
    """清空表数据"""
    tables = [
        'customer_service_info',
        'cs_special_offer_activity',
        'cs_special_offer_products',
        'cs_special_offer_skus',
        'cs_offer_send_records',
        'cs_offer_redemption_records'
    ]
    with conn.cursor() as cursor:
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
            except Exception as e:
                print(f"清空表 {table} 时出错: {e}")
    conn.commit()

def generate_time_series_data(days_span=30):
    """生成30天内的时间序列数据"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_span)

    def random_time():
        # 工作日(70%) vs 周末(30%)
        if random.random() < 0.7:
            # 工作时间段 9:00-18:00 (70%)
            if random.random() < 0.7:
                hour = random.randint(9, 17)
            # 早晚高峰 8:00-9:00 和 18:00-20:00 (25%)
            else:
                hour = random.choice([8, 18, 19])
        else:
            # 周末时间段 10:00-21:00
            hour = random.randint(10, 20)

        random_day = random.randint(1, days_span)
        return start_date + timedelta(
            days=random_day,
            hours=hour,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

    return random_time

def generate_customer_service(num):
    """生成客服数据"""
    departments = ['客服一部', '客服二部', '客服三部', 'VIP客服部', '国际客服部']
    positions = ['客服专员']*15 + ['高级客服']*3 + ['客服主管']*1 + ['客服经理']*1

    data = []
    for cs_id in tqdm(range(1, num+1), desc="👩💼 生成客服", unit="名"):
        hire_date = fake.date_between(start_date='-30d', end_date='today')
        status = 0 if random.random() < 0.05 else 1
        data.append((
            cs_id,
            fake.name(),
            random.choice(departments),
            random.choice(positions),
            hire_date.strftime('%Y-%m-%d'),
            status
        ))
    return data

def generate_activities(num):
    """生成优惠活动"""
    offer_types = [1, 2, 3]  # 1:满减 2:折扣 3:代金券
    activity_types = ['周年庆', '节日特惠', '会员日', '限时抢购', '新品促销']
    time_generator = generate_time_series_data()

    data = []
    for activity_id in tqdm(range(1, num+1), desc="🎪 生成活动", unit="个"):
        start_time = time_generator()
        duration = min(
            random.choices([3,7,14,21,28], weights=[0.1,0.3,0.3,0.2,0.1])[0],
            30 - (datetime.now() - (datetime.now()-timedelta(days=30))).days
        )
        end_time = start_time + timedelta(days=duration)

        current_status = 1 if datetime.now() > start_time and datetime.now() < end_time else (
            0 if datetime.now() < start_time else 2)

        data.append((
            activity_id,
            f"{random.choice(activity_types)}-{fake.color_name()}优惠",
            random.randint(1, 3),
            random.choice(offer_types),
            start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_time.strftime('%Y-%m-%d %H:%M:%S'),
            round(random.uniform(50, 500), 2) if random.random() > 0.7 else None,
            current_status,
            random.randint(1, 10),
            (start_time - timedelta(days=random.randint(1, 3))).strftime('%Y-%m-%d %H:%M:%S'),
            fake.date_time_between(start_time, end_time).strftime('%Y-%m-%d %H:%M:%S')
        ))
    return data

def generate_activity_products(max_activity_id):
    """生成活动商品"""
    data = []
    product_id = 1
    for activity_id in tqdm(range(1, max_activity_id+1), desc="🛍️ 生成商品", unit="个"):
        for _ in range(random.randint(*CONFIG['products_per_activity'])):
            data.append((
                product_id,
                activity_id,
                product_id,
                round(random.uniform(5, 100), 2) if random.random() > 0.3 else None,
                round(random.uniform(20, 200), 2) if random.random() > 0.5 else None,
                random.randint(1, 5) if random.random() > 0.6 else None,
                random.choices([0, 1], weights=[0.2, 0.8])[0],
                fake.date_time_between('-30d', 'now').strftime('%Y-%m-%d %H:%M:%S')
            ))
            product_id += 1
    return data, product_id-1

def generate_activity_skus(max_product_id):
    """生成商品SKU"""
    data = []
    sku_id = 1
    for product_id in tqdm(range(1, max_product_id+1), desc="📦 生成SKU", unit="个"):
        for _ in range(random.randint(*CONFIG['skus_per_product'])):
            data.append((
                sku_id,
                product_id,
                sku_id,
                round(random.uniform(5, 100), 2) if random.random() > 0.4 else None,
                round(random.uniform(20, 200), 2) if random.random() > 0.6 else None,
                fake.date_time_between('-30d', 'now').strftime('%Y-%m-%d %H:%M:%S')
            ))
            sku_id += 1
    return data, sku_id-1

def generate_offer_records(conn, max_activity_id, max_cs_id, num):
    """生成优惠发放记录"""
    time_generator = generate_time_series_data()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT activity_id, start_time, end_time FROM cs_special_offer_activity")
        activities = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        cursor.execute("SELECT product_id, activity_id FROM cs_special_offer_products")
        products = cursor.fetchall()

        cursor.execute("SELECT sku_id, product_id FROM cs_special_offer_skus")
        skus = cursor.fetchall()
        sku_dict = {}
        for sku in skus:
            if sku[1] not in sku_dict:
                sku_dict[sku[1]] = []
            sku_dict[sku[1]].append(sku[0])

        data = []
        for record_id in tqdm(range(1, num+1), desc="✉️ 发放记录", unit="条"):
            activity_id = random.randint(1, max_activity_id)
            activity_products = [p for p in products if p[1] == activity_id]
            if not activity_products:
                continue

            product = random.choice(activity_products)
            product_id = product[0]
            sku_id = random.choice(sku_dict.get(product_id, [None]))

            act_start, act_end = activities[activity_id]
            send_time = fake.date_time_between(
                start_date=max(act_start, datetime.now()-timedelta(days=30)),
                end_date=min(act_end, datetime.now())
            )

            if random.random() < 0.7:
                expire_hours = random.choice([24, 48, 72])
            else:
                expire_hours = random.choice([96, 120, 144, 168])

            expire_time = send_time + timedelta(hours=expire_hours)
            status = 1  # 初始状态全部设为未使用

            offer_amount = round(np.random.normal(50, 20), 2)
            offer_amount = max(5, min(200, offer_amount))

            data.append((
                record_id, activity_id, product_id, sku_id,
                random.randint(1000, 9999),
                random.randint(1, max_cs_id),
                offer_amount,
                expire_hours,
                send_time.strftime('%Y-%m-%d %H:%M:%S'),
                expire_time.strftime('%Y-%m-%d %H:%M:%S'),
                status,
                fake.sentence() if random.random() > 0.9 else None
            ))

            if len(data) >= CONFIG['batch_size']:
                batch_insert(cursor, 'cs_offer_send_records',
                             ['record_id','activity_id','product_id','sku_id',
                              'customer_id','cs_id','offer_amount','valid_hours',
                              'send_time','expire_time','status','remark'], data)
                data = []
                conn.commit()

        if data:
            batch_insert(cursor, 'cs_offer_send_records',
                         ['record_id','activity_id','product_id','sku_id',
                          'customer_id','cs_id','offer_amount','valid_hours',
                          'send_time','expire_time','status','remark'], data)
            conn.commit()

        return num
    finally:
        cursor.close()

def generate_redemption_records(conn, max_record_id):
    """生成核销记录（确保65%核销率）"""
    cursor = conn.cursor()

    try:
        # 1. 计算需要核销的总数
        total_redemptions = int(max_record_id * CONFIG['redemption_rate'])

        # 2. 获取所有可核销的记录（状态为1且未过期）
        cursor.execute("""
        SELECT record_id, send_time, expire_time, offer_amount 
        FROM cs_offer_send_records 
        WHERE status = 1 
          AND expire_time > NOW()
        ORDER BY RAND()
        LIMIT %s
        """, (total_redemptions,))
        available_records = cursor.fetchall()

        # 3. 实际可核销数量
        actual_redemptions = len(available_records)
        if actual_redemptions < total_redemptions:
            print(f"⚠️ 警告：只有{actual_redemptions}条记录可核销（需要{total_redemptions}条）")

        # 4. 生成核销记录
        data = []
        for redemption_id, record in enumerate(
                tqdm(available_records, desc="💰 核销记录", unit="条", total=actual_redemptions),
                start=1
        ):
            record_id, send_time, expire_time, offer_amount = record

            redemption_time = fake.date_time_between(
                start_date=send_time,
                end_date=min(expire_time, datetime.now())
            )

            actual_amount = round(float(offer_amount) * random.uniform(0.9, 1.0), 2)
            payment_amount = round(actual_amount * random.uniform(2, 10), 2)

            data.append((
                redemption_id,
                record_id,
                random.randint(100000, 999999),
                redemption_time.strftime('%Y-%m-%d %H:%M:%S'),
                actual_amount,
                payment_amount
            ))

            if len(data) >= CONFIG['batch_size']:
                batch_insert(cursor, 'cs_offer_redemption_records',
                             ['redemption_id','record_id','order_id',
                              'redemption_time','actual_offer_amount','payment_amount'], data)
                data = []
                conn.commit()

        if data:
            batch_insert(cursor, 'cs_offer_redemption_records',
                         ['redemption_id','record_id','order_id',
                          'redemption_time','actual_offer_amount','payment_amount'], data)
            conn.commit()

        # 5. 更新发放记录状态
        if actual_redemptions > 0:
            cursor.execute("""
            UPDATE cs_offer_send_records 
            SET status = 2 
            WHERE record_id IN (
                SELECT record_id FROM cs_offer_redemption_records
            )
            """)
            conn.commit()

        return actual_redemptions
    finally:
        cursor.close()

def main():
    print("📊 开始生成10万级测试数据...")
    start_time = time.time()
    conn = get_db_connection()

    try:
        # 清空表
        print("🧹 清空现有数据...", end=' ')
        truncate_tables(conn)
        print("完成")

        # 生成客服
        cs_data = generate_customer_service(CONFIG['customer_service_num'])
        with conn.cursor() as cursor:
            batch_insert(cursor, 'customer_service_info',
                         ['cs_id','cs_name','department','position','hire_date','status'], cs_data)
        conn.commit()
        max_cs_id = CONFIG['customer_service_num']

        # 生成活动
        activity_data = generate_activities(CONFIG['activities_num'])
        with conn.cursor() as cursor:
            batch_insert(cursor, 'cs_special_offer_activity',
                         ['activity_id','activity_name','activity_level','offer_type',
                          'start_time','end_time','max_custom_amount','status',
                          'shop_id','create_time','update_time'], activity_data)
        conn.commit()
        max_activity_id = CONFIG['activities_num']

        # 生成商品
        product_data, max_product_id = generate_activity_products(max_activity_id)
        with conn.cursor() as cursor:
            batch_insert(cursor, 'cs_special_offer_products',
                         ['id','activity_id','product_id','fixed_offer_amount',
                          'max_offer_amount','purchase_limit','status','create_time'], product_data)
        conn.commit()

        # 生成SKU
        sku_data, max_sku_id = generate_activity_skus(max_product_id)
        with conn.cursor() as cursor:
            batch_insert(cursor, 'cs_special_offer_skus',
                         ['id','product_id','sku_id','fixed_offer_amount',
                          'max_offer_amount','create_time'], sku_data)
        conn.commit()

        # 生成发放记录（全部初始状态为1-未使用）
        record_count = generate_offer_records(conn, max_activity_id, max_cs_id, CONFIG['offer_records_num'])

        # 生成核销记录（确保65%核销率）
        redemption_count = generate_redemption_records(conn, record_count)

        # 打印统计
        print("\n✅ 数据生成完成！")
        print(f"""
        ============ 数据统计 ============
        👩💻 客服人员: {max_cs_id}名
        🎡 优惠活动: {max_activity_id}个
        🛒 活动商品: {max_product_id}个
        📦 商品SKU: {max_sku_id}个
        ✉️ 优惠发放: {record_count:,}条
        💰 优惠核销: {redemption_count:,}条 (核销率: {redemption_count/record_count:.0%})
        ⏱️ 总耗时: {time.time()-start_time:.2f}秒
        =================================
        """)

    finally:
        conn.close()

if __name__ == '__main__':
    main()