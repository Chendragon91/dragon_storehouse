import random
from faker import Faker
from datetime import datetime, timedelta
import pymysql
from tqdm import tqdm

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

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def generate_and_insert_customer_service(conn, num=20):
    departments = ['客服一部', '客服二部', '客服三部', 'VIP客服部']
    positions = ['客服专员', '客服主管', '客服经理', '高级客服']

    with conn.cursor() as cursor:
        for cs_id in tqdm(range(1, num+1), desc="生成客服信息"):
            hire_date = fake.date_between(start_date='-5y', end_date='today')
            cursor.execute(
                "INSERT INTO customer_service_info VALUES (%s, %s, %s, %s, %s, %s)",
                (cs_id, fake.name(), random.choice(departments),
                 random.choice(positions), hire_date.strftime('%Y-%m-%d'),
                 random.choices([0, 1], weights=[0.1, 0.9])[0])
            )
    conn.commit()

def generate_and_insert_activities(conn, num=15):
    offer_types = [1, 2, 3]

    with conn.cursor() as cursor:
        for activity_id in tqdm(range(1, num+1), desc="生成优惠活动"):
            start_time = fake.date_time_between(start_date='-2m', end_date='+1m')
            end_time = start_time + timedelta(days=random.randint(7, 30))
            current_status = 1 if datetime.now() > start_time and datetime.now() < end_time else (
                0 if datetime.now() < start_time else 2)

            cursor.execute(
                """INSERT INTO cs_special_offer_activity 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (activity_id, f"{fake.word().capitalize()}专属优惠活动", random.randint(1, 3),
                 random.choice(offer_types), start_time.strftime('%Y-%m-%d %H:%M:%S'),
                 end_time.strftime('%Y-%m-%d %H:%M:%S'),
                 round(random.uniform(50, 500), 2) if random.random() > 0.7 else None,
                 current_status, random.randint(1, 10),
                 (start_time - timedelta(days=random.randint(1, 7))).strftime('%Y-%m-%d %H:%M:%S'),
                 fake.date_time_between(start_date=start_time, end_date=end_time).strftime('%Y-%m-%d %H:%M:%S'))
            )
    conn.commit()

def generate_and_insert_activity_products(conn, max_activity_id, num_per_activity=3):
    with conn.cursor() as cursor:
        product_id = 1
        for activity_id in tqdm(range(1, max_activity_id+1), desc="生成活动商品"):
            for _ in range(random.randint(1, num_per_activity)):
                cursor.execute(
                    """INSERT INTO cs_special_offer_products 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (product_id, activity_id, product_id,
                     round(random.uniform(5, 100), 2) if random.random() > 0.3 else None,
                     round(random.uniform(20, 200), 2) if random.random() > 0.5 else None,
                     random.randint(1, 5) if random.random() > 0.6 else None,
                     random.choices([0, 1], weights=[0.2, 0.8])[0],
                     fake.date_time_between(start_date='-2m', end_date='now').strftime('%Y-%m-%d %H:%M:%S'))
                )
                product_id += 1
    conn.commit()
    return product_id - 1

def generate_and_insert_activity_skus(conn, max_product_id, num_per_product=2):
    with conn.cursor() as cursor:
        sku_id = 1
        for product_id in tqdm(range(1, max_product_id+1), desc="生成活动SKU"):
            for _ in range(random.randint(1, num_per_product)):
                cursor.execute(
                    """INSERT INTO cs_special_offer_skus 
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (sku_id, product_id, sku_id,
                     round(random.uniform(5, 100), 2) if random.random() > 0.4 else None,
                     round(random.uniform(20, 200), 2) if random.random() > 0.6 else None,
                     fake.date_time_between(start_date='-2m', end_date='now').strftime('%Y-%m-%d %H:%M:%S'))
                )
                sku_id += 1
    conn.commit()
    return sku_id - 1

def generate_and_insert_offer_records(conn, max_activity_id, max_cs_id, num=200):
    with conn.cursor() as cursor:
        cursor.execute("SELECT product_id, activity_id FROM cs_special_offer_products")
        products = cursor.fetchall()

        cursor.execute("SELECT sku_id, product_id FROM cs_special_offer_skus")
        skus = cursor.fetchall()
        sku_dict = {}
        for sku in skus:
            if sku[1] not in sku_dict:
                sku_dict[sku[1]] = []
            sku_dict[sku[1]].append(sku[0])

        for record_id in tqdm(range(1, num+1), desc="生成发送记录"):
            activity_id = random.randint(1, max_activity_id)
            activity_products = [p for p in products if p[1] == activity_id]
            if not activity_products:
                continue

            product = random.choice(activity_products)
            product_id = product[0]
            sku_id = random.choice(sku_dict.get(product_id, [None]))

            # 确保有足够多的过期记录
            if random.random() < 0.4:  # 40%的记录强制为已过期
                send_time = fake.date_time_between(start_date='-2m', end_date='-1d')
                expire_time = send_time + timedelta(hours=random.randint(1, 23))
                status = 0
            else:
                send_time = fake.date_time_between(start_date='-1d', end_date='now')
                expire_time = send_time + timedelta(hours=random.choice([24, 48, 72, 168]))
                status = 1 if datetime.now() < expire_time else 0

            cursor.execute(
                """INSERT INTO cs_offer_send_records 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (record_id, activity_id, product_id, sku_id,
                 random.randint(1000, 9999), random.randint(1, max_cs_id),
                 round(random.uniform(5, 200), 2), random.choice([24, 48, 72, 168]),
                 send_time.strftime('%Y-%m-%d %H:%M:%S'), expire_time.strftime('%Y-%m-%d %H:%M:%S'),
                 status, fake.sentence() if random.random() > 0.7 else None)
            )
    conn.commit()
    return num

def generate_and_insert_redemption_records(conn, max_record_id, redemption_rate=0.7):
    with conn.cursor() as cursor:
        # 调试查询1：检查状态分布
        cursor.execute("SELECT status, COUNT(*) FROM cs_offer_send_records GROUP BY status")
        status_dist = cursor.fetchall()
        print(f"发送记录状态分布: {status_dist}")

        # 调试查询2：直接查询status=0的记录
        cursor.execute("SELECT COUNT(*) FROM cs_offer_send_records WHERE status = 0")
        expired_count = cursor.fetchone()[0]
        print(f"状态为0的记录总数: {expired_count}")

        # 关键修改：移除expire_time < NOW()条件，因为status=0已经表示过期
        cursor.execute("""
        SELECT record_id, send_time, expire_time, offer_amount 
        FROM cs_offer_send_records 
        WHERE status = 0
        """)
        expired_records = cursor.fetchall()
        print(f"找到{len(expired_records)}条过期记录可用于核销")

        redemption_id = 1
        for record in tqdm(expired_records, desc="生成核销记录"):
            if random.random() < redemption_rate:
                record_id, send_time, expire_time, offer_amount = record
                redemption_time = fake.date_time_between(
                    start_date=send_time,
                    end_date=expire_time
                )

                cursor.execute(
                    """INSERT INTO cs_offer_redemption_records 
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (redemption_id, record_id, random.randint(100000, 999999),
                     redemption_time.strftime('%Y-%m-%d %H:%M:%S'),
                     round(float(offer_amount) * random.uniform(0.9, 1.0), 2),
                     round(random.uniform(100, 2000), 2))
                )
                redemption_id += 1
        print(f"成功生成{redemption_id-1}条核销记录")
    conn.commit()

def main():
    conn = get_db_connection()

    try:
        generate_and_insert_customer_service(conn, num=20)
        max_cs_id = 20

        generate_and_insert_activities(conn, num=15)
        max_activity_id = 15

        max_product_id = generate_and_insert_activity_products(conn, max_activity_id)

        generate_and_insert_activity_skus(conn, max_product_id)

        record_count = generate_and_insert_offer_records(conn, max_activity_id, max_cs_id, num=200)

        generate_and_insert_redemption_records(conn, record_count)

        print("\n所有模拟数据已成功插入数据库！")

    finally:
        conn.close()

if __name__ == '__main__':
    main()