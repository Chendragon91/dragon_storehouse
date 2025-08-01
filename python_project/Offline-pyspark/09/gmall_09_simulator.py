import random
import time
from datetime import datetime, timedelta
from faker import Faker
from tqdm import tqdm
import pymysql
import numpy as np

# 初始化Faker和随机种子
fake = Faker('zh_CN')
random.seed(42)
np.random.seed(42)

# 数据库连接配置
db_config = {
    'host': 'cdh03',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'gmall_09',
    'charset': 'utf8mb4'
}

# 连接数据库
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# 全局变量
TOTAL_RECORDS = 1000000
BATCH_SIZE = 10000  # 每批插入的数据量
current_date = datetime.now().strftime('%Y-%m-%d')

# 中国的省份列表
provinces = ['北京', '上海', '天津', '重庆', '河北', '山西', '辽宁', '吉林', '黑龙江',
             '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南',
             '广东', '海南', '四川', '贵州', '云南', '陕西', '甘肃', '青海', '台湾',
             '内蒙古', '广西', '西藏', '宁夏', '新疆', '香港', '澳门']

# 准备基础数据
def prepare_base_data():
    """准备一些基础数据用于关联"""
    print("准备基础数据...")

    # 生成1000个用户
    user_ids = list(range(1000, 11000))
    user_data = []
    for user_id in tqdm(user_ids, desc="生成用户数据", unit="用户"):
        gender = random.choice(['男', '女'])
        age = random.randint(18, 70)
        register_time = fake.date_time_between(start_date='-3y', end_date='-1d')
        vip_level = np.random.choice([1, 2, 3, 4, 5], p=[0.5, 0.3, 0.1, 0.05, 0.05])
        province = random.choice(provinces)
        city = fake.city()
        phone_prefix = fake.phone_number()[:3]

        user_data.append((
            user_id, fake.user_name(), gender, age, register_time,
            vip_level, province, city, phone_prefix, current_date
        ))

    # 插入用户数据
    cursor.executemany(
        "INSERT INTO user_info VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        user_data
    )
    conn.commit()

    # 生成1000个商品
    product_ids = list(range(10000, 11000))
    product_data = []
    for product_id in tqdm(product_ids, desc="生成商品数据", unit="商品"):
        category_id = random.randint(1, 8)
        category_name = ['手机', '电脑', '家具', '厨具', '男装', '女装', '零食', '饮料'][category_id-1]
        brand_id = random.randint(1, 10)
        brand_name = ['苹果', '华为', '小米', '联想', '美的', '格力', '耐克', '阿迪达斯', '可口可乐', '百事'][brand_id-1]
        price = round(random.uniform(50, 10000), 2)
        cost_price = round(price * random.uniform(0.3, 0.7), 2)
        status = random.choices([0, 1], weights=[0.1, 0.9])[0]
        create_time = fake.date_time_between(start_date='-2y', end_date='-1d')

        product_data.append((
            product_id, fake.text(max_nb_chars=20), category_id, category_name,
            brand_id, brand_name, price, cost_price, status, create_time, current_date
        ))

    # 插入商品数据
    cursor.executemany(
        "INSERT INTO product_info VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        product_data
    )
    conn.commit()

    # 生成100个店铺页面
    page_ids = list(range(100, 200))
    page_data = []
    for page_id in tqdm(page_ids, desc="生成页面数据", unit="页面"):
        page_type = random.choice(['home', 'activity', 'category', 'new', 'hot', 'recommend'])
        page_level = 1 if page_type in ['home', 'activity'] else 2
        parent_page_id = 100 if page_level == 2 else None
        is_active = random.choices([0, 1], weights=[0.05, 0.95])[0]
        create_time = fake.date_time_between(start_date='-1y', end_date='-1d')

        page_data.append((
            page_id, fake.text(max_nb_chars=15), f'/shop/{page_id}',
            page_type, page_level, parent_page_id, is_active, create_time, current_date
        ))

    # 插入店铺页面数据
    cursor.executemany(
        "INSERT INTO shop_page_info VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        page_data
    )
    conn.commit()

    # 生成流量来源数据
    source_data = []
    for source_id in tqdm(range(1, 21), desc="生成流量来源数据", unit="来源"):
        source_type = random.choice(['search', 'social', 'ad', 'direct', 'email', 'referral'])
        source_name = f"{source_type}_{source_id}"
        source_url = f"https://{source_name}.com"
        campaign_id = random.randint(1, 5) if source_type == 'ad' else None
        campaign_name = ['双11', '618', '年货节', '国庆促销', '新品上市'][campaign_id-1] if campaign_id else None

        source_data.append((
            source_id, source_type, source_name, source_url,
            campaign_id, campaign_name, current_date
        ))

    # 插入流量来源数据
    cursor.executemany(
        "INSERT INTO traffic_source VALUES (%s, %s, %s, %s, %s, %s, %s)",
        source_data
    )
    conn.commit()

    # 生成营销活动数据
    activity_data = []
    for activity_id in tqdm(range(1, 11), desc="生成营销活动数据", unit="活动"):
        activity_name = f"活动_{activity_id}"
        start_time = fake.date_time_between(start_date='-30d', end_date='-1d')
        end_time = start_time + timedelta(days=random.randint(1, 7))
        activity_type = random.choice(['discount', 'coupon', 'flash_sale', 'group_buy', 'new_user'])

        activity_data.append((
            activity_id, activity_name, start_time, end_time, activity_type, current_date
        ))

    # 插入营销活动数据
    cursor.executemany(
        "INSERT INTO marketing_activity VALUES (%s, %s, %s, %s, %s, %s)",
        activity_data
    )
    conn.commit()

    # 生成页面跳转关系数据
    relationship_data = []
    for _ in tqdm(range(200), desc="生成页面关系数据", unit="关系"):
        from_page_id = random.choice(page_ids)
        to_page_id = random.choice(page_ids)
        relation_type = random.choice(['click', 'recommend', 'banner', 'search'])
        create_time = fake.date_time_between(start_date='-30d', end_date='-1d')

        relationship_data.append((
            from_page_id, to_page_id, relation_type, create_time, current_date
        ))

    # 插入页面跳转关系数据
    cursor.executemany(
        "INSERT INTO page_relationship VALUES (%s, %s, %s, %s, %s)",
        relationship_data
    )
    conn.commit()

    return user_ids, product_ids, page_ids

def generate_page_visit_log(user_ids, product_ids, page_ids):
    """生成页面访问日志数据"""
    print("\n开始生成页面访问日志数据...")

    # 准备设备类型分布
    device_types = ['mobile', 'pc', 'applet']
    device_weights = [0.6, 0.3, 0.1]

    # 准备操作系统分布
    os_types = ['iOS', 'Android', 'Windows', 'Mac OS', 'Linux']
    os_weights = [0.3, 0.4, 0.2, 0.08, 0.02]

    # 准备浏览器类型分布
    browser_types = ['Chrome', 'Safari', 'Firefox', 'Edge', 'WeChat', 'QQ']
    browser_weights = [0.5, 0.2, 0.1, 0.1, 0.08, 0.02]

    # 准备页面类型分布
    page_types = ['shop', 'product', 'other']
    page_weights = [0.4, 0.4, 0.2]

    # 准备会话ID前缀
    session_prefixes = ['sess_', 'visit_', 'user_']

    # 准备订单数据用于后续关联
    order_data = []

    # 生成数据
    for i in tqdm(range(0, TOTAL_RECORDS, BATCH_SIZE),
                  total=TOTAL_RECORDS//BATCH_SIZE,
                  desc="生成访问日志",
                  unit="批"):
        batch = []
        order_batch = []

        for _ in range(BATCH_SIZE):
            log_id = i + _ + 1
            user_id = random.choice(user_ids)
            session_id = random.choice(session_prefixes) + str(random.randint(100000, 999999))

            # 随机决定页面类型
            page_type = random.choices(page_types, weights=page_weights)[0]

            if page_type == 'product':
                page_id = random.choice(product_ids)
                page_url = f'/product/{page_id}'
            elif page_type == 'shop':
                page_id = random.choice(page_ids)
                page_url = f'/shop/{page_id}'
            else:
                page_id = None
                page_url = fake.uri_path()

            # 随机决定是否有来源页面
            if random.random() < 0.7:  # 70%的访问有来源
                referer_page_id = random.choice(page_ids + product_ids)
                referer_url = f'/{"product" if referer_page_id in product_ids else "shop"}/{referer_page_id}'
            else:
                referer_page_id = None
                referer_url = None

            visit_time = fake.date_time_between(start_date='-30d', end_date='now')
            stay_duration = int(np.random.exponential(30))  # 停留时间符合指数分布
            stay_duration = max(1, min(stay_duration, 600))  # 确保在1-600秒之间

            device_type = random.choices(device_types, weights=device_weights)[0]
            os_type = random.choices(os_types, weights=os_weights)[0]
            browser_type = random.choices(browser_types, weights=browser_weights)[0]
            ip_address = fake.ipv4()
            province = random.choice(provinces)
            city = fake.city()
            is_new_visitor = 1 if random.random() < 0.2 else 0  # 20%是新访客

            batch.append((
                log_id, user_id, session_id, page_url, page_type, page_id,
                referer_url, referer_page_id, visit_time, stay_duration,
                device_type, os_type, browser_type, ip_address, province, city,
                is_new_visitor, current_date
            ))

            # 随机生成一些订单数据（约1%的访问会产生订单）
            if random.random() < 0.01 and page_type == 'product':
                order_id = log_id * 1000
                product_id = page_id
                order_amount = round(random.uniform(50, 5000), 2)
                payment_amount = round(order_amount * random.uniform(0.7, 1.0), 2)

                # 确保order_time在visit_time之后且不超过stay_duration
                min_duration = min(10, stay_duration)
                order_time = visit_time + timedelta(seconds=random.randint(min_duration, stay_duration))

                # 确保payment_time在order_time之后
                payment_time = order_time + timedelta(minutes=random.randint(1, 60))

                order_status = random.choices([0, 1, 2], weights=[0.05, 0.9, 0.05])[0]  # 0未支付,1已支付,2已取消

                order_batch.append((
                    order_id, user_id, product_id, order_amount, payment_amount,
                    order_time, payment_time, order_status, current_date
                ))

        # 插入页面访问日志
        cursor.executemany(
            "INSERT INTO page_visit_log VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            batch
        )

        # 插入订单数据
        if order_batch:
            cursor.executemany(
                "INSERT INTO order_info VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                order_batch
            )

        conn.commit()

def main():
    try:
        start_time = time.time()

        # 清空表数据（可选）
        print("清空表数据...")
        tables = [
            'page_visit_log', 'user_info', 'product_info',
            'shop_page_info', 'order_info', 'traffic_source',
            'marketing_activity', 'page_relationship'
        ]
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
        conn.commit()

        # 准备基础数据
        user_ids, product_ids, page_ids = prepare_base_data()

        # 生成页面访问日志数据
        generate_page_visit_log(user_ids, product_ids, page_ids)

        end_time = time.time()
        print(f"\n数据生成完成！总耗时: {end_time - start_time:.2f}秒")

    except Exception as e:
        print(f"发生错误: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()