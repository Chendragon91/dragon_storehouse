import socket
import random
import string
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from faker import Faker
import uuid
import sys
from decimal import Decimal

# 初始化Faker
fake = Faker('zh_CN')

# MySQL连接配置 - 使用纯Python实现
config = {
    'user': 'root',
    'password': 'root',
    'host': '192.168.142.130',  # 使用IP地址
    'database': 'gmall_01',
    'port': 3306,
    'use_pure': True,  # 使用纯Python实现
    'raise_on_warnings': True,
    'connection_timeout': 10,
    'autocommit': False,
    'use_unicode': True,
    'charset': 'utf8mb4',
    'connect_timeout': 60,
    'buffered': True
}

def test_network_connection(host, port):
    """测试网络连接"""
    try:
        print(f"测试网络连接到 {host}:{port}...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            print(f"网络连接成功: {host}:{port}")
            return True
        else:
            print(f"网络连接失败: {host}:{port} (错误代码: {result})")
            return False
    except Exception as e:
        print(f"网络连接测试出错: {e}")
        return False

def test_dns_resolution(host):
    """测试DNS解析"""
    try:
        print(f"测试DNS解析: {host}")
        ip = socket.gethostbyname(host)
        print(f"DNS解析成功: {host} -> {ip}")
        return True
    except Exception as e:
        print(f"DNS解析失败: {e}")
        return False

def generate_unique_log_id():
    """生成唯一的log_id"""
    return f"log_{uuid.uuid4().hex[:16]}"

def update_progress(current, total, description=""):
    """实时更新进度显示"""
    percent = (current / total) * 100 if total > 0 else 0
    # 使用更简洁的显示方式
    progress_line = f"\r{description} {current:,}/{total:,} ({percent:.1f}%)"
    sys.stdout.write(progress_line)
    sys.stdout.flush()

def batch_insert(cursor, sql, data, batch_size=1000):
    """批量插入数据"""
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(sql, batch)

def generate_mysql_data():
    """生成MySQL模拟数据"""
    conn = None
    cursor = None

    try:
        # 连接MySQL
        print("正在连接数据库...")
        print(f"连接配置: {config}")
        conn = mysql.connector.connect(**config)
        if conn is None:
            print("数据库连接失败: 无法建立连接")
            return

        cursor = conn.cursor()
        print("数据库连接成功")

        print("开始生成模拟数据...")

        # 1. 清空所有表
        print("清空现有表数据...")
        tables = [
            'user_behavior', 'order_info', 'order_detail', 'product_info',
            'product_category', 'micro_detail_visit', 'order_refund',
            'marketing_activity', 'product_favorite', 'shopping_cart',
            'product_competitiveness', 'yearly_payment_snapshot'
        ]
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"表 {table} 清空完成")
            except Exception as e:
                print(f"清空表 {table} 时出错: {e}")

        # 2. 插入商品类目数据
        print("插入商品类目数据...")
        category_data = []
        for cat in categories:
            for dt in date_range:
                category_data.append((cat[0], cat[1], cat[2], int(cat[3]), dt))

        batch_insert(cursor,
                     "INSERT INTO product_category (category_id, category_name, parent_id, is_leaf, dt) VALUES (%s, %s, %s, %s, %s)",
                     category_data)
        print(f"商品类目数据插入完成，共插入 {len(category_data)} 条记录")

        # 3. 插入商品信息数据
        print("插入商品信息数据...")
        product_data = []
        for product in products:
            for dt in date_range:
                product_data.append((*product, dt))

        batch_insert(cursor,
                     "INSERT INTO product_info (product_id, product_name, category_id, price, status, dt) VALUES (%s, %s, %s, %s, %s, %s)",
                     product_data)
        print(f"商品信息数据插入完成，共插入 {len(product_data)} 条记录")

        # 4. 生成用户行为数据（更真实的分布）
        print("生成用户行为数据...")
        behavior_types = ['view', 'cart', 'fav', 'buy']
        page_types = ['detail', 'shop', 'micro_detail']
        terminal_types = ['pc', 'wireless']

        total_behavior_records = 10000 * len(date_range)  # 从40万改为1万
        inserted_count = 0

        # 创建用户活跃度分布（20%的用户贡献80%的行为）
        active_users = random.sample(users, int(len(users) * 0.2))  # 20%活跃用户
        regular_users = [u for u in users if u not in active_users]  # 80%普通用户

        for dt_index, dt in enumerate(date_range):
            current_date = datetime.strptime(dt, '%Y-%m-%d')

            # 不同时间段行为分布不同
            # 工作日高峰时段：10-12点, 14-17点, 20-22点
            peak_hours = [(10, 12), (14, 17), (20, 22)]
            off_hours = [(0, 9), (13, 13), (18, 19), (23, 23)]

            print(f"\n生成日期 {dt} 的用户行为数据...")
            behavior_data = []

            for i in range(10000):  # 从40万改为1万（每天1万条）
                # 根据活跃度选择用户
                if random.random() < 0.8:  # 80%概率选择活跃用户
                    user_id = random.choice(active_users)
                else:
                    user_id = random.choice(regular_users)

                visitor_id = random.choice(visitors)
                product_id = random.choice(products)[0]

                # 更真实的浏览行为权重
                behavior_type = random.choices(
                    behavior_types,
                    weights=[0.65, 0.2, 0.1, 0.05],  # 浏览65%，加购20%，收藏10%，购买5%
                    k=1
                )[0]

                # 根据行为类型设置时间分布
                if behavior_type == 'buy':
                    # 购买行为更多在高峰时段
                    hour = random.choices(
                        [h for hours in peak_hours for h in range(hours[0], hours[1]+1)] +
                        [h for hours in off_hours for h in range(hours[0], hours[1]+1)],
                        weights=[2]*13 + [1]*11,  # 高峰时段权重更高
                        k=1
                    )[0]
                else:
                    # 其他行为时间分布相对均匀
                    hour = random.randint(0, 23)

                behavior_time = current_date.replace(
                    hour=hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                stay_duration = random.randint(1, 300) if behavior_type == 'view' else 0
                terminal_type = random.choice(terminal_types)
                page_type = random.choice(page_types)
                click_behavior = fake.word() if random.random() < 0.3 else None

                behavior_data.append((
                    generate_unique_log_id(),  # 使用新的唯一ID生成方法
                    user_id,
                    visitor_id,
                    product_id,
                    behavior_type,
                    behavior_time,
                    stay_duration,
                    terminal_type,
                    page_type,
                    click_behavior,
                    dt
                ))

                # 每收集1000条数据就批量插入一次（原为1万条）
                if len(behavior_data) >= 1000:
                    batch_insert(cursor,
                                 "INSERT INTO user_behavior (log_id, user_id, visitor_id, product_id, behavior_type, behavior_time, stay_duration, terminal_type, page_type, click_behavior, dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                 behavior_data)
                    inserted_count += len(behavior_data)
                    update_progress(inserted_count, total_behavior_records, f"用户行为[{dt}]:")
                    behavior_data = []  # 清空数据列表

            # 插入剩余的数据
            if behavior_data:
                batch_insert(cursor,
                             "INSERT INTO user_behavior (log_id, user_id, visitor_id, product_id, behavior_type, behavior_time, stay_duration, terminal_type, page_type, click_behavior, dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                             behavior_data)
                inserted_count += len(behavior_data)
                update_progress(inserted_count, total_behavior_records, f"用户行为[{dt}]:")

            print(f"\n日期 {dt} 的用户行为数据生成完成")

        print("\n用户行为数据生成完成")

        # 5. 生成订单数据（按比例从2000改为500）
        print("生成订单数据...")
        order_statuses = ['pending', 'paid', 'shipped', 'completed', 'cancelled']

        total_orders = 500 * len(date_range)  # 从2000改为500
        order_count = 0

        # 创建用户购买力分布
        high_value_users = random.sample(users, int(len(users) * 0.1))  # 10%高价值用户
        medium_value_users = random.sample([u for u in users if u not in high_value_users], int(len(users) * 0.3))  # 30%中等价值用户
        low_value_users = [u for u in users if u not in high_value_users and u not in medium_value_users]  # 60%低价值用户

        for dt in date_range:
            current_date = datetime.strptime(dt, '%Y-%m-%d')

            # 订单时间分布（模拟真实购物时间）
            order_hours = list(range(8, 24))  # 8点到23点
            hour_weights = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6, 5]  # 不同时间下单概率不同

            print(f"\n生成日期 {dt} 的订单数据...")
            order_info_data = []
            order_detail_data = []

            # 每天生成500个订单（原为2000）
            for order_num in range(1, 501):
                # 选择用户并根据用户类型设置订单金额
                user_id = random.choice(users)
                if user_id in high_value_users:
                    payment_amount = round(random.uniform(500, 10000), 2)
                elif user_id in medium_value_users:
                    payment_amount = round(random.uniform(200, 2000), 2)
                else:
                    payment_amount = round(random.uniform(50, 500), 2)

                # 设置订单时间
                order_hour = random.choices(order_hours, weights=hour_weights, k=1)[0]
                payment_time = current_date.replace(
                    hour=order_hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                # 订单状态分布（更真实）
                order_status = random.choices(
                    order_statuses,
                    weights=[0.05, 0.7, 0.15, 0.08, 0.02],  # 待支付5%，已支付70%，已发货15%，已完成8%，已取消2%
                    k=1
                )[0]

                terminal_type = random.choice(terminal_types)
                is_new_buyer = random.choice([0, 1])

                order_id = f"order_{dt.replace('-', '')}_{order_num:05d}"

                order_info_data.append((
                    order_id,
                    user_id,
                    payment_amount if order_status in ['paid', 'shipped', 'completed'] else 0,
                    payment_time if order_status in ['paid', 'shipped', 'completed'] else None,
                    order_status,
                    terminal_type,
                    is_new_buyer,
                    dt
                ))

                # 每个订单1-10个商品（更真实）
                num_products = random.choices([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                              weights=[0.4, 0.25, 0.15, 0.08, 0.05, 0.03, 0.02, 0.01, 0.005, 0.005],
                                              k=1)[0]
                order_products = random.sample(products, min(num_products, len(products)))

                for product in order_products:
                    # 商品数量分布（大部分1件，少量多件）
                    quantity = random.choices([1, 2, 3, 4, 5], weights=[0.7, 0.2, 0.06, 0.03, 0.01], k=1)[0]
                    order_detail_data.append((
                        order_id,
                        product[0],
                        product[1],
                        product[2],
                        product[3],
                        quantity,
                        dt
                    ))

                order_count += 1
                if order_count % 50 == 0:  # 每50个订单更新一次进度（原为200）
                    update_progress(order_count, total_orders, f"订单数据[{dt}]:")

            # 批量插入订单信息
            batch_insert(cursor,
                         "INSERT INTO order_info (order_id, user_id, payment_amount, payment_time, order_status, terminal_type, is_new_buyer, dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                         order_info_data)

            # 批量插入订单详情
            batch_insert(cursor,
                         "INSERT INTO order_detail (order_id, product_id, product_name, category_id, price, quantity, dt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                         order_detail_data)

            print(f"\n日期 {dt} 的订单数据生成完成")

        print("\n订单数据生成完成")

        # 6. 生成微详情访问数据（从5万改为1250）
        print("生成微详情访问数据...")
        total_visits = 1250 * len(date_range)  # 从5万改为1250
        visit_count = 0

        for dt in date_range:
            current_date = datetime.strptime(dt, '%Y-%m-%d')

            print(f"\n生成日期 {dt} 的微详情访问数据...")
            visit_data = []

            for i in range(1250):  # 从5万改为1250（每天1250条）
                visitor_id = random.choice(visitors)
                product_id = random.choice(products)[0]
                stay_duration = random.randint(5, 600)  # 更真实的停留时间

                # 微详情访问时间分布
                visit_hour = random.randint(8, 23)
                visit_time = current_date.replace(
                    hour=visit_hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                visit_data.append((
                    f"visit_{uuid.uuid4().hex[:16]}",  # 使用UUID确保唯一性
                    visitor_id,
                    product_id,
                    stay_duration,
                    visit_time,
                    dt
                ))

                # 每收集125条数据就批量插入一次（原为5000）
                if len(visit_data) >= 125:
                    batch_insert(cursor,
                                 "INSERT INTO micro_detail_visit (visit_id, visitor_id, product_id, stay_duration, visit_time, dt) VALUES (%s, %s, %s, %s, %s, %s)",
                                 visit_data)
                    visit_count += len(visit_data)
                    update_progress(visit_count, total_visits, f"微详情访问[{dt}]:")
                    visit_data = []  # 清空数据列表

            # 插入剩余的数据
            if visit_data:
                batch_insert(cursor,
                             "INSERT INTO micro_detail_visit (visit_id, visitor_id, product_id, stay_duration, visit_time, dt) VALUES (%s, %s, %s, %s, %s, %s)",
                             visit_data)
                visit_count += len(visit_data)
                update_progress(visit_count, total_visits, f"微详情访问[{dt}]:")

            print(f"\n日期 {dt} 的微详情访问数据生成完成")

        print("\n微详情访问数据生成完成")

        # 7. 生成退款数据（按订单比例自动调整）
        print("生成退款数据...")
        refund_types = ['only_refund', 'refund_return']

        for dt in date_range:
            # 获取已支付的订单
            cursor.execute(f"SELECT order_id, user_id FROM order_info WHERE order_status IN ('paid', 'shipped', 'completed') AND dt='{dt}'")
            paid_orders = cursor.fetchall()

            # 约3-8%的订单有退款（更真实）
            refund_rate = random.uniform(0.03, 0.08)
            refund_orders = random.sample(paid_orders, max(1, int(len(paid_orders) * refund_rate)))

            total_refunds = len(refund_orders)
            refund_count = 0

            print(f"\n生成日期 {dt} 的退款数据...")
            refund_data = []

            for order in refund_orders:
                order_id = order[0]
                user_id = order[1]

                # 获取订单中的商品
                cursor.execute(f"SELECT product_id FROM order_detail WHERE order_id='{order_id}' AND dt='{dt}'")
                order_products = cursor.fetchall()
                refund_product = random.choice(order_products)[0]

                # 获取商品价格
                cursor.execute(f"SELECT price FROM product_info WHERE product_id='{refund_product}' AND dt='{dt}'")
                result = cursor.fetchone()
                if result:
                    product_price = result[0]

                    # 退款金额分布（部分退款更常见）
                    refund_ratio = random.choices([1.0, 0.8, 0.5, 0.3], weights=[0.6, 0.25, 0.1, 0.05], k=1)[0]
                    refund_amount = round(float(product_price) * refund_ratio, 2)

                    refund_type = random.choice(refund_types)

                    # 退款时间（通常在购买后几天内）
                    cursor.execute(f"SELECT payment_time FROM order_info WHERE order_id='{order_id}' AND dt='{dt}'")
                    payment_result = cursor.fetchone()
                    if payment_result and payment_result[0]:
                        payment_time = payment_result[0]
                        # 退款时间在购买后1-30天内
                        refund_delay = timedelta(days=random.randint(1, 30))
                        refund_time = payment_time + refund_delay
                    else:
                        refund_time = generate_random_date(
                            datetime.strptime(dt, '%Y-%m-%d').replace(hour=12, minute=0, second=0),
                            datetime.strptime(dt, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
                        )

                    refund_data.append((
                        f"refund_{uuid.uuid4().hex[:16]}",
                        order_id,
                        refund_product,
                        refund_amount,
                        refund_type,
                        refund_time,
                        dt
                    ))

                refund_count += 1
                update_progress(refund_count, total_refunds, f"退款数据[{dt}]:")

            # 批量插入退款数据
            if refund_data:
                batch_insert(cursor,
                             "INSERT INTO order_refund (refund_id, order_id, product_id, refund_amount, refund_type, refund_time, dt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                             refund_data)

            print(f"\n日期 {dt} 的退款数据生成完成")

        print("\n退款数据生成完成")

        # 8. 生成营销活动数据（按订单比例自动调整）
        print("生成营销活动数据...")
        activity_types = ['juhuasuan', 'shuang11', '618', 'qingcang', 'new_user']
        activity_weights = [0.25, 0.15, 0.2, 0.25, 0.15]  # 各活动类型分布

        for dt in date_range:
            # 获取已支付的订单
            cursor.execute(f"SELECT order_id FROM order_info WHERE order_status IN ('paid', 'shipped', 'completed') AND dt='{dt}'")
            paid_orders = cursor.fetchall()

            # 约15-25%的订单参与活动（更真实）
            activity_rate = random.uniform(0.15, 0.25)
            activity_orders = random.sample(paid_orders, max(1, int(len(paid_orders) * activity_rate)))

            total_activities = len(activity_orders)
            activity_count = 0

            print(f"\n生成日期 {dt} 的营销活动数据...")
            activity_data = []

            for order in activity_orders:
                order_id = order[0]
                activity_type = random.choices(activity_types, weights=activity_weights, k=1)[0]

                # 不同活动类型的折扣分布
                if activity_type in ['shuang11', '618']:
                    discount_amount = round(random.uniform(50, 500), 2)  # 大促活动折扣更高
                elif activity_type == 'juhuasuan':
                    discount_amount = round(random.uniform(20, 200), 2)  # 聚划算中等折扣
                else:
                    discount_amount = round(random.uniform(5, 100), 2)   # 其他活动折扣较低

                activity_data.append((
                    f"act_{uuid.uuid4().hex[:16]}",
                    order_id,
                    activity_type,
                    discount_amount,
                    dt
                ))

                activity_count += 1
                update_progress(activity_count, total_activities, f"营销活动[{dt}]:")

            # 批量插入营销活动数据
            if activity_data:
                batch_insert(cursor,
                             "INSERT INTO marketing_activity (activity_id, order_id, activity_type, discount_amount, dt) VALUES (%s, %s, %s, %s, %s)",
                             activity_data)

            print(f"\n日期 {dt} 的营销活动数据生成完成")

        print("\n营销活动数据生成完成")

        # 9. 生成商品收藏数据（从1万改为250）
        print("生成商品收藏数据...")
        total_favorites = 250 * len(date_range)  # 从1万改为250
        fav_count = 0

        for dt in date_range:
            current_date = datetime.strptime(dt, '%Y-%m-%d')

            print(f"\n生成日期 {dt} 的商品收藏数据...")
            favorite_data = []

            for i in range(250):  # 从1万改为250（每天250条）
                user_id = random.choice(users)
                product_id = random.choice(products)[0]

                # 收藏时间分布
                fav_hour = random.randint(8, 23)
                fav_time = current_date.replace(
                    hour=fav_hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                # 取消收藏的概率（20%会取消）
                is_cancel = random.choice([0, 1]) if random.random() < 0.2 else 0

                favorite_data.append((
                    f"fav_{uuid.uuid4().hex[:16]}",
                    user_id,
                    product_id,
                    fav_time,
                    is_cancel,
                    dt
                ))

                # 每收集25条数据就批量插入一次（原为1000）
                if len(favorite_data) >= 25:
                    batch_insert(cursor,
                                 "INSERT INTO product_favorite (fav_id, user_id, product_id, fav_time, is_cancel, dt) VALUES (%s, %s, %s, %s, %s, %s)",
                                 favorite_data)
                    fav_count += len(favorite_data)
                    update_progress(fav_count, total_favorites, f"商品收藏[{dt}]:")
                    favorite_data = []  # 清空数据列表

            # 插入剩余的数据
            if favorite_data:
                batch_insert(cursor,
                             "INSERT INTO product_favorite (fav_id, user_id, product_id, fav_time, is_cancel, dt) VALUES (%s, %s, %s, %s, %s, %s)",
                             favorite_data)
                fav_count += len(favorite_data)
                update_progress(fav_count, total_favorites, f"商品收藏[{dt}]:")

            print(f"\n日期 {dt} 的商品收藏数据生成完成")

        print("\n商品收藏数据生成完成")

        # 10. 生成购物车数据（从1.5万改为375）
        print("生成购物车数据...")
        total_carts = 375 * len(date_range)  # 从1.5万改为375
        cart_count = 0

        for dt in date_range:
            current_date = datetime.strptime(dt, '%Y-%m-%d')

            print(f"\n生成日期 {dt} 的购物车数据...")
            cart_data = []

            for i in range(375):  # 从1.5万改为375（每天375条）
                user_id = random.choice(users)
                product_id = random.choice(products)[0]

                # 购物车商品数量分布
                quantity = random.choices([1, 2, 3, 4, 5], weights=[0.7, 0.2, 0.06, 0.03, 0.01], k=1)[0]

                # 加入购物车时间
                cart_hour = random.randint(8, 23)
                cart_time = current_date.replace(
                    hour=cart_hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                # 删除购物车的概率（30%会删除）
                is_delete = random.choice([0, 1]) if random.random() < 0.3 else 0

                cart_data.append((
                    f"cart_{uuid.uuid4().hex[:16]}",
                    user_id,
                    product_id,
                    quantity,
                    cart_time,
                    is_delete,
                    dt
                ))

                # 每收集37条数据就批量插入一次（原为1500）
                if len(cart_data) >= 37:
                    batch_insert(cursor,
                                 "INSERT INTO shopping_cart (cart_id, user_id, product_id, quantity, cart_time, is_delete, dt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                 cart_data)
                    cart_count += len(cart_data)
                    update_progress(cart_count, total_carts, f"购物车[{dt}]:")
                    cart_data = []  # 清空数据列表

            # 插入剩余的数据
            if cart_data:
                batch_insert(cursor,
                             "INSERT INTO shopping_cart (cart_id, user_id, product_id, quantity, cart_time, is_delete, dt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                             cart_data)
                cart_count += len(cart_data)
                update_progress(cart_count, total_carts, f"购物车[{dt}]:")

            print(f"\n日期 {dt} 的购物车数据生成完成")

        print("\n购物车数据生成完成")

        # 11. 生成商品竞争力评分数据（保持不变，因为与用户行为无关）
        print("生成商品竞争力评分数据...")
        score_dimensions = ['流量获取', '转化', '内容营销', '客户拉新', '服务质量']

        for dt in date_range:
            current_date = datetime.strptime(dt, '%Y-%m-%d')

            print(f"\n生成日期 {dt} 的商品竞争力评分数据...")
            competitiveness_data = []

            for product in products:
                # 评分时间分布
                score_hour = random.randint(9, 18)
                score_time = current_date.replace(
                    hour=score_hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )

                # 评分分布（大部分商品评分较高）
                score = round(random.choices(
                    [random.uniform(4.5, 5.0), random.uniform(4.0, 4.5), random.uniform(3.5, 4.0), random.uniform(3.0, 3.5)],
                    weights=[0.4, 0.35, 0.2, 0.05],
                    k=1
                )[0], 2)

                # 评价维度（通常选择3-5个维度）
                num_dimensions = random.randint(3, 5)
                dimensions = random.sample(score_dimensions, num_dimensions)

                competitiveness_data.append((
                    product[0],
                    score,
                    score_time,
                    ','.join(dimensions),
                    dt
                ))

            # 批量插入竞争力评分数据
            batch_insert(cursor,
                         "INSERT INTO product_competitiveness (product_id, score, score_time, score_dimensions, dt) VALUES (%s, %s, %s, %s, %s)",
                         competitiveness_data)

            print(f"\n日期 {dt} 的商品竞争力评分数据生成完成")

        print("\n商品竞争力评分数据生成完成")

        # 12. 生成商品年累计支付金额快照数据（保持不变）
        print("生成商品年累计支付金额快照数据...")

        for dt in date_range:
            print(f"\n生成日期 {dt} 的支付快照数据...")
            snapshot_data = []

            for product in products:
                # 年累计支付金额（根据商品价格和销量计算）
                base_amount = float(product[3]) * random.uniform(10, 1000)  # 基础金额
                year_payment_amount = round(base_amount, 2)

                snapshot_data.append((
                    product[0],
                    year_payment_amount,
                    dt,
                    dt
                ))

            # 批量插入支付快照数据
            batch_insert(cursor,
                         "INSERT INTO yearly_payment_snapshot (product_id, year_payment_amount, snapshot_date, dt) VALUES (%s, %s, %s, %s)",
                         snapshot_data)

            print(f"\n日期 {dt} 的支付快照数据生成完成")

        print("\n商品年累计支付金额快照数据生成完成")

        print("\n所有数据生成完成！")

        # 提交事务
        conn.commit()
        print("事务提交完成！")

    except mysql.connector.Error as e:
        print(f"\nMySQL数据库错误: {e}")
        print(f"错误代码: {e.errno}")
        print(f"SQL状态: {e.sqlstate}")
        if conn:
            conn.rollback()
    except Error as e:
        print(f"\n数据库连接错误: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn is not None:
            try:
                if conn.is_connected():
                    cursor.close()
                    conn.close()
                    print("数据库连接已关闭")
            except Exception as e:
                print(f"关闭数据库连接时出错: {e}")

# 商品类目数据
categories = [
    ('101', '女装', '1', '1'),
    ('10101', '连衣裙', '101', '1'),
    ('10102', 'T恤', '101', '1'),
    ('10103', '衬衫', '101', '1'),
    ('102', '男装', '2', '1'),
    ('10201', '夹克', '102', '1'),
    ('10202', '牛仔裤', '102', '1'),
    ('10203', '休闲裤', '102', '1'),
    ('103', '数码', '3', '1'),
    ('10301', '手机', '103', '1'),
    ('10302', '笔记本电脑', '103', '1'),
    ('10303', '平板电脑', '103', '1'),
    ('104', '家居', '4', '1'),
    ('10401', '床上用品', '104', '1'),
    ('10402', '厨房用品', '104', '1'),
    ('10403', '收纳用品', '104', '1')
]

# 商品数据
products = []
for i in range(1, 501):  # 500个商品
    category = random.choice([c for c in categories if c[3] == '1'])  # 只选叶子类目
    price = round(random.uniform(10, 5000), 2)
    products.append((
        f'p{i:05d}',
        f'商品{i}',
        category[0],
        price,
        random.choice([0, 1])
    ))

# 用户数据
users = [f'u{i:05d}' for i in range(1, 1001)]  # 1000个用户
visitors = [f'v{i:07d}' for i in range(1, 5001)]  # 5000个访客

# 日期范围
date_range = ['2025-08-05', '2025-08-06', '2025-08-07']

def generate_random_date(start_date, end_date):
    """生成随机日期时间"""
    delta = end_date - start_date
    random_seconds = random.randint(0, delta.total_seconds())
    return start_date + timedelta(seconds=random_seconds)

if __name__ == "__main__":
    try:
        generate_mysql_data()
    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()