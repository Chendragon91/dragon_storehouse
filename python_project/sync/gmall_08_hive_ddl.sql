
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_08.ods_cs_offer_redemption_records (
    `redemption_id` BIGINT COMMENT '核销ID',
    `record_id` BIGINT COMMENT '记录ID',
    `order_id` BIGINT COMMENT '订单ID',
    `redemption_time` STRING COMMENT '核销时间',
    `actual_offer_amount` DECIMAL(10,2) COMMENT '实际优惠金额',
    `payment_amount` DECIMAL(10,2) COMMENT '支付金额'
) COMMENT '优惠核销记录表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall_08/ods/ods_cs_offer_redemption_records/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall_08.ods_cs_offer_send_records (
    `record_id` BIGINT COMMENT '记录ID',
    `activity_id` BIGINT COMMENT '活动ID',
    `product_id` BIGINT COMMENT '商品ID',
    `sku_id` BIGINT COMMENT 'SKU ID',
    `customer_id` BIGINT COMMENT '客户ID',
    `cs_id` BIGINT COMMENT '客服ID',
    `offer_amount` DECIMAL(10,2) COMMENT '优惠金额',
    `valid_hours` INT COMMENT '有效小时数',
    `send_time` STRING COMMENT '发送时间',
    `expire_time` STRING COMMENT '过期时间',
    `status` INT COMMENT '状态',
    `remark` STRING COMMENT '备注'
) COMMENT '优惠发送记录表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall_08/ods/ods_cs_offer_send_records/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall_08.ods_cs_special_offer_activity (
    `activity_id` BIGINT COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `activity_level` INT COMMENT '活动级别',
    `offer_type` INT COMMENT '优惠类型',
    `start_time` STRING COMMENT '开始时间',
    `end_time` STRING COMMENT '结束时间',
    `max_custom_amount` DECIMAL(10,2) COMMENT '最大自定义金额',
    `status` INT COMMENT '状态',
    `shop_id` BIGINT COMMENT '店铺ID',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间'
) COMMENT '客服专属优惠活动表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall_08/ods/ods_cs_special_offer_activity/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall_08.ods_cs_special_offer_products (
    `id` BIGINT COMMENT '主键ID',
    `activity_id` BIGINT COMMENT '活动ID',
    `product_id` BIGINT COMMENT '商品ID',
    `fixed_offer_amount` DECIMAL(10,2) COMMENT '固定优惠金额',
    `max_offer_amount` DECIMAL(10,2) COMMENT '最大优惠金额',
    `purchase_limit` INT COMMENT '购买限制',
    `status` INT COMMENT '状态',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '活动商品表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall_08/ods/ods_cs_special_offer_products/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall_08.ods_cs_special_offer_skus (
    `id` BIGINT COMMENT '主键ID',
    `product_id` BIGINT COMMENT '商品ID',
    `sku_id` BIGINT COMMENT 'SKU ID',
    `fixed_offer_amount` DECIMAL(10,2) COMMENT '固定优惠金额',
    `max_offer_amount` DECIMAL(10,2) COMMENT '最大优惠金额',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '活动SKU表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall_08/ods/ods_cs_special_offer_skus/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS gmall_08.ods_customer_service_info (
    `cs_id` BIGINT COMMENT '客服ID',
    `cs_name` STRING COMMENT '客服姓名',
    `department` STRING COMMENT '部门',
    `position` STRING COMMENT '职位',
    `hire_date` STRING COMMENT '入职日期',
    `status` INT COMMENT '状态'
) COMMENT '客服信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall_08/ods/ods_customer_service_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
