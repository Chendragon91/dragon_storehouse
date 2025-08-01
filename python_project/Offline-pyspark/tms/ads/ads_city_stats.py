from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round, when
from pyspark.sql import DataFrame


def get_spark_session():
    spark = SparkSession.builder \
        .appName("TMSCityStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_city_stats(df: DataFrame):
    try:
        target_table = "tms_ads.ads_city_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)

        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e


def execute_city_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行城市统计ETL，目标日期：{target_date}")

    city_order_1d = spark.table("tms_dws.dws_trade_org_cargo_type_order_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("city_id", "city_name") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    trans_origin = spark.table("tms_dws.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == target_date) \
        .select("org_id", "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec")

    organ = spark.table("tms_dim.dim_organ_full") \
        .filter(col("dt") == target_date) \
        .select("id", "org_level", "region_id")

    city_for_level1 = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == target_date) \
        .select("id", "name", "parent_id")

    city_for_level2 = spark.table("tms_dim.dim_region_full") \
        .filter(col("dt") == target_date) \
        .select("id", "name")

    trans_with_city = trans_origin \
        .join(organ, trans_origin.org_id == organ.id, "left") \
        .join(city_for_level1, organ.region_id == city_for_level1.id, "left") \
        .join(city_for_level2, city_for_level1.parent_id == city_for_level2.id, "left") \
        .select(
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("org_level"),
        city_for_level1["id"].alias("city_level1_id"),
        city_for_level1["name"].alias("city_level1_name"),
        city_for_level2["id"].alias("city_level2_id"),
        city_for_level2["name"].alias("city_level2_name")
    )

    trans_1d = trans_with_city \
        .withColumn("city_id",
                    when(col("org_level") == 1, col("city_level1_id"))
                    .otherwise(col("city_level2_id"))) \
        .withColumn("city_name",
                    when(col("org_level") == 1, col("city_level1_name"))
                    .otherwise(col("city_level2_name"))) \
        .filter(col("city_id").isNotNull()) \
        .groupBy("city_id", "city_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    part1d = city_order_1d \
        .join(trans_1d, on=["dt", "recent_days", "city_id", "city_name"], how="full_outer") \
        .select(
        coalesce(city_order_1d["dt"], trans_1d["dt"]).alias("dt"),
        coalesce(city_order_1d["recent_days"], trans_1d["recent_days"]).alias("recent_days"),
        coalesce(city_order_1d["city_id"], trans_1d["city_id"]).alias("city_id"),
        coalesce(city_order_1d["city_name"], trans_1d["city_name"]).alias("city_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("avg_trans_finish_distance"),
        col("avg_trans_finish_dur_sec")
    )

    city_order_nd = spark.table("tms_dws.dws_trade_org_cargo_type_order_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "city_id", "city_name") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(target_date))

    city_trans_nd = spark.table("tms_dws.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "city_id", "city_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date))

    partnd = city_order_nd \
        .join(city_trans_nd, on=["dt", "recent_days", "city_id", "city_name"], how="full_outer") \
        .select(
        coalesce(city_order_nd["dt"], city_trans_nd["dt"]).alias("dt"),
        coalesce(city_order_nd["recent_days"], city_trans_nd["recent_days"]).alias("recent_days"),
        coalesce(city_order_nd["city_id"], city_trans_nd["city_id"]).alias("city_id"),
        coalesce(city_order_nd["city_name"], city_trans_nd["city_name"]).alias("city_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("avg_trans_finish_distance"),
        col("avg_trans_finish_dur_sec")
    )

    final_df = part1d.unionByName(partnd)

    target_columns = [
        "dt", "recent_days", "city_id", "city_name",
        "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    write_to_ads_city_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '20250713'
    execute_city_stats_etl(target_date)