use tms_dim;

set hive.exec.mode.local.auto=true;

drop table if exists dim_complex_full;
create external table dim_complex_full(
                                          `id` bigint comment '小区ID',
                                          `complex_name` string comment '小区名称',
                                          `courier_emp_ids` array<string> comment '负责快递员IDS',
                                          `province_id` bigint comment '省份ID',
                                          `province_name` string comment '省份名称',
                                          `city_id` bigint comment '城市ID',
                                          `city_name` string comment '城市名称',
                                          `district_id` bigint comment '区（县）ID',
                                          `district_name` string comment '区（县）名称'
) comment '小区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_complex_full'
    tblproperties('orc.compress'='snappy');


WITH
    complex_info AS (
        SELECT id, complex_name, province_id, city_id, district_id, district_name
        FROM tms.ods_base_complex
        WHERE dt = '20250713' AND is_deleted = '0'
    ),
    region_dict AS (
        SELECT id, name
        FROM tms.ods_base_region_info
        WHERE dt = '20250713' AND is_deleted = '0'
    ),
    courier_data AS (
        SELECT
            complex_id,
            collect_set(cast(courier_emp_id AS STRING)) AS courier_emp_ids
        FROM tms.ods_express_courier_complex
        WHERE dt = '20250713' AND is_deleted = '0'
        GROUP BY complex_id
    )

INSERT OVERWRITE TABLE dim_complex_full PARTITION (dt = '20250713')
SELECT
    c.id,
    c.complex_name,
    cd.courier_emp_ids,
    c.province_id,
    p.name AS province_name,
    c.city_id,
    ci.name AS city_name,
    c.district_id,
    c.district_name
FROM complex_info c
         JOIN region_dict p ON c.province_id = p.id
         JOIN region_dict ci ON c.city_id = ci.id
         LEFT JOIN courier_data cd ON c.id = cd.complex_id;

select * from dim_complex_full;


drop table if exists dim_organ_full;
create external table dim_organ_full(
                                        `id` bigint COMMENT '机构ID',
                                        `org_name` string COMMENT '机构名称',
                                        `org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
                                        `region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
                                        `region_name` string COMMENT '地区名称',
                                        `region_code` string COMMENT '地区编码（行政级别）',
                                        `org_parent_id` bigint COMMENT '父级机构ID',
                                        `org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_organ_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table dim_organ_full
    partition (dt = '20250713')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from tms.ods_base_organ
      where dt = '20250713'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from tms.ods_base_region_info
    where dt = '20250713'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from tms.ods_base_organ
    where dt = '20250713'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;

select * from dim_organ_full;

drop table if exists dim_region_full;
create external table dim_region_full(
                                         `id` bigint COMMENT '地区ID',
                                         `parent_id` bigint COMMENT '上级地区ID',
                                         `name` string COMMENT '地区名称',
                                         `dict_code` string COMMENT '编码（行政级别）',
                                         `short_name` string COMMENT '简称'
) comment '地区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_region_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table dim_region_full
    partition (dt = '20250713')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from tms.ods_base_region_info
where dt = '20250713'
  and is_deleted = '0';


select * from dim_region_full;


drop table if exists dim_express_courier_full;
create external table dim_express_courier_full(
                                                  `id` bigint COMMENT '快递员ID',
                                                  `emp_id` bigint COMMENT '员工ID',
                                                  `org_id` bigint COMMENT '所属机构ID',
                                                  `org_name` string COMMENT '机构名称',
                                                  `working_phone` string COMMENT '工作电话',
                                                  `express_type` string COMMENT '快递员类型（收货；发货）',
                                                  `express_type_name` string COMMENT '快递员类型名称'
) comment '快递员维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_express_courier_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table dim_express_courier_full
    partition (dt = '20250713')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             md5(working_phone) working_phone,
             express_type
      from tms.ods_express_courier
      where dt = '20250713'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from tms.ods_base_organ
    where dt = '20250713'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from tms.ods_base_dic
    where dt = '20250713'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;

select * from dim_express_courier_full;


drop table if exists dim_shift_full;
create external table dim_shift_full(
                                        `id` bigint COMMENT '班次ID',
                                        `line_id` bigint COMMENT '线路ID',
                                        `line_name` string COMMENT '线路名称',
                                        `line_no` string COMMENT '线路编号',
                                        `line_level` string COMMENT '线路级别',
                                        `org_id` bigint COMMENT '所属机构',
                                        `transport_line_type_id` string COMMENT '线路类型ID',
                                        `transport_line_type_name` string COMMENT '线路类型名称',
                                        `start_org_id` bigint COMMENT '起始机构ID',
                                        `start_org_name` string COMMENT '起始机构名称',
                                        `end_org_id` bigint COMMENT '目标机构ID',
                                        `end_org_name` string COMMENT '目标机构名称',
                                        `pair_line_id` bigint COMMENT '配对线路ID',
                                        `distance` decimal(10,2) COMMENT '直线距离',
                                        `cost` decimal(10,2) COMMENT '公路里程',
                                        `estimated_time` bigint COMMENT '预计时间（分钟）',
                                        `start_time` string COMMENT '班次开始时间',
                                        `driver1_emp_id` bigint COMMENT '第一司机',
                                        `driver2_emp_id` bigint COMMENT '第二司机',
                                        `truck_id` bigint COMMENT '卡车ID',
                                        `pair_shift_id` bigint COMMENT '配对班次(同一辆车一去一回的另一班次)'
) comment '班次维度表'
    partitioned by (`dt` string comment '统计周期')
    stored as orc
    location '/warehouse/tms/dim/dim_shift_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table dim_shift_full
    partition (dt = '20250713')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from tms.ods_line_base_shift
      where dt = '20250713'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from tms.ods_line_base_info
      where dt = '20250713'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from tms.ods_base_dic
    where dt = '20250713'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;

select * from dim_shift_full;


drop table if exists dim_truck_driver_full;
create external table dim_truck_driver_full(
                                               `id` bigint COMMENT '司机信息ID',
                                               `emp_id` bigint COMMENT '员工ID',
                                               `org_id` bigint COMMENT '所属机构ID',
                                               `org_name` string COMMENT '所属机构名称',
                                               `team_id` bigint COMMENT '所属车队ID',
                                               `tream_name` string COMMENT '所属车队名称',
                                               `license_type` string COMMENT '准驾车型',
                                               `init_license_date` string COMMENT '初次领证日期',
                                               `expire_date` string COMMENT '有效截止日期',
                                               `license_no` string COMMENT '驾驶证号',
                                               `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '司机维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_driver_full'
    tblproperties('orc.compress'='snappy');

SET hive.exec.mode.local.auto=true;

insert overwrite table dim_truck_driver_full
    partition (dt = '20250713')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from tms.ods_truck_driver
      where dt = '20250713'
        and is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from tms.ods_base_organ
    where dt = '20250713'
      and is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from tms.ods_truck_team
    where dt = '20250713'
      and is_deleted = '0'
) team_info
              on driver_info.team_id = team_info.id;



drop table if exists dim_truck_full;
create external table dim_truck_full(
                                        `id` bigint COMMENT '卡车ID',
                                        `team_id` bigint COMMENT '所属车队ID',
                                        `team_name` string COMMENT '所属车队名称',
                                        `team_no` string COMMENT '车队编号',
                                        `org_id` bigint COMMENT '所属机构',
                                        `org_name` string COMMENT '所属机构名称',
                                        `manager_emp_id` bigint COMMENT '负责人',
                                        `truck_no` string COMMENT '车牌号码',
                                        `truck_model_id` string COMMENT '型号',
                                        `truck_model_name` string COMMENT '型号名称',
                                        `truck_model_type` string COMMENT '型号类型',
                                        `truck_model_type_name` string COMMENT '型号类型名称',
                                        `truck_model_no` string COMMENT '型号编码',
                                        `truck_brand` string COMMENT '品牌',
                                        `truck_brand_name` string COMMENT '品牌名称',
                                        `truck_weight` decimal(16,2) COMMENT '整车重量（吨）',
                                        `load_weight` decimal(16,2) COMMENT '额定载重（吨）',
                                        `total_weight` decimal(16,2) COMMENT '总质量（吨）',
                                        `eev` string COMMENT '排放标准',
                                        `boxcar_len` decimal(16,2) COMMENT '货箱长（m）',
                                        `boxcar_wd` decimal(16,2) COMMENT '货箱宽（m）',
                                        `boxcar_hg` decimal(16,2) COMMENT '货箱高（m）',
                                        `max_speed` bigint COMMENT '最高时速（千米/时）',
                                        `oil_vol` bigint COMMENT '油箱容积（升）',
                                        `device_gps_id` string COMMENT 'GPS设备ID',
                                        `engine_no` string COMMENT '发动机编码',
                                        `license_registration_date` string COMMENT '注册时间',
                                        `license_last_check_date` string COMMENT '最后年检日期',
                                        `license_expire_date` string COMMENT '失效日期',
                                        `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '卡车维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_full'
    tblproperties('orc.compress'='snappy');



insert overwrite table dim_truck_full
    partition (dt = '20250713')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from tms.ods_truck_info
      where dt = '20250713'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,

             manager_emp_id
      from tms.ods_truck_team
      where dt = '20250713'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,

             model_no,
             brand,

             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from tms.ods_truck_model
      where dt = '20250713'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from tms.ods_base_organ
      where dt = '20250713'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from tms.ods_base_dic
      where dt = '20250713'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from tms.ods_base_dic
      where dt = '20250713'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;


drop table if exists dim_user_zip;
create external table dim_user_zip(
                                      `id` bigint COMMENT '用户地址信息ID',
                                      `login_name` string COMMENT '用户名称',
                                      `nick_name` string COMMENT '用户昵称',
                                      `passwd` string COMMENT '用户密码',
                                      `real_name` string COMMENT '用户姓名',
                                      `phone_num` string COMMENT '手机号',
                                      `email` string COMMENT '邮箱',
                                      `user_level` string COMMENT '用户级别',
                                      `birthday` string COMMENT '用户生日',
                                      `gender` string COMMENT '性别 M男,F女',
                                      `start_date` string COMMENT '起始日期',
                                      `end_date` string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_user_zip
    partition (dt = '20250713')
select id,
       login_name,
       nick_name,
       md5(passwd)                                                                                    passwd,
       md5(real_name)                                                                                 realname,
       md5(if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone_num, null))                                                                       phone_num,
       md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
       user_level,
       date_add('1970-01-01', cast(birthday as int))                                                  birthday,
       gender,
       date_format(from_utc_timestamp(
                           cast(create_time as bigint), 'UTC'),
                   'yyyy-MM-dd')                                                                            start_date,
       '9999-12-31'                                                                                         end_date
from tms.ods_user_info
where dt = '20250713'
  and is_deleted = '0';

select * from dim_user_zip;

drop table if exists dim_user_address_zip;
create external table dim_user_address_zip(
                                              `id` bigint COMMENT '地址ID',
                                              `user_id` bigint COMMENT '用户ID',
                                              `phone` string COMMENT '电话号',
                                              `province_id` bigint COMMENT '所属省份ID',
                                              `city_id` bigint COMMENT '所属城市ID',
                                              `district_id` bigint COMMENT '所属区县ID',
                                              `complex_id` bigint COMMENT '所属小区ID',
                                              `address` string COMMENT '详细地址',
                                              `is_default` tinyint COMMENT '是否默认',
                                              `start_date` string COMMENT '起始日期',
                                              `end_date` string COMMENT '结束日期'
) comment '用户地址拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_address_zip'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_user_address_zip
    partition (dt = '9999-12-31')
select id,
       user_id,
       md5(if(phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone, null))               phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       concat(substr(create_time, 1, 10), ' ',
              substr(create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from tms.ods_user_address
where dt = '20250713'
  and is_deleted = '0';

select * from dim_user_address_zip;