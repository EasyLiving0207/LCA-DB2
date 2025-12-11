select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120240120251202YS'
order by LCA_PROC_CODE, PRODUCT_CODE, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CODE;


select *
from T_ADS_FACT_LCA_PROC_DATA_CONS
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120240120251202YS_CONS'
  and (LCA_PROC_CODE like 'CO%' or LCA_PROC_CODE like 'LT%' or LCA_PROC_CODE like 'LG%' or LCA_PROC_CODE = 'XABC')
order by LCA_PROC_CODE, PRODUCT_CODE, LCA_DATA_ITEM_CAT_CODE
;


select LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, NAME, LCI_ELEMENT_CNAME, LCI_ELEMENT_VALUE
from (select distinct LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
      from T_ADS_FACT_LCA_PROC_DATA_0002_DT
      where COMPANY_CODE = 'ZG'
        and BATCH_NUMBER = '20240120240120251202YS') A
         join (select distinct UUID, DATA_CODE
               from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
               where BASE_CODE = 'ZG'
                 and START_TIME = '2025') B on A.LCA_DATA_ITEM_CODE = B.DATA_CODE
         join (select *
               from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
               where VERSION = 'EN15804_Ecoinvent3.11'
                 and LCI_ELEMENT_CODE = 'GWP-total') C
              on B.UUID = C.UUID;


select LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, DISCH_COEFF, DISCH_COEFF_UNIT, HOTVALUE, HOTVALUE_UNIT
from (select distinct LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
      from T_ADS_FACT_LCA_PROC_DATA_0002_DT
      where COMPANY_CODE = 'ZG'
        and BATCH_NUMBER = '20240120240120251202YS') A
         join (select *
               from T_ADS_WH_LCA_MAT_DATA
               where START_TIME = '2025'
                 and ORG_CODE = 'ZG') B
              on A.LCA_DATA_ITEM_CODE = B.ITEM_CODE
order by LCA_DATA_ITEM_CODE;

WITH RAW AS (SELECT DISTINCT LCA_DATA_ITEM_CODE          AS ITEM_CODE,
                             LCA_DATA_ITEM_NAME          AS ITEM_NAME,
                             BIGCLASS_NAME,
                             TRANS_MTHD_CODE,
                             CASE
                                 WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                                 ELSE TRANS_DISTANCE END AS TRANS_DISTANCE,
                             CASE
                                 WHEN UNIT = 'nm' THEN 'km'
                                 ELSE UNIT END           AS UNIT
             FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
             WHERE ORG_CODE = 'ZG'
               AND YEAR = '2024'
               AND LCA_DATA_ITEM_CODE IS NOT NULL),
     TRANS_DATA AS (SELECT A.*
                    FROM (SELECT LCA_DATA_ITEM_CODE    AS ITEM_CODE,
                                 LCA_DATA_ITEM_NAME    AS ITEM_NAME,
                                 CASE
                                     WHEN CLASS_CODE IS NOT NULL THEN CLASS_CODE
                                     WHEN CLASS_CODE IS NULL AND LCA_DATA_ITEM_CODE LIKE '125%' THEN 'THJ001'
                                     ELSE 'FYL001' END AS CLASS_CODE
                          FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                          WHERE COMPANY_CODE = 'ZG'
                            AND START_TIME = '2025') A
                             JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_ITEM_BASE B
                                  ON A.ITEM_CODE = B.ITEM_CODE),
     OTHER AS (SELECT *
               FROM TRANS_DATA
               WHERE ITEM_CODE NOT IN (SELECT ITEM_CODE FROM RAW)),
     TRANS_AVG AS (SELECT CASE
                              WHEN BIGCLASS_NAME = '铁矿石' THEN 'TKS001'
                              WHEN BIGCLASS_NAME = '废钢' THEN 'FG001'
                              WHEN BIGCLASS_NAME = '铁合金' THEN 'THJ001'
                              ELSE 'FYL001' END       AS CLASS_CODE,
                          TRANS_MTHD_CODE,
                          CASE
                              WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                              ELSE TRANS_DISTANCE END AS TRANS_DISTANCE,
                          CASE
                              WHEN UNIT = 'nm' THEN 'km'
                              ELSE UNIT END           AS UNIT
                   FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION
                   WHERE ORG_CODE = 'ZG'
                     AND YEAR = '2024'
                     AND BIGCLASS_NAME IN ('副原料', '铁矿石', '铁合金', '废钢')),
     TRANS_OTHER AS (SELECT ITEM_CODE,
                            ITEM_NAME,
                            A.CLASS_CODE,
                            TRANS_MTHD_CODE,
                            TRANS_DISTANCE,
                            UNIT
                     FROM OTHER A
                              JOIN TRANS_AVG B ON A.CLASS_CODE = B.CLASS_CODE),
     TRANS AS (SELECT ITEM_CODE,
                      ITEM_NAME,
                      CASE
                          WHEN TRANS_MTHD_CODE = 'carTrans' THEN '汽运'
                          WHEN TRANS_MTHD_CODE = 'riversTrans' THEN '河运'
                          WHEN TRANS_MTHD_CODE = 'seaTrans' THEN '海运'
                          WHEN TRANS_MTHD_CODE = 'railwayTrans' THEN '铁运'
                          ELSE TRANS_MTHD_CODE END AS DATA_CODE,
                      TRANS_DISTANCE,
                      UNIT
               FROM (SELECT *
                     FROM RAW
                     UNION
                     SELECT *
                     FROM TRANS_OTHER)),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                           WHERE VERSION = 'EN15804_Ecoinvent3.11'
                             AND LCI_ELEMENT_CODE IN (SELECT LCI_ELEMENT_CODE
                                                      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_LCI_LIST)
                             AND NAME IN ('海运', '河运', '铁运', '汽运', '柴油'))
select *
from TRANS
order by ITEM_CODE, DATA_CODE;

select batch_number,
       start_ym,
       end_ym,
       company_code        as 基地,
       proc_key,
       proc_code           as 工序代码,
       proc_name           as 工序名称,
       product_code        as 产品代码,
       product_name        as 产品名称,
       A.lci_element_code  as 环境影响指标,
       B.LCI_ELEMENT_CNAME as 环境影响指标名称,
       c_cycle             as 生命周期负荷,
       c1_direct           as 直接负荷,
       c2_bp               as 副产品负荷,
       c3_out              as 外购物料负荷,
       c4_bp_neg           as 回用抵扣负荷,
       c5_trans            as 运输负荷,
       c_insite            as 厂内负荷,
       c_outsite           as 厂外负荷
from (select *
      from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
      where COMPANY_CODE = 'ZG'
        and BATCH_NUMBER = '20240120240120251202YS_CONS') A
         join (select *
               from T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION
               where VERSION = 'EN15804_Ecoinvent3.11'
                 and LCI_ELEMENT_CODE is not null) B on A.LCI_ELEMENT_CODE = B.LCI_ELEMENT_CODE
order by PROC_KEY, A.LCI_ELEMENT_CODE;


select update_date,
       mat_no,
       mat_track_no,
       mat_seq_no,
       case
           when FAMILY_CODE is null then '00'
           else FAMILY_CODE end as FAMILY_CODE,
       unit_code,
       unit_name,
       type_code,
       type_name,
       unitm_ac,
       item_code,
       item_name,
       value,
       mat_wt,
       mat_status
from T_ADS_FACT_LCA_BSZG_HP0001_2024_TEST
order by FAMILY_CODE, MAT_SEQ_NO, TYPE_CODE;


CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_EPD_CONS_CALC_AUTH(
        'ZG',--基地代码
        '2025',--因子年份 默认2025
        'EN15804_Ecoinvent3.11',--因子库版本 默认EN15804_Ecoinvent3.11
        'T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT',--大类结果表
        '20240120240120251202YS_CONS',--大类结果批次号（批次号 + _CONS）
        'T_ADS_FACT_LCA_BSZG_HP0001_2024_TEST',--活动数据表
        'T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_BSZG_HP0001_AUTH',
        'T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_BSZG_HP0001_AUTH_DIST'
     );


select company_code,
       main_cat_batch_number,
       update_date,
       mat_no,
       mat_track_no,
       mat_seq_no,
       family_code,
       unit_code,
       unit_name,
       product_code,
       product_name,
       product_value,
       lci_element_code,
       lci_element_cname,
       c_cycle   as 生命周期负荷,
       c1        as 直接负荷,
       c2        as 副产品负荷,
       c3        as 外购物料负荷,
       c4        as 回用抵扣负荷,
       c5        as 运输负荷,
       c_insite  as 厂内负荷,
       c_outsite as 厂外负荷
from T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_BSZG_HP0001_AUTH
where MAIN_CAT_BATCH_NUMBER = '20240120240120251202YS_CONS'
order by LCI_ELEMENT_CODE, FAMILY_CODE, MAT_SEQ_NO;


select update_date,
       mat_no,
       mat_track_no,
       mat_seq_no,
       family_code,
       unit_code,
       unit_name,
       product_code,
       product_name,
       product_value,
       type_code,
       type_name,
       item_code,
       item_name,
       value,
       unitm_ac,
       unit_cost,
       lci_element_code,
       c1 + c2 + c3 + c4 + c5 as 生命周期负荷,
       c1                     as 直接负荷,
       c2                     as 副产品负荷,
       c3                     as 外购物料负荷,
       c4                     as 回用抵扣负荷,
       c5                     as 运输负荷
from T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_BSZG_HP0001_AUTH_DIST
where LCI_ELEMENT_CODE = 'GWP-total'
order by LCI_ELEMENT_CODE, FAMILY_CODE, MAT_SEQ_NO, TYPE_CODE;

