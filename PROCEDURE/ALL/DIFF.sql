WITH ZG25 AS (SELECT *
              FROM T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
              WHERE COMPANY_CODE = 'ZG'
                AND BATCH_NUMBER = '20250120251220260107YS'
                AND FACTOR_VERSION = 'NORM_Ecoinvent3.11'
                and LCI_ELEMENT_CODE = 'GWP-total'),
     ZG24 AS (SELECT *
              FROM T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
              WHERE COMPANY_CODE = 'ZG'
                AND BATCH_NUMBER = '20240120241220250915YS'
                AND FACTOR_VERSION = 'NORM_Ecoinvent3.11'
                and LCI_ELEMENT_CODE = 'GWP-total')
select ZG24.PROC_KEY,
       ZG24.PROC_NAME,
       ZG24.PRODUCT_NAME,
       ZG24.C_CYCLE                    AS C_24,
       ZG25.C_CYCLE                    AS C_25,
       ZG25.C_CYCLE - ZG24.C_CYCLE     AS DIFF,
       ZG25.C1_DIRECT - ZG24.C1_DIRECT AS C1_DIFF,
       ZG25.C2_BP - ZG24.C2_BP         AS C2_DIFF,
       ZG25.C3_OUT - ZG24.C3_OUT       AS C3_DIFF,
       ZG25.C4_BP_NEG - ZG24.C4_BP_NEG AS C4_DIFF,
       ZG25.C5_TRANS - ZG24.C5_TRANS   AS C5_DIFF
from ZG24
         join ZG25 on ZG24.PROC_KEY = ZG25.PROC_KEY
ORDER BY ZG25.C_CYCLE - ZG24.C_CYCLE DESC;

WITH T0 AS (SELECT MAT_TRACK_NO,
                   MAT_NO,
                   PRODUCT_CODE,
                   FAMILY_CODE,
                   LCI_ELEMENT_CNAME                                                                                                AS LCI_ELEMENT_NAME,
                   C1                                                                                                               AS G1,
                   C2                                                                                                               AS G2,
                   C3                                                                                                               AS G3,
                   C4                                                                                                               AS G4,
                   C_INSITE                                                                                                         AS G_INSITE,
                   C5                                                                                                               AS G5,
                   NULL                                                                                                             AS G6,
                   NULL                                                                                                             AS G7,
                   C_OUTSITE                                                                                                        AS G_OUTSITE,
                   C_CYCLE                                                                                                          AS G_CYCLE,
                   NULL                                                                                                             AS CREATE_TIME
                    ,
                   ROW_NUMBER() OVER (PARTITION BY LCI_ELEMENT_CNAME,MAT_TRACK_NO,MAT_NO,FAMILY_CODE ORDER BY REC_CREATE_TIME DESC) AS RN
            FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_BGMG_CR0001_2025_NEW
            WHERE 1 = 1
              AND MAT_STATUS = '27'),
     T1 AS (SELECT DISTINCT B.FIN_ORDER_NO,
                            B.ST_NO,
                            B.MF_PRODUCE_END_TIME AS PRODUCE_END_TIME,
                            NULL                  AS WHOLE_BACKLOG_ACT,
                            B.MAT_WT,
                            B.SHARE_CAPC          AS SHARE_CAPC,
                            B.OUTPUT_BYPROD_CODE  AS OUTPUT_BYPROD_CODE,
                            B.OUTPUT_UNIT_CODE    AS OUTPUT_UNIT_CODE,
                            B.MAT_ACT_THICK,
                            B.MAT_ACT_WIDTH,
                            A.MAT_TRACK_NO,
                            A.MAT_NO,
                            A.PRODUCT_CODE,
                            A.FAMILY_CODE,
                            A.LCI_ELEMENT_NAME,
                            A.G1,
                            A.G2,
                            A.G3,
                            A.G4,
                            A.G_INSITE,
                            A.G5,
                            A.G6,
                            A.G7,
                            A.G_OUTSITE,
                            A.G_CYCLE,
                            A.CREATE_TIME
            FROM T0 A
                     JOIN BGTAMSZZ00.T_DWD_FACT_ZZCH_MM_MGMBCR01 B
                          ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                              AND A.MAT_NO = B.MAT_NO
                              AND A.FAMILY_CODE = B.FAMILY_CODE
                              AND B.MAT_STATUS = '27'
            WHERE A.RN = 1),
     T2 AS (SELECT HEX(RAND()) AS REC_ID,
                   A.*,
                   B.TYPE_LAR_CODE,      --大类品种代码
                   B.TYPE_LAR,           --大类品种名称
                   B.SALE_PROD_CODE,     --销售品种代码
                   B.SALE_PROD_CNAME,    --销售品种名称
                   B.KEY_PRODUCT_CODE_1, --重点品种代码1
                   B.KEY_PRODUCT_DESC_1, --重点品种名称1
                   B.KEY_PRODUCT_CODE_2, --重点品种代码2
                   B.KEY_PRODUCT_DESC_2, --重点品种名称2
                   B.KEY_PRODUCT_CODE_3, --重点品种代码3
                   B.KEY_PRODUCT_DESC_3, --重点品种名称3
                   B.KEY_PRODUCT_CODE_4, --重点品种代码4
                   B.KEY_PRODUCT_DESC_4, --重点品种名称4
                   B.SIGN_CODE,
                   B.FIN_CUST_CODE,
                   B.TYPE_MID,
                   B.TYPE_MID_CODE,
                   B.APN,
                   B.PSR,
                   B.SG_SIGN
            FROM T1 A
                     LEFT JOIN M1_WE.SU_WE00_MAHT01 B
                               ON A.FIN_ORDER_NO = B.ORDER_NO
                                   AND B.ACCOUNT = '1011')
SELECT TYPE_LAR,
       TYPE_LAR_CODE,
       LCI_ELEMENT_NAME,
       SUM(MAT_WT)                      AS MAT_WT,
       CAST(ROUND(SUM(G3 * MAT_WT + G4 * MAT_WT + G5 * MAT_WT) / DECODE(SUM(MAT_WT), 0, NULL, SUM(MAT_WT)),
                  4) AS DECIMAL(18, 4)) AS SY,
       CAST(ROUND(SUM(G1 * MAT_WT + G2 * MAT_WT) / DECODE(SUM(MAT_WT), 0, NULL, SUM(MAT_WT)),
                  4) AS DECIMAL(18, 4)) AS SCZZ,
       CAST(ROUND(SUM(G5 * MAT_WT) / DECODE(SUM(MAT_WT), 0, NULL, SUM(MAT_WT)),
                  4) AS DECIMAL(18, 4)) AS YS,
       CAST(ROUND(SUM(G_CYCLE * MAT_WT) / DECODE(SUM(MAT_WT), 0, NULL, SUM(MAT_WT)),
                  4) AS DECIMAL(18, 4)) AS HJ,
       CASE
           WHEN LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):化石能源' THEN 1
           WHEN LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):生物质' THEN 2
           WHEN LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):土地利用和土地用途改变' THEN 3
           WHEN LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计' THEN 4
           END                          AS SORT_INDEX
FROM T2
WHERE 1 = 1
  AND LEFT(PRODUCE_END_TIME, 4) = '2025'
GROUP BY TYPE_LAR, TYPE_LAR_CODE, LCI_ELEMENT_NAME;

-- 20250327222948493776

select ITEM_CODE, ITEM_NAME, AVG(UNIT_COST) AS UNIT_COST_AVG, AVG(C_CYCLE) AS C_CYCLE_AVG
from (select *
      from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_CR0001_2025_NEW_DIST
      union
      select *
      from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_HP0001_2025_NEW_DIST
      union
      select *
      from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_SI0001_2025_NEW_DIST)
where ITEM_CODE not in
      (select distinct PRODUCT_CODE
       from (select *
             from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_CR0001_2025_NEW_DIST
             union
             select *
             from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_HP0001_2025_NEW_DIST
             union
             select *
             from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_SI0001_2025_NEW_DIST))
  and ITEM_CODE <> '70202'
GROUP BY ITEM_CODE, ITEM_NAME
order by AVG(C_CYCLE) DESC;


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where FLAG = 'DIRECT';

with zg24 as (select *, sum(LOAD) OVER (partition by SOURCE_PROC_KEY) as LOAD_SUM
              from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
              where COMPANY_CODE = 'ZG'
                and BATCH_NUMBER = '20240120241220250915YS'
                and TYPE = 'C1'
                and PRODUCT_NAME = '热镀锌卷-D170-热基'),
     zg25 as (select *, sum(LOAD) OVER (partition by SOURCE_PROC_KEY) as LOAD_SUM
              from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
              where COMPANY_CODE = 'ZG'
                and BATCH_NUMBER = '20250120251220260126YS'
                and TYPE = 'C1'
                and PRODUCT_NAME = '热镀锌卷-D170-热基')
select COALESCE(zg25.SOURCE_PROC_KEY, zg24.SOURCE_PROC_KEY)   as SOURCE_PROC_KEY,
       COALESCE(zg25.SOURCE_PROC_CODE, zg24.SOURCE_PROC_CODE) as SOURCE_PROC_CODE,
       COALESCE(zg25.SOURCE_PROC_NAME, zg24.SOURCE_PROC_NAME) as SOURCE_PROC_NAME,
       COALESCE(zg25.TARGET_PROC_KEY, zg24.TARGET_PROC_KEY)   as TARGET_PROC_KEY,
       COALESCE(zg25.TARGET_PROC_CODE, zg24.TARGET_PROC_CODE) as TARGET_PROC_CODE,
       COALESCE(zg25.TARGET_PROC_NAME, zg24.TARGET_PROC_NAME) as TARGET_PROC_NAME,
       COALESCE(zg25.PRODUCT_CODE, zg24.PRODUCT_CODE)         as PRODUCT_CODE,
       COALESCE(zg25.PRODUCT_NAME, zg24.PRODUCT_NAME)         as PRODUCT_NAME,
       COALESCE(zg25.ITEM_CAT_CODE, zg24.ITEM_CAT_CODE)       as ITEM_CAT_CODE,
       COALESCE(zg25.ITEM_CAT_NAME, zg24.ITEM_CAT_NAME)       as ITEM_CAT_NAME,
       COALESCE(zg25.ITEM_CODE, zg24.ITEM_CODE)               as ITEM_CODE,
       COALESCE(zg25.ITEM_NAME, zg24.ITEM_NAME)               as ITEM_NAME,
       COALESCE(zg25.LCI_ELEMENT_CODE, zg24.LCI_ELEMENT_CODE) as LCI_ELEMENT_CODE,
       COALESCE(zg25.TYPE, zg24.TYPE)                         as TYPE,
       zg25.LOAD                                              as LOAD_24,
       zg25.LOAD                                              as LOAD_25,
       zg25.LOAD - zg24.LOAD                                  as LOAD_DIFF,
       zg24.LOAD_SUM                                          as LOAD_SUM_24,
       zg25.LOAD_SUM                                          as LOAD_SUM_25,
       zg25.LOAD_SUM - zg24.LOAD_SUM                          as LOAD_SUM_DIFF
from zg24
         full outer join zg25 on zg24.TARGET_PROC_KEY = zg25.TARGET_PROC_KEY
    and zg24.SOURCE_PROC_KEY = zg25.SOURCE_PROC_KEY
    and zg24.ITEM_CAT_CODE = zg25.ITEM_CAT_CODE
    and zg24.ITEM_CODE = zg25.ITEM_CODE;



with YT as (select source_proc_key,
                   source_proc_code,
                   source_proc_name,
                   target_proc_key,
                   target_proc_code,
                   target_proc_name,
                   product_code,
                   product_name,
                   item_cat_code,
                   item_cat_name,
                   item_code,
                   item_name,
                   lci_element_code,
                   type,
                   load
            from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
            where COMPANY_CODE = 'WG'
              and BATCH_NUMBER = '20250120251220260107YS'
              and FACTOR_VERSION = '易碳_Ecoinvent3.8'),
     NORM as (select source_proc_key,
                     source_proc_code,
                     source_proc_name,
                     target_proc_key,
                     target_proc_code,
                     target_proc_name,
                     product_code,
                     product_name,
                     item_cat_code,
                     item_cat_name,
                     item_code,
                     item_name,
                     lci_element_code,
                     type,
                     load
              from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
              where COMPANY_CODE = 'WG'
                and BATCH_NUMBER = '20250120251220260107YS'
                and FACTOR_VERSION = 'NORM_Ecoinvent3.11'),
     FACTOR_YT AS (select item.ITEM_CODE, LIB.NAME, lib.LCI_ELEMENT_VALUE
                   from (select *
                         from T_ADS_WH_LCA_ITEM_CONTRAST
                         where COMPANY_CODE = 'WG'
                           and FLAG = 'MAT'
                           and VERSION = '易碳_Ecoinvent3.8') item
                            join
                        (select *
                         from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                         where FLAG = 'MAT'
                           and VERSION = '易碳_Ecoinvent3.8'
                           and LCI_ELEMENT_CODE = 'GWP-total') lib
                        on item.UUID = lib.UUID),
     FACTOR_NORM AS (select item.ITEM_CODE, LIB.NAME, lib.LCI_ELEMENT_VALUE
                     from (select *
                           from T_ADS_WH_LCA_ITEM_CONTRAST
                           where COMPANY_CODE = 'WG'
                             and FLAG = 'MAT'
                             and VERSION = 'NORM_Ecoinvent3.11') item
                              join
                          (select *
                           from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                           where FLAG = 'MAT'
                             and VERSION = 'NORM_Ecoinvent3.11'
                             and LCI_ELEMENT_CODE = 'GWP-total') lib
                          on item.UUID = lib.UUID),
     FACTOR AS (SELECT FACTOR_NORM.ITEM_CODE,
                       FACTOR_NORM.NAME              AS NAME_NORM,
                       FACTOR_NORM.LCI_ELEMENT_VALUE AS FACTOR_NORM,
                       FACTOR_YT.NAME                AS NAME_YT,
                       FACTOR_YT.LCI_ELEMENT_VALUE   AS FACTOR_YT
                FROM FACTOR_NORM
                         JOIN FACTOR_YT
                              ON FACTOR_NORM.ITEM_CODE = FACTOR_YT.ITEM_CODE),
     DIFF AS (select NORM.*,
                     SUM(NORM.LOAD) OVER (PARTITION BY NORM.ITEM_CODE, NORM.TARGET_PROC_KEY) AS ITEM_LOAD,
                     YT.LOAD                                                                 as LOAD_YT,
                     SUM(YT.LOAD) OVER (PARTITION BY YT.ITEM_CODE, YT.TARGET_PROC_KEY)       AS ITEM_LOAD_YT,
                     YT.LOAD - NORM.LOAD                                                     AS DIFF,
--        case when YT.LOAD = 0 then 0 else (YT.LOAD - NORM.LOAD) / YT.LOAD end as PERCENTAGE,
                     FACTOR.NAME_NORM,
                     FACTOR.FACTOR_NORM,
                     FACTOR.NAME_YT,
                     FACTOR.FACTOR_YT
              from NORM
                       JOIN YT on NORM.SOURCE_PROC_KEY = YT.SOURCE_PROC_KEY
                  and NORM.TARGET_PROC_KEY = YT.TARGET_PROC_KEY
                  and NORM.ITEM_CAT_CODE = YT.ITEM_CAT_CODE
                  and NORM.ITEM_CODE = YT.ITEM_CODE
                  and NORM.TYPE = YT.TYPE
                       LEFT JOIN FACTOR ON NORM.ITEM_CODE = FACTOR.ITEM_CODE
              where NORM.PRODUCT_CODE = '70202'
              order by YT.LOAD - NORM.LOAD desc),
     ITEM AS (SELECT DISTINCT ITEM_CODE, ITEM_NAME, ITEM_LOAD, ITEM_LOAD_YT
              FROM DIFF
              ORDER BY ITEM_LOAD DESC)
SELECT *
FROM DIFF
;


-- X4070 电力
-- 50100 电-外购


select BATCH_NUMBER, TARGET_PROC_KEY, PRODUCT_NAME, SUM(LOAD) AS LOAD
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
where COMPANY_CODE = 'TM'
  and BATCH_NUMBER like '%_GREEN'
  and PRODUCT_CODE = '70202'
  and ITEM_CODE = '50110'
GROUP BY BATCH_NUMBER, TARGET_PROC_KEY, PRODUCT_NAME;


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where COMPANY_CODE = 'RG'
  and VERSION like 'NORM%';


select * from T_ADS_DIM_EMISSION_FACTOR_CATEGORY
where SOURCE_ITEM_CAT_ID = '086e0f42-e974-4b38-9996-c2782ad4d009';


select * from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA'
and FACTOR_VERSION like 'NORM%'
and BATCH_NUMBER = '20250120251220260209YS'
and LCI_ELEMENT_CODE = 'GWP-total'
order by PROC_KEY;




