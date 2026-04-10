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
                   LOAD as IMPACT
            from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
            where COMPANY_CODE = 'TA'
              and BATCH_NUMBER = '20250120251220260209YS'
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
                     LOAD as IMPACT
              from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
              where COMPANY_CODE = 'TA'
                and BATCH_NUMBER = '20250120251220260209YS'
                and FACTOR_VERSION = 'NORM_Ecoinvent3.11'),
     FACTOR_YT AS (select item.ITEM_CODE, LIB.NAME, lib.LCI_ELEMENT_VALUE
                   from (select *
                         from T_ADS_WH_LCA_ITEM_CONTRAST
                         where COMPANY_CODE = 'TA'
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
                           where COMPANY_CODE = 'TA'
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
     DIFF AS (select NORM.SOURCE_PROC_KEY,
                     NORM.SOURCE_PROC_CODE,
                     NORM.SOURCE_PROC_NAME,
                     NORM.TARGET_PROC_KEY,
                     NORM.TARGET_PROC_CODE,
                     NORM.TARGET_PROC_NAME,
                     NORM.PRODUCT_CODE,
                     NORM.PRODUCT_NAME,
                     NORM.ITEM_CAT_CODE,
                     NORM.ITEM_CAT_NAME,
                     NORM.ITEM_CODE,
                     NORM.ITEM_NAME,
                     NORM.LCI_ELEMENT_CODE,
                     NORM.TYPE,
                     NORM.IMPACT                                                               AS IMPACT,
                     SUM(NORM.IMPACT) OVER (PARTITION BY NORM.ITEM_CODE, NORM.TARGET_PROC_KEY) AS ITEM_IMPACT,
                     YT.IMPACT                                                                 as IMPACT_YT,
                     SUM(YT.IMPACT) OVER (PARTITION BY YT.ITEM_CODE, YT.TARGET_PROC_KEY)       AS ITEM_IMPACT_YT,
                     YT.IMPACT - NORM.IMPACT                                                   AS DIFF,
--        case when YT.IMPACT = 0 then 0 else (YT.IMPACT - NORM.IMPACT) / YT.IMPACT end as PERCENTAGE,
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
              order by YT.IMPACT - NORM.IMPACT desc),
     DIFF_ITEM AS (select DISTINCT TARGET_PROC_KEY,
                                   TARGET_PROC_CODE,
                                   TARGET_PROC_NAME,
                                   PRODUCT_CODE,
                                   PRODUCT_NAME,
                                   ITEM_CAT_CODE,
                                   ITEM_CAT_NAME,
                                   ITEM_CODE,
                                   ITEM_NAME,
                                   LCI_ELEMENT_CODE,
                                   ITEM_IMPACT,
                                   ITEM_IMPACT_YT,
                                   ITEM_IMPACT_YT - ITEM_IMPACT AS DIFF,
                                   NAME_NORM,
                                   FACTOR_NORM,
                                   NAME_YT,
                                   FACTOR_YT
                   from DIFF)
SELECT *
FROM DIFF_ITEM
WHERE ABS(DIFF) > 0.001
order by TARGET_PROC_KEY, DIFF DESC;


with YT as (select proc_key,
                   proc_code,
                   proc_name,
                   product_code,
                   product_name,
                   lci_element_code,
                   C_CYCLE
            from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
            where COMPANY_CODE = 'TA'
              and BATCH_NUMBER = '20250120251220260209YS'
              and FACTOR_VERSION = '易碳_Ecoinvent3.8'
              and lci_element_code = 'GWP-total'),
     NORM as (select proc_key,
                     proc_code,
                     proc_name,
                     product_code,
                     product_name,
                     lci_element_code,
                     C_CYCLE
              from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
              where COMPANY_CODE = 'TA'
                and BATCH_NUMBER = '20250120251220260209YS'
                and FACTOR_VERSION = 'NORM_Ecoinvent3.11'
                and lci_element_code = 'GWP-total')
select NORM.proc_key,
       NORM.proc_code,
       NORM.proc_name,
       NORM.product_code,
       NORM.product_name,
       NORM.lci_element_code,
       NORM.c_cycle              as IMPACT,
       YT.C_CYCLE                as IMPACT_YT,
       YT.C_CYCLE - NORM.c_cycle as DIFF
from NORM
         join YT on NORM.PROC_KEY = YT.PROC_KEY
where NORM.PRODUCT_NAME in
      ('烧结矿', '焦炭-合并', '电力', '生铁-铁水-大院', '连铸板坯-二炼钢', '热轧中间坯-1580', '冷轧硬卷-1730')
order by IMPACT;

select distinct *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20250120251220260209YS';

