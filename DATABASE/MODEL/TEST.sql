with NORM_OLD as (select distinct VERSION, UUID, NAME, BACKGROUND, UNIT
                  from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                  where VERSION like 'NORM%'
                    and FLAG = 'MAT')
select *
from (select distinct *
      from T_ADS_DIM_LCA_UNIT_PROCESS
      where PCR = 'BASIC') A
         FULL OUTER JOIN (select * from NORM_OLD) B on a.UNIT_PROCESS_NAME = B.NAME;


select *
from (select *
      from T_ADS_DIM_LCA_UNIT_PROCESS) UP
         join (select * from T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT) UPI on UP.UNIT_PROCESS_ID = UPI.UNIT_PROCESS_ID
         join (select *
               from T_ADS_DIM_LCA_PCR_INDICATOR
               where INDICATOR_CODE = 'GWP-total') PI
              on UPI.IMPACT_INDICATOR_ID = PI.REFERENCE_IMPACT_INDICATOR_ID
;


select *
from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
where VERSION like 'NORM%'
  and FLAG = 'MAT'
  and NAME = '镍铬铁'
  and LCI_ELEMENT_CODE = 'GWP-total';

SELECT *
FROM BG00MAC102.V_ADS_LCA_UNIT_PROCESS_IMPACT
ORDER BY PCR, SOURCE, CATEGORY, UNIT_PROCESS_NAME, INDICATOR_CODE;

select * from BG00MAC102.V_ADS_LCA_UNIT_PROCESS_IMPACT;

with DATA AS (select DISTINCT LCA_DATA_ITEM_CAT_CODE AS ITEM_CAT_CODE,
                              LCA_DATA_ITEM_CAT_NAME AS ITEM_CAT_NAME,
                              LCA_DATA_ITEM_CODE     AS ITEM_CODE,
                              LCA_DATA_ITEM_NAME     AS ITEM_NAME
              from T_ADS_FACT_LCA_PROC_DATA
              where COMPANY_CODE = 'ZG'
                and BATCH_NUMBER = '20250120251220260226YS'),
     BR AS (SELECT *
            FROM T_ADS_WH_LCA_ITEM_CONTRAST
            WHERE COMPANY_CODE = 'ZG'
              AND VERSION = 'NORM_Ecoinvent3.11'
              AND FLAG = 'MAT'),
     FACTOR AS (SELECT *
                FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                WHERE VERSION = 'NORM_Ecoinvent3.11'
                  AND LCI_ELEMENT_CODE = 'GWP-total'
                  AND FLAG = 'MAT')
SELECT BR.COMPANY_CODE,
       BR.VERSION,
       DATA.ITEM_CAT_CODE,
       DATA.ITEM_CAT_NAME,
       DATA.ITEM_CODE,
       DATA.ITEM_NAME,
       NAME,
       BACKGROUND,
       UNIT,
       LCI_ELEMENT_CODE,
       LCI_ELEMENT_CNAME,
       LCI_ELEMENT_VALUE
FROM DATA
         JOIN BR ON DATA.ITEM_CODE = BR.ITEM_CODE
         JOIN FACTOR ON BR.UUID = FACTOR.UUID
ORDER BY ITEM_CAT_CODE, ITEM_CODE;



