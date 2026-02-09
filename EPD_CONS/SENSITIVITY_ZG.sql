select *
from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST
where LCI_ELEMENT_CODE = 'GWP-total';



WITH data as (select *
              from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_DIST
              where COMPANY_CODE = 'ZG'
                and BATCH_NUMBER in
                    ('20230920250320260121YS_CONS')),
     PRODUCT AS (SELECT DISTINCT BATCH_NUMBER, PRODUCT_CODE, LCA_DATA_ITEM_NAME, VALUE AS PRODUCT_VALUE, UNIT
                 FROM T_ADS_FACT_LCA_PROC_DATA_CONS
                 where COMPANY_CODE = 'ZG'
                   AND PRODUCT_CODE IN ('J1210', 'J0210', 'J2210')
                   AND LCA_DATA_ITEM_CAT_CODE = '04'
                   and BATCH_NUMBER in
                       ('20230920250320260121YS_CONS')),
     RESULT1 AS (SELECT BATCH_NUMBER,
                        TARGET_PROC_KEY  as PROC_KEY,
                        TARGET_PROC_NAME AS PROC_NAME,
                        PRODUCT_CODE,
                        PRODUCT_NAME,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD)        AS LOAD
                 FROM DATA
                 GROUP BY BATCH_NUMBER,
                          TARGET_PROC_KEY,
                          TARGET_PROC_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          ITEM_CODE,
                          ITEM_NAME,
                          LCI_ELEMENT_CODE),
     RESULT2 AS (SELECT *
                 FROM RESULT1
                 WHERE PROC_KEY IN ('HB01_J1210', 'P032_J0210', 'P070_J2210')),
     RESULT3 AS (SELECT A.BATCH_NUMBER,
                        PROC_KEY,
                        PROC_NAME,
                        A.PRODUCT_CODE,
                        PRODUCT_NAME,
                        PRODUCT_VALUE,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        LOAD * PRODUCT_VALUE                                                    AS EMISSION,
                        SUM(LOAD * PRODUCT_VALUE) OVER ( PARTITION BY A.BATCH_NUMBER, PROC_KEY) AS EMISSION_BATCH,
                        SUM(LOAD * PRODUCT_VALUE) OVER ( PARTITION BY PROC_KEY)                 AS EMISSION_TOTAL
                 FROM RESULT2 A
                          JOIN PRODUCT B ON A.PRODUCT_CODE = B.PRODUCT_CODE AND A.BATCH_NUMBER = B.BATCH_NUMBER),
     RESULT4 AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_CODE,
                        PRODUCT_NAME,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(EMISSION)                                  AS EMISSION,
                        EMISSION_TOTAL,
                        SUM(EMISSION) / CAST(EMISSION_TOTAL AS DOUBLE) AS SENSITIVITY
                 FROM RESULT3
                 GROUP BY PROC_KEY,
                          PROC_NAME, PRODUCT_CODE, PRODUCT_NAME, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE, EMISSION_TOTAL)
SELECT *
FROM RESULT4
ORDER BY PROC_KEY, SENSITIVITY DESC;



INSERT INTO T_ADS_FACT_LCA_PROC_DATA_0002_DT (REC_ID, BATCH_NUMBER, START_YM, END_YM, LCA_PROC_CODE, LCA_PROC_NAME,
                                              PRODUCT_CODE, PRODUCT_NAME, LCA_DATA_ITEM_CAT_CODE,
                                              LCA_DATA_ITEM_CAT_NAME, LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, VALUE,
                                              UNIT, COMPANY_CODE)
select HEX(RAND()),
       '20240120241220260122YS',
       '202401',
       '202412',
       LCA_PROC_CODE,
       LCA_PROC_NAME,
       PRODUCT_CODE,
       PRODUCT_NAME,
       LCA_DATA_ITEM_CAT_CODE,
       LCA_DATA_ITEM_CAT_NAME,
       LCA_DATA_ITEM_CODE,
       LCA_DATA_ITEM_NAME,
       SUM(VALUE) AS VALUE,
       UNIT,
       COMPANY_CODE
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'ZG'
  AND BATCH_NUMBER in ('20240120240120251202YS',
                       '20240220240220251202YS',
                       '20240320240320251202YS',
                       '20240420240420251202YS',
                       '20240520240520251202YS',
                       '20240620240620251202YS',
                       '20240720240720251202YS',
                       '20240820240820251202YS',
                       '20240920240920251202YS',
                       '20241020241020251202YS',
                       '20241120241120251202YS',
                       '20241220241220251202YS')
GROUP BY LCA_PROC_CODE, LCA_PROC_NAME, PRODUCT_CODE, PRODUCT_NAME, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
         LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE;



select distinct *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'BSZG'
  and BATCH_NUMBER = '20240120241220250814YS'
  and LCA_PROC_CODE = 'LT09';

SELECT *
FROM T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
WHERE BASE_CODE = 'ZG'
  AND START_TIME = '2025';

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where UUID = '3f185f1aec484f9d928f31015a0b9860';

select distinct item_code, ITEM_NAME
from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA
where T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA.ITEM_CAT_CODE < '06'
  AND ITEM_CODE not in (select distinct item_code
                        from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_MAT
                        union
                        select distinct product_code
                        from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST);

SELECT *
FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_MAT;

INSERT INTO BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST (REC_ID, START_TIME, END_TIME, DATA_CODE, UUID, BASE_CODE,
                                                               REC_CREATOR, REC_CREATE_TIME, REC_REVISOR,
                                                               REC_REVISE_TIME, REMARK, FLAG)
VALUES (HEX(RAND()), '2024', '2024', '10304', '3f185f1aec484f9d928f31015a0b9860', 'TA', null,
        '20250825', null, null, null, 'SY');





