WITH DATA as (SELECT DISTINCT *
              FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
              WHERE COMPANY_CODE = 'TA'
                AND BATCH_NUMBER = '20250120251220260209YS'
                AND FACTOR_VERSION = 'NORM_Ecoinvent3.11'),
     PRODUCT AS (SELECT DISTINCT BATCH_NUMBER, PRODUCT_CODE, LCA_DATA_ITEM_NAME, VALUE AS PRODUCT_VALUE, UNIT
                 FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
                 where COMPANY_CODE = 'TA'
                   AND LCA_DATA_ITEM_CAT_CODE = '04'
                   AND BATCH_NUMBER = '20250120251220260209YS'
                   AND PRODUCT_CODE = 'L7211'),
     RESULT1 AS (SELECT COMPANY_CODE,
                        BATCH_NUMBER,
                        FACTOR_VERSION,
                        TARGET_PROC_KEY  as PROC_KEY,
                        TARGET_PROC_NAME AS PROC_NAME,
                        PRODUCT_CODE,
                        PRODUCT_NAME,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD)        AS LOAD
                 FROM DATA
                 GROUP BY COMPANY_CODE,
                          BATCH_NUMBER,
                          FACTOR_VERSION,
                          TARGET_PROC_KEY,
                          TARGET_PROC_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          ITEM_CODE,
                          ITEM_NAME,
                          LCI_ELEMENT_CODE),
     RESULT2 AS (SELECT *
                 FROM RESULT1
                 WHERE PRODUCT_CODE IN (SELECT DISTINCT PRODUCT_CODE FROM PRODUCT)),
     RESULT3 AS (SELECT A.COMPANY_CODE,
                        A.BATCH_NUMBER,
                        A.FACTOR_VERSION,
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
     RESULT4 AS (SELECT COMPANY_CODE,
                        BATCH_NUMBER,
                        FACTOR_VERSION,
                        PROC_KEY,
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
                 GROUP BY COMPANY_CODE, BATCH_NUMBER, FACTOR_VERSION, PROC_KEY, PROC_NAME, PRODUCT_CODE, PRODUCT_NAME,
                          ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE, EMISSION_TOTAL)
SELECT *
FROM RESULT4
ORDER BY PROC_KEY, SENSITIVITY DESC;
