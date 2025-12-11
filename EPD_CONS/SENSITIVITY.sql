WITH C1_DIST AS (SELECT *
                 FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST
                 WHERE LCI_ELEMENT_CODE = 'GWP-total'),
     DIST AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                     B.PROC_KEY           AS TARGET_PROC_KEY,
                     A.ITEM_CODE,
                     A.ITEM_NAME,
                     A.LCI_ELEMENT_CODE,
                     A.LOAD * B.UNIT_COST AS LOAD
              FROM C1_DIST A
                       CROSS JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_MATRIX_INV B
              WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     DIST_SUM AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                         ITEM_CODE,
                         ITEM_NAME,
                         LCI_ELEMENT_CODE,
                         SUM(LOAD)       AS LOAD
                  FROM DIST
                  GROUP BY TARGET_PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     DIST_C1 AS (SELECT PROC_KEY,
                        CASE
                            WHEN ITEM_CODE IN ('70201', '70202') OR ITEM_CODE LIKE 'BF0%'
                                THEN 'HSGT01'
                            WHEN ITEM_CODE LIKE 'CO0%' OR ITEM_CODE = '965' THEN 'HSGT04'
                            ELSE ITEM_CODE END AS ITEM_CODE,
                        CASE
                            WHEN ITEM_CODE IN ('70201', '70202') OR ITEM_CODE LIKE 'BF0%' THEN '无烟煤'
                            WHEN ITEM_CODE LIKE 'CO0%' OR ITEM_CODE = '965' THEN '洗精煤'
                            ELSE ITEM_NAME END AS ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        LOAD
                 FROM DIST_SUM),
--      DIST_C1 AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
--                         ITEM_CODE,
--                         ITEM_NAME,
--                         LCI_ELEMENT_CODE,
--                         SUM(LOAD)       AS LOAD
--                  FROM (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
--                               B.PROC_KEY           AS TARGET_PROC_KEY,
--                               A.ITEM_CODE,
--                               A.ITEM_NAME,
--                               A.LCI_ELEMENT_CODE,
--                               A.LOAD * B.UNIT_COST AS LOAD
--                        FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST A
--                                 CROSS JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_MATRIX_INV B
--                        WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
--                          AND A.LCI_ELEMENT_CODE = 'GWP-total')
--                  GROUP BY TARGET_PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     DIST_C2 AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD)       AS LOAD
                 FROM (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C2_DIST A
                                CROSS JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                         AND A.LCI_ELEMENT_CODE = 'GWP-total')
                 GROUP BY TARGET_PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     DIST_C3 AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD)       AS LOAD
                 FROM (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C3_DIST A
                                CROSS JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                         AND A.LCI_ELEMENT_CODE = 'GWP-total')
                 GROUP BY TARGET_PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     DIST_C4 AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD)       AS LOAD
                 FROM (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C4_DIST A
                                CROSS JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                         AND A.LCI_ELEMENT_CODE = 'GWP-total')
                 GROUP BY TARGET_PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     DIST_C5 AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD)       AS LOAD
                 FROM (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C5_DIST A
                                CROSS JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                         AND A.LCI_ELEMENT_CODE = 'GWP-total')
                 GROUP BY TARGET_PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     RESULT1 AS (SELECT PROC_KEY,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        SUM(LOAD) AS LOAD
                 FROM (SELECT *
                       FROM DIST_C1
                       UNION
                       SELECT *
                       FROM DIST_C2
                       UNION
                       SELECT *
                       FROM DIST_C3
                       UNION
                       SELECT *
                       FROM DIST_C4
                       UNION
                       SELECT *
                       FROM DIST_C5)
                 GROUP BY PROC_KEY, ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE),
     RESULT2 AS (SELECT A.PROC_KEY,
                        B.PROC_NAME,
                        PRODUCT_NAME,
                        PRODUCT_VALUE,
                        ITEM_CODE,
                        ITEM_NAME,
                        LCI_ELEMENT_CODE,
                        LOAD,
                        SUM(LOAD) OVER ( PARTITION BY A.PROC_KEY)        AS GWP,
                        LOAD / SUM(LOAD) OVER ( PARTITION BY A.PROC_KEY) AS SENSITIVITY
                 FROM (SELECT *
                       FROM RESULT1
                       WHERE LOAD IS NOT NULL) A
                          JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST B
                               ON A.PROC_KEY = B.PROC_KEY
                 ORDER BY PROC_KEY, SENSITIVITY DESC),
     RESULT3 AS (SELECT *
                 FROM RESULT2
                 WHERE PROC_KEY IN ('HB01_J0210', 'HB02_J2210', 'HB03_J1210')),
     RESULT4 AS (SELECT *,
                        (SELECT sum(PRODUCT_VALUE)
                         FROM (select distinct PRODUCT_NAME, PRODUCT_VALUE, GWP
                               from RESULT3)) AS SUM_PRODUCT,
                        (SELECT sum(PRODUCT_VALUE * GWP)
                         FROM (select distinct PRODUCT_NAME, PRODUCT_VALUE, GWP
                               from RESULT3)) AS SUM_GWP
                 FROM RESULT3),
     RESULT5 AS (SELECT *, PRODUCT_VALUE * LOAD AS AGG_LOAD
                 FROM RESULT4),
     RESULT AS (SELECT ITEM_CODE, ITEM_NAME, SUM_GWP, SUM(AGG_LOAD) AS SUM_LOAD, SUM(AGG_LOAD) / SUM_GWP AS SENSITIVITY
                FROM RESULT5
                GROUP BY ITEM_CODE, ITEM_NAME, SUM_GWP
                ORDER BY SENSITIVITY DESC)
SELECT *
FROM RESULT
;

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where VERSION like 'EN%'
  and NAME like '%生石灰%'
  and LCI_ELEMENT_CODE = 'GWP-total';


select *
from T_ADS_WH_LCA_MAT_DATA
where ITEM_NAME like '%石灰%';






