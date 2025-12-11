WITH NORM_RESULT AS (SELECT *
                     FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
                     WHERE COMPANY_CODE = 'TA'
                       AND BATCH_NUMBER = '20240120241220250107YS'),
     CONS_RESULT AS (SELECT *
                     FROM T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
                     WHERE COMPANY_CODE = 'TA'
                       AND BATCH_NUMBER = '20240120241220250107YS_CONS'),
     LCI_ELEMENT AS (SELECT *
                     FROM T_ADS_FACT_LCA_EPD_LCI_ELEMENT_NORM_CONS_MAP
                     WHERE NORM_LCI_ELEMENT_NAME IS NOT NULL
                       AND CONS_LCI_ELEMENT_NAME IS NOT NULL),
     DIFF AS (SELECT A.NORM_LCI_ELEMENT_NAME                                   AS LCI_ELEMENT_NAME,
                     NORM.BATCH_NUMBER,
                     NORM.COMPANY_CODE,
                     NORM.PROC_KEY,
                     NORM.PROC_CODE,
                     NORM.PROC_NAME,
                     NORM.PRODUCT_CODE,
                     NORM.PRODUCT_NAME,
                     NORM.C_CYCLE                                              AS NORM_CYCLE,
                     CONS.C_CYCLE                                              AS CONS_CYCLE,
                     NORM.C_CYCLE - CONS.C_CYCLE                               AS DIFF,
                     CASE
                         WHEN NORM.C_CYCLE = 0 THEN NULL
                         ELSE (NORM.C_CYCLE - CONS.C_CYCLE) / NORM.C_CYCLE END AS PERCENTAGE,
                     NORM.C1_DIRECT,
                     CONS.C1_DIRECT                                            AS CONS_C1,
                     NORM.C2_BP,
                     CONS.C2_BP                                                AS CONS_C2,
                     NORM.C3_OUT,
                     CONS.C3_OUT                                               AS CONS_C3,
                     NORM.C4_BP_NEG,
                     CONS.C4_BP_NEG                                            AS CONS_C4,
                     NORM.C5_TRANS,
                     CONS.C5_TRANS                                             AS CONS_C5
              FROM LCI_ELEMENT A
                       JOIN NORM_RESULT NORM ON A.NORM_LCI_ELEMENT_NAME = NORM.LCI_ELEMENT_NAME
                       JOIN CONS_RESULT CONS
                            ON A.CONS_LCI_ELEMENT_NAME = CONS.LCI_ELEMENT_NAME AND NORM.PROC_KEY = CONS.PROC_KEY
              ORDER BY NORM.PROC_KEY, A.NORM_LCI_ELEMENT_NAME)
SELECT *
FROM DIFF
;


-- INSERT INTO T_ADS_FACT_LCA_PROC_DATA_CONS (REC_ID, BATCH_NUMBER, START_YM, END_YM, LCA_PROC_CODE, LCA_PROC_NAME,
--                                            PRODUCT_CODE, PRODUCT_NAME, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
--                                            LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, VALUE, UNIT, I NDEX_CODE,
--                                            COMPANY_CODE,
--                                            REC_CREATE_TIME, REC_CREATOR, MAT_STATUS, WG_PRODUCT_CODE)
WITH DATA AS (SELECT *
              FROM T_ADS_FACT_LCA_PROC_DATA
              WHERE COMPANY_CODE = 'BSZG'
                AND BATCH_NUMBER = '20240120241220250813YS'),
     DATA_REST AS (SELECT *
                   FROM DATA
                   WHERE LCA_PROC_CODE NOT IN ('LT04', 'XABC', 'X004', 'XABD', 'LT05', 'LT06', 'LT10', 'LG04')),
     CO_DATA AS (SELECT * FROM DATA WHERE LCA_PROC_CODE IN ('LT04', 'XABC')),
     CO_DIST AS (SELECT * FROM CO_DATA WHERE LCA_DATA_ITEM_CAT_CODE NOT IN ('04', '05')),
     COKE_DATA AS (SELECT REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          LCA_PROC_CODE,
                          LCA_PROC_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          VALUE * 0.7638 AS VALUE,
                          UNIT,
                          INDEX_CODE,
                          COMPANY_CODE,
                          REC_CREATE_TIME,
                          REC_CREATOR,
                          MAT_STATUS,
                          WG_PRODUCT_CODE
                   FROM CO_DIST
                   UNION
                   SELECT *
                   FROM CO_DATA
                   WHERE LCA_DATA_ITEM_CAT_CODE = '04'),
     COG_OUTPUT AS (SELECT *
                    FROM DATA
                    WHERE LCA_PROC_CODE IN ('X004', 'XABD')
                      AND LCA_DATA_ITEM_CAT_CODE = '04'),
     COG_INPUT AS (SELECT REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          CASE
                              WHEN LCA_PROC_CODE = 'LT04' THEN 'X004'
                              ELSE 'XABD' END                                                        AS LCA_PROC_CODE,
                          CASE
                              WHEN LCA_PROC_CODE = 'LT04'
                                  THEN (SELECT LCA_PROC_NAME FROM DATA WHERE LCA_PROC_CODE = 'X004')
                              ELSE (SELECT LCA_PROC_NAME FROM DATA WHERE LCA_PROC_CODE = 'XABD') END AS LCA_PROC_NAME,
                          CASE
                              WHEN LCA_PROC_CODE = 'LT04' THEN 'X0046'
                              ELSE 'XABD6' END                                                       AS PRODUCT_CODE,
                          CASE
                              WHEN LCA_PROC_CODE = 'LT04' THEN '回收精制煤气4'
                              ELSE '回收精制煤气D' END                                               AS PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          VALUE * 0.1063                                                             AS VALUE,
                          UNIT,
                          ''                                                                         AS INDEX_CODE,
                          COMPANY_CODE,
                          ''                                                                         AS REC_CREATE_TIME,
                          ''                                                                         AS REC_CREATOR,
                          ''                                                                         AS MAT_STATUS,
                          ''                                                                         AS WG_PRODUCT_CODE
                   FROM CO_DIST
                   UNION
                   SELECT *
                   FROM DATA
                   WHERE LCA_PROC_CODE IN ('X004', 'XABD')
                     AND LCA_DATA_ITEM_CAT_CODE NOT IN ('04', '05')
                     AND LCA_DATA_ITEM_CODE != '35200'),
     COG_DATA AS (SELECT * FROM COG_OUTPUT UNION SELECT * FROM COG_INPUT),
     BF_DATA AS (SELECT * FROM DATA WHERE LCA_PROC_CODE IN ('LT05', 'LT06', 'LT10')),
     BF_DIST AS (SELECT * FROM BF_DATA WHERE LCA_DATA_ITEM_CAT_CODE NOT IN ('04', '05')),
     IRON_DATA AS (SELECT REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          LCA_PROC_CODE,
                          LCA_PROC_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          VALUE * 0.9322 AS VALUE,
                          UNIT,
                          INDEX_CODE,
                          COMPANY_CODE,
                          REC_CREATE_TIME,
                          REC_CREATOR,
                          MAT_STATUS,
                          WG_PRODUCT_CODE
                   FROM BF_DIST
                   UNION
                   SELECT *
                   FROM BF_DATA
                   WHERE LCA_DATA_ITEM_CAT_CODE = '04'),
     BFG_OUTPUT AS (SELECT HEX(RAND())        AS REC_ID,
                           BATCH_NUMBER,
                           START_YM,
                           END_YM,
                           'LTMQ'             AS LCA_PROC_CODE,
                           '炼铁厂-高炉-煤气' AS LCA_PROC_NAME,
                           '48080'            AS PRODUCT_CODE,
                           '回收高炉煤气'     AS PRODUCT_NAME,
                           '04'               AS LCA_DATA_ITEM_CAT_CODE,
                           '产品'             AS LCA_DATA_ITEM_CAT_NAME,
                           LCA_DATA_ITEM_CODE,
                           LCA_DATA_ITEM_NAME,
                           SUM(VALUE)         AS VALUE,
                           UNIT,
                           ''                 AS INDEX_CODE,
                           COMPANY_CODE,
                           ''                 AS REC_CREATE_TIME,
                           ''                 AS REC_CREATOR,
                           ''                 AS MAT_STATUS,
                           ''                 AS WG_PRODUCT_CODE
                    FROM BF_DATA
                    WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                      AND LCA_DATA_ITEM_CODE = '48080'
                    GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CODE,
                             LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     BFG_INPUT AS (SELECT HEX(RAND())         AS REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          'LTMQ'              AS LCA_PROC_CODE,
                          '炼铁厂-高炉-煤气'  AS LCA_PROC_NAME,
                          '48080'             AS PRODUCT_CODE,
                          '回收高炉煤气'      AS PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          SUM(VALUE * 0.0545) AS VALUE,
                          UNIT,
                          ''                  AS INDEX_CODE,
                          COMPANY_CODE,
                          ''                  AS REC_CREATE_TIME,
                          ''                  AS REC_CREATOR,
                          ''                  AS MAT_STATUS,
                          ''                  AS WG_PRODUCT_CODE
                   FROM BF_DIST
                   GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                            LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     BFG_DATA AS (SELECT * FROM BFG_INPUT UNION SELECT * FROM BFG_OUTPUT),
     TRT_OUTPUT AS (SELECT HEX(RAND())           AS REC_ID,
                           BATCH_NUMBER,
                           START_YM,
                           END_YM,
                           'LTDL'                AS LCA_PROC_CODE,
                           '炼铁厂-高炉-TRT发电' AS LCA_PROC_NAME,
                           '58080'               AS PRODUCT_CODE,
                           '电回收TRT电'         AS PRODUCT_NAME,
                           '04'                  AS LCA_DATA_ITEM_CAT_CODE,
                           '产品'                AS LCA_DATA_ITEM_CAT_NAME,
                           LCA_DATA_ITEM_CODE,
                           LCA_DATA_ITEM_NAME,
                           SUM(VALUE)            AS VALUE,
                           UNIT,
                           ''                    AS INDEX_CODE,
                           COMPANY_CODE,
                           ''                    AS REC_CREATE_TIME,
                           ''                    AS REC_CREATOR,
                           ''                    AS MAT_STATUS,
                           ''                    AS WG_PRODUCT_CODE
                    FROM BF_DATA
                    WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                      AND LCA_DATA_ITEM_CODE = '58080'
                    GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CODE,
                             LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     TRT_INPUT AS (SELECT HEX(RAND())           AS REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          'LTDL'                AS LCA_PROC_CODE,
                          '炼铁厂-高炉-TRT发电' AS LCA_PROC_NAME,
                          '58080'               AS PRODUCT_CODE,
                          '电回收TRT电'         AS PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          SUM(VALUE * 0.0094)   AS VALUE,
                          UNIT,
                          ''                    AS INDEX_CODE,
                          COMPANY_CODE,
                          ''                    AS REC_CREATE_TIME,
                          ''                    AS REC_CREATOR,
                          ''                    AS MAT_STATUS,
                          ''                    AS WG_PRODUCT_CODE
                   FROM BF_DIST
                   GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                            LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     TRT_DATA AS (SELECT * FROM TRT_OUTPUT UNION SELECT * FROM TRT_INPUT),
     BOF_DATA AS (SELECT * FROM DATA WHERE LCA_PROC_CODE IN ('LG04')),
     BOF_DIST AS (SELECT * FROM BOF_DATA WHERE LCA_DATA_ITEM_CAT_CODE NOT IN ('04', '05')),
     STEEL_DATA AS (SELECT REC_ID,
                           BATCH_NUMBER,
                           START_YM,
                           END_YM,
                           LCA_PROC_CODE,
                           LCA_PROC_NAME,
                           PRODUCT_CODE,
                           PRODUCT_NAME,
                           LCA_DATA_ITEM_CAT_CODE,
                           LCA_DATA_ITEM_CAT_NAME,
                           LCA_DATA_ITEM_CODE,
                           LCA_DATA_ITEM_NAME,
                           VALUE * 0.9877 AS VALUE,
                           UNIT,
                           INDEX_CODE,
                           COMPANY_CODE,
                           REC_CREATE_TIME,
                           REC_CREATOR,
                           MAT_STATUS,
                           WG_PRODUCT_CODE
                    FROM BOF_DIST
                    UNION
                    SELECT *
                    FROM BOF_DATA
                    WHERE LCA_DATA_ITEM_CAT_CODE = '04'),
     LDG_OUTPUT AS (SELECT HEX(RAND())            AS REC_ID,
                           BATCH_NUMBER,
                           START_YM,
                           END_YM,
                           'LGMQ'                 AS LCA_PROC_CODE,
                           '炼钢厂-冶炼-转炉煤气' AS LCA_PROC_NAME,
                           '48082'                AS PRODUCT_CODE,
                           '回收转炉煤气'         AS PRODUCT_NAME,
                           '04'                   AS LCA_DATA_ITEM_CAT_CODE,
                           '产品'                 AS LCA_DATA_ITEM_CAT_NAME,
                           LCA_DATA_ITEM_CODE,
                           LCA_DATA_ITEM_NAME,
                           SUM(VALUE)             AS VALUE,
                           UNIT,
                           ''                     AS INDEX_CODE,
                           COMPANY_CODE,
                           ''                     AS REC_CREATE_TIME,
                           ''                     AS REC_CREATOR,
                           ''                     AS MAT_STATUS,
                           ''                     AS WG_PRODUCT_CODE
                    FROM BOF_DATA
                    WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                      AND LCA_DATA_ITEM_CODE = '48082'
                    GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CODE,
                             LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     LDG_INPUT AS (SELECT HEX(RAND())            AS REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          'LGMQ'                 AS LCA_PROC_CODE,
                          '炼钢厂-冶炼-转炉煤气' AS LCA_PROC_NAME,
                          '48082'                AS PRODUCT_CODE,
                          '回收转炉煤气'         AS PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          SUM(VALUE * 0.0084)    AS VALUE,
                          UNIT,
                          ''                     AS INDEX_CODE,
                          COMPANY_CODE,
                          ''                     AS REC_CREATE_TIME,
                          ''                     AS REC_CREATOR,
                          ''                     AS MAT_STATUS,
                          ''                     AS WG_PRODUCT_CODE
                   FROM BOF_DIST
                   GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                            LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     LDG_DATA AS (SELECT * FROM LDG_INPUT UNION SELECT * FROM LDG_OUTPUT),
     DATA_CONS AS (SELECT *
                   FROM DATA_REST
                   UNION
                   SELECT *
                   FROM COKE_DATA
                   UNION
                   SELECT *
                   FROM COG_DATA
                   UNION
                   SELECT *
                   FROM IRON_DATA
                   UNION
                   SELECT *
                   FROM BFG_DATA
                   UNION
                   SELECT *
                   FROM TRT_DATA
                   UNION
                   SELECT *
                   FROM STEEL_DATA
                   UNION
                   SELECT *
                   FROM LDG_DATA)
select *
from DATA_CONS;




;