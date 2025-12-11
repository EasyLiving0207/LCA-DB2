WITH NORM_RESULT AS (SELECT *
                     FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
                     WHERE COMPANY_CODE = 'TA'
                       AND BATCH_NUMBER = '20240120241220250107YS'),
     CONS_RESULT AS (SELECT *
                     FROM T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT_VER2
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
              ORDER BY NORM.PROC_KEY, A.NORM_LCI_ELEMENT_NAME),
     RESULT AS (SELECT A.REC_ID,
                       BATCH_NUMBER,
                       START_YM,
                       END_YM,
                       COMPANY_CODE,
                       PROC_KEY,
                       PROC_CODE,
                       PROC_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       LCI_ELEMENT_NAME,
                       B.CONS_LCI_ELEMENT_CNAME,
                       C_CYCLE,
                       C1_DIRECT,
                       C2_BP,
                       C3_OUT,
                       C4_BP_NEG,
                       C5_TRANS,
                       C_INSITE,
                       C_OUTSITE,
                       REC_CREATOR,
                       REC_CREATE_TIME,
                       REC_REVISOR,
                       REC_REVISE_TIME
                FROM CONS_RESULT A
                         LEFT JOIN T_ADS_FACT_LCA_EPD_LCI_ELEMENT_NORM_CONS_MAP B ON
                    A.LCI_ELEMENT_NAME = B.CONS_LCI_ELEMENT_NAME
                        AND B.CONS_LCI_ELEMENT_CNAME IS NOT NULL
                ORDER BY LCI_ELEMENT_NAME, PROC_KEY)
SELECT *
FROM RESULT
;


-- INSERT INTO T_ADS_FACT_LCA_PROC_DATA_CONS (REC_ID, BATCH_NUMBER, START_YM, END_YM, LCA_PROC_CODE, LCA_PROC_NAME,
--                                            PRODUCT_CODE, PRODUCT_NAME, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
--                                            LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, VALUE, UNIT, INDEX_CODE,
--                                            COMPANY_CODE,
--                                            REC_CREATE_TIME, REC_CREATOR, MAT_STATUS, WG_PRODUCT_CODE)
WITH DATA AS (SELECT *
              FROM T_ADS_FACT_LCA_PROC_DATA
              WHERE COMPANY_CODE = 'TA'
                AND BATCH_NUMBER = '20240120241220250107YS'),
     DATA_REST AS (SELECT *
                   FROM DATA
                   WHERE LCA_PROC_CODE NOT IN ('CO01', 'CO04', 'CO03', 'BF01', 'BF02', 'BF03', 'BF04', '1YLT', '2YLT')
                     AND NOT (LCA_PROC_CODE = 'MQ01' AND PRODUCT_CODE = '961')),
     CO_DATA AS (SELECT * FROM DATA WHERE LCA_PROC_CODE IN ('CO01', 'CO04', 'CO03')),
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
     COG_OUTPUT AS (SELECT HEX(RAND())        AS REC_ID,
                           BATCH_NUMBER,
                           START_YM,
                           END_YM,
                           'COMQ'             AS LCA_PROC_CODE,
                           '炼铁厂-焦炉-煤气' AS LCA_PROC_NAME,
                           '35200'            AS PRODUCT_CODE,
                           '焦炉荒煤气'       AS PRODUCT_NAME,
                           '04'               AS LCA_DATA_ITEM_CAT_CODE,
                           '产品'             AS LCA_DATA_ITEM_CAT_NAME,
                           '35200'            AS LCA_DATA_ITEM_CODE,
                           '焦炉荒煤气'       AS LCA_DATA_ITEM_NAME,
                           SUM(VALUE)         AS VALUE,
                           UNIT,
                           ''                 AS INDEX_CODE,
                           COMPANY_CODE,
                           ''                 AS REC_CREATE_TIME,
                           ''                 AS REC_CREATOR,
                           ''                 AS MAT_STATUS,
                           ''                 AS WG_PRODUCT_CODE
                    FROM CO_DATA
                    WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                      AND LCA_DATA_ITEM_CODE = '35200'
                    GROUP BY BATCH_NUMBER, START_YM, END_YM, UNIT, COMPANY_CODE),
     COG_INPUT AS (SELECT HEX(RAND())         AS REC_ID,
                          BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          'COMQ'              AS LCA_PROC_CODE,
                          '炼铁厂-焦炉-煤气'  AS LCA_PROC_NAME,
                          '35200'             AS PRODUCT_CODE,
                          '焦炉荒煤气'        AS PRODUCT_NAME,
                          LCA_DATA_ITEM_CAT_CODE,
                          LCA_DATA_ITEM_CAT_NAME,
                          LCA_DATA_ITEM_CODE,
                          LCA_DATA_ITEM_NAME,
                          SUM(VALUE * 0.1063) AS VALUE,
                          UNIT,
                          ''                  AS INDEX_CODE,
                          COMPANY_CODE,
                          ''                  AS REC_CREATE_TIME,
                          ''                  AS REC_CREATOR,
                          ''                  AS MAT_STATUS,
                          ''                  AS WG_PRODUCT_CODE
                   FROM CO_DIST
                   GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                            LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     COG_DATA AS (SELECT * FROM COG_OUTPUT UNION SELECT * FROM COG_INPUT),
     COG_SUM AS (SELECT *
                 FROM DATA
                 WHERE LCA_PROC_CODE = 'MQ01'
                   AND PRODUCT_CODE = '961'
                   AND LCA_DATA_ITEM_CAT_CODE != '02'),
     COG_SUM_INPUT AS (SELECT REC_ID,
                              BATCH_NUMBER,
                              START_YM,
                              END_YM,
                              'MQ01'                 AS LCA_PROC_CODE,
                              '能环部-能源中心-煤气' AS LCA_PROC_NAME,
                              '961'                  AS PRODUCT_CODE,
                              '焦炉煤气发生量'       AS PRODUCT_NAME,
                              '02'                   AS LCA_DATA_ITEM_CAT_CODE,
                              '原材料'               AS LCA_DATA_ITEM_CAT_NAME,
                              LCA_DATA_ITEM_CODE,
                              LCA_DATA_ITEM_NAME,
                              VALUE,
                              UNIT,
                              INDEX_CODE,
                              COMPANY_CODE,
                              REC_CREATE_TIME,
                              REC_CREATOR,
                              MAT_STATUS,
                              WG_PRODUCT_CODE
                       FROM COG_OUTPUT),
     COG_SUM_DATA AS (SELECT *
                      FROM COG_SUM
                      UNION
                      SELECT *
                      FROM COG_SUM_INPUT),
     BF_DATA AS (SELECT * FROM DATA WHERE LCA_PROC_CODE IN ('BF01', 'BF02', 'BF03', 'BF04')),
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
                           'BFMQ'             AS LCA_PROC_CODE,
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
                          'BFMQ'              AS LCA_PROC_CODE,
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
                           'BFDL'                AS LCA_PROC_CODE,
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
                          'BFDL'                AS LCA_PROC_CODE,
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
     BOF_DATA AS (SELECT * FROM DATA WHERE LCA_PROC_CODE IN ('1YLT', '2YLT')),
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
                           '1LDG'                 AS LCA_PROC_CODE,
                           '炼钢厂-转炉-转炉煤气' AS LCA_PROC_NAME,
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
                          '1LDG'                 AS LCA_PROC_CODE,
                          '炼钢厂-转炉-转炉煤气' AS LCA_PROC_NAME,
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
                   FROM COG_SUM_DATA
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
SELECT *
FROM BF_DATA;

-- SELECT HEX(RAND())                                  AS REC_ID,
--        CONCAT(BATCH_NUMBER, '_CONS')                AS BATCH_NUMBER,
--        START_YM,
--        END_YM,
--        LCA_PROC_CODE,
--        LCA_PROC_NAME,
--        PRODUCT_CODE,
--        PRODUCT_NAME,
--        LCA_DATA_ITEM_CAT_CODE,
--        LCA_DATA_ITEM_CAT_NAME,
--        LCA_DATA_ITEM_CODE,
--        LCA_DATA_ITEM_NAME,
--        VALUE,
--        UNIT,
--        NULL                                         AS INDEX_CODE,
--        COMPANY_CODE,
--        TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI') AS REC_CREATE_TIME,
--        NULL                                         AS REC_CREATOR,
--        MAT_STATUS,
--        WG_PRODUCT_CODE
-- FROM DATA_CONS;

SELECT *
FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST
WHERE PROC_KEY LIKE 'COMQ%';

WITH DATA_NORM AS (SELECT *
                   FROM (SELECT PROC_KEY,
                                PROC_CODE,
                                PROC_NAME,
                                PRODUCT_NAME,
                                A.ITEM_CODE,
                                A.ITEM_NAME,
                                A.VALUE,
                                A.UNIT,
                                B.LCI_ELEMENT_ID,
                                B.LCI_ELEMENT_NAME,
                                B.LCI_ELEMENT_VALUE,
                                CASE
                                    WHEN ITEM_CAT_CODE < '04' THEN A.VALUE * B.LCI_ELEMENT_VALUE
                                    ELSE -ABS(A.VALUE * B.LCI_ELEMENT_VALUE) END AS LOAD
                         FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA_NORM A
                                  INNER JOIN (SELECT *
                                              FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_GWP) B
                                             ON A.ITEM_CODE = B.ITEM_CODE
                         UNION
                         SELECT PROC_KEY,
                                PROC_CODE,
                                PROC_NAME,
                                PRODUCT_NAME,
                                A.ITEM_CODE,
                                A.ITEM_NAME,
                                A.VALUE,
                                A.UNIT,
                                B.LCI_ELEMENT_ID,
                                B.LCI_ELEMENT_NAME,
                                B.LCI_ELEMENT_VALUE,
                                A.VALUE * B.LCI_ELEMENT_VALUE AS LOAD
                         FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA_NORM A
                                  INNER JOIN (SELECT *
                                              FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_EP) B
                                             ON A.ITEM_CODE = B.ITEM_CODE)
                   WHERE LOAD IS NOT NULL),
     DIST_NORM AS (SELECT *
                   FROM BG00MAC102.T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST_NORM
                   WHERE LOAD IS NOT NULL),
     CO_DIST AS (SELECT * FROM DIST_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')),
     COKE_LOAD AS (SELECT PROC_KEY,
                          PROC_CODE,
                          PROC_NAME,
                          PRODUCT_NAME,
                          ITEM_CODE,
                          ITEM_NAME,
                          UNIT_COST * 0.7638 AS UNIT_COST,
                          LCI_ELEMENT_ID,
                          LCI_ELEMENT_NAME,
                          LCI_ELEMENT_VALUE,
                          LOAD * 0.7638      AS LOAD
                   FROM CO_DIST),
     COG_LOAD AS (SELECT A.PROC_KEY,
                         A.PROC_CODE,
                         A.PROC_NAME,
                         A.PRODUCT_NAME,
                         ITEM_CODE,
                         ITEM_NAME,
                         CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.1063 AS UNIT_COST,
                         LCI_ELEMENT_ID,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.1063  AS LOAD
                  FROM (SELECT *
                        FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COMQ') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')) B ON 1 = 1),
     BF_DIST AS (SELECT * FROM DIST_NORM WHERE PROC_CODE IN ('BF01', 'BF02', 'BF03', 'BF04')),
     IRON_LOAD AS (SELECT PROC_KEY,
                          PROC_CODE,
                          PROC_NAME,
                          PRODUCT_NAME,
                          ITEM_CODE,
                          ITEM_NAME,
                          UNIT_COST * 0.9322 AS UNIT_COST,
                          LCI_ELEMENT_ID,
                          LCI_ELEMENT_NAME,
                          LCI_ELEMENT_VALUE,
                          LOAD * 0.9322      AS LOAD
                   FROM BF_DIST),
     BFG_LOAD AS (SELECT A.PROC_KEY,
                         A.PROC_CODE,
                         A.PROC_NAME,
                         A.PRODUCT_NAME,
                         ITEM_CODE,
                         ITEM_NAME,
                         CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0545 AS UNIT_COST,
                         LCI_ELEMENT_ID,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0545  AS LOAD
                  FROM (SELECT *
                        FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'BFMQ') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('BF01', 'BF02', 'BF03', 'BF04')) B
                                ON 1 = 1),
     TRT_LOAD AS (SELECT A.PROC_KEY,
                         A.PROC_CODE,
                         A.PROC_NAME,
                         A.PRODUCT_NAME,
                         ITEM_CODE,
                         ITEM_NAME,
                         CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0094 AS UNIT_COST,
                         LCI_ELEMENT_ID,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0094  AS LOAD
                  FROM (SELECT *
                        FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'BFDL') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('BF01', 'BF02', 'BF03', 'BF04')) B
                                ON 1 = 1),
     BOF_DIST AS (SELECT * FROM DIST_NORM WHERE PROC_CODE IN ('1YLT', '2YLT')),
     STEEL_LOAD AS (SELECT PROC_KEY,
                           PROC_CODE,
                           PROC_NAME,
                           PRODUCT_NAME,
                           ITEM_CODE,
                           ITEM_NAME,
                           UNIT_COST * 0.9877 AS UNIT_COST,
                           LCI_ELEMENT_ID,
                           LCI_ELEMENT_NAME,
                           LCI_ELEMENT_VALUE,
                           LOAD * 0.9877      AS LOAD
                    FROM BOF_DIST),
     LDG_LOAD AS (SELECT A.PROC_KEY,
                         A.PROC_CODE,
                         A.PROC_NAME,
                         A.PRODUCT_NAME,
                         ITEM_CODE,
                         ITEM_NAME,
                         CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0084 AS UNIT_COST,
                         LCI_ELEMENT_ID,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0084  AS LOAD
                  FROM (SELECT *
                        FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST
                        WHERE PROC_CODE = '1LDG') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('1YLT', '2YLT')) B ON 1 = 1),
     SUM_LOAD AS (SELECT *
                  FROM COKE_LOAD
                  UNION
                  SELECT *
                  FROM COG_LOAD
                  UNION
                  SELECT *
                  FROM IRON_LOAD
                  UNION
                  SELECT *
                  FROM BFG_LOAD
                  UNION
                  SELECT *
                  FROM TRT_LOAD
                  UNION
                  SELECT *
                  FROM STEEL_LOAD
                  UNION
                  SELECT *
                  FROM LDG_LOAD),
     REST_LOAD AS (SELECT *
                   FROM BG00MAC102.T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST
                   WHERE LOAD IS NOT NULL
                     AND PROC_KEY NOT IN (SELECT DISTINCT PROC_KEY FROM SUM_LOAD)),
     RESULT AS (SELECT *
                FROM REST_LOAD
                UNION
                SELECT *
                FROM SUM_LOAD)
SELECT *
FROM RESULT;


select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'ZG'
  AND BATCH_NUMBER = '20240120241220250715YS';

