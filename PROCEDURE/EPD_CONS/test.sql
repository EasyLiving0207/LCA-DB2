select *
from T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION
where VERSION = 'EF3.1_Ecoinvent3.11';

select *
from BG00MAC102.T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120241220250915YS';

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120241220250915YS_CONS';


select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where BASE_CODE = 'BSZG'
  and UUID = 'e315699cc059480cab7cdc75dbfedc19';

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where UUID = '8d2d0806be9f49a187b91b378de5aaf7'
  and VERSION = 'EF3.1_Ecoinvent3.11';

select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where UUID = '32d5dd4b04404874bb275e1d830c2b65';

select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where DATA_CODE = '12304';

select *
from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where company_code = 'TA'
  AND BATCH_NUMBER = '20240120241220250911YS_CONS'
order by LCI_ELEMENT_CODE, PROC_KEY;


WITH DATA AS (SELECT *
              FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA_0002_DT
              WHERE COMPANY_CODE = 'TA'
                AND BATCH_NUMBER = '20240120241220250911YS'),
     DATA_REST AS (SELECT *
                   FROM DATA
                   WHERE LCA_PROC_CODE NOT IN
                         ('CO01', 'CO04', 'CO03', 'BF01', 'BF02', 'BF03', 'BF04', '1YLT', '2YLT')),
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
     CO_STEAM1_OUTPUT AS (SELECT HEX(RAND())            AS REC_ID,
                                 BATCH_NUMBER,
                                 START_YM,
                                 END_YM,
                                 'COZQ1'                AS LCA_PROC_CODE,
                                 '炼铁厂-焦炉-低压蒸汽' AS LCA_PROC_NAME,
                                 'HJ280'                AS PRODUCT_CODE,
                                 '回收低压蒸汽_焦炉'    AS PRODUCT_NAME,
                                 '04'                   AS LCA_DATA_ITEM_CAT_CODE,
                                 '产品'                 AS LCA_DATA_ITEM_CAT_NAME,
                                 'HJ280'                AS LCA_DATA_ITEM_CODE,
                                 '回收低压蒸汽_焦炉'    AS LCA_DATA_ITEM_NAME,
                                 SUM(VALUE)             AS VALUE,
                                 UNIT,
                                 ''                     AS INDEX_CODE,
                                 COMPANY_CODE,
                                 ''                     AS REC_CREATE_TIME,
                                 ''                     AS REC_CREATOR,
                                 ''                     AS MAT_STATUS,
                                 ''                     AS WG_PRODUCT_CODE
                          FROM CO_DATA
                          WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                            AND LCA_DATA_ITEM_CODE = 'HJ280'
                          GROUP BY BATCH_NUMBER, START_YM, END_YM, UNIT, COMPANY_CODE),
     CO_STEAM2_OUTPUT AS (SELECT HEX(RAND())            AS REC_ID,
                                 BATCH_NUMBER,
                                 START_YM,
                                 END_YM,
                                 'COZQ2'                AS LCA_PROC_CODE,
                                 '炼铁厂-焦炉-高压蒸汽' AS LCA_PROC_NAME,
                                 'HJ270'                AS PRODUCT_CODE,
                                 '回收高压蒸汽_焦炉'    AS PRODUCT_NAME,
                                 '04'                   AS LCA_DATA_ITEM_CAT_CODE,
                                 '产品'                 AS LCA_DATA_ITEM_CAT_NAME,
                                 'HJ270'                AS LCA_DATA_ITEM_CODE,
                                 '回收高压蒸汽_焦炉'    AS LCA_DATA_ITEM_NAME,
                                 SUM(VALUE)             AS VALUE,
                                 UNIT,
                                 ''                     AS INDEX_CODE,
                                 COMPANY_CODE,
                                 ''                     AS REC_CREATE_TIME,
                                 ''                     AS REC_CREATOR,
                                 ''                     AS MAT_STATUS,
                                 ''                     AS WG_PRODUCT_CODE
                          FROM CO_DATA
                          WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                            AND LCA_DATA_ITEM_CODE = 'HJ270'
                          GROUP BY BATCH_NUMBER, START_YM, END_YM, UNIT, COMPANY_CODE),
     CO_STEAM3_OUTPUT AS (SELECT HEX(RAND())            AS REC_ID,
                                 BATCH_NUMBER,
                                 START_YM,
                                 END_YM,
                                 'COZQ3'                AS LCA_PROC_CODE,
                                 '炼铁厂-焦炉-中压蒸汽' AS LCA_PROC_NAME,
                                 'HJ290'                AS PRODUCT_CODE,
                                 '回收中压蒸汽_焦炉'    AS PRODUCT_NAME,
                                 '04'                   AS LCA_DATA_ITEM_CAT_CODE,
                                 '产品'                 AS LCA_DATA_ITEM_CAT_NAME,
                                 'HJ290'                AS LCA_DATA_ITEM_CODE,
                                 '回收中压蒸汽_焦炉'    AS LCA_DATA_ITEM_NAME,
                                 SUM(VALUE)             AS VALUE,
                                 UNIT,
                                 ''                     AS INDEX_CODE,
                                 COMPANY_CODE,
                                 ''                     AS REC_CREATE_TIME,
                                 ''                     AS REC_CREATOR,
                                 ''                     AS MAT_STATUS,
                                 ''                     AS WG_PRODUCT_CODE
                          FROM CO_DATA
                          WHERE LCA_DATA_ITEM_CAT_CODE = '05'
                            AND LCA_DATA_ITEM_CODE = 'HJ290'
                          GROUP BY BATCH_NUMBER, START_YM, END_YM, UNIT, COMPANY_CODE),
     CO_STEAM1_INPUT AS (SELECT HEX(RAND())                                 AS REC_ID,
                                BATCH_NUMBER,
                                START_YM,
                                END_YM,
                                'COZQ1'                                     AS LCA_PROC_CODE,
                                '炼铁厂-焦炉-低压蒸汽'                      AS LCA_PROC_NAME,
                                'HJ280'                                     AS PRODUCT_CODE,
                                '回收低压蒸汽_焦炉'                         AS PRODUCT_NAME,
                                LCA_DATA_ITEM_CAT_CODE,
                                LCA_DATA_ITEM_CAT_NAME,
                                LCA_DATA_ITEM_CODE,
                                LCA_DATA_ITEM_NAME,
                                SUM(VALUE * 0.0401 * (SELECT VALUE FROM CO_STEAM1_OUTPUT) /
                                    ((SELECT VALUE FROM CO_STEAM1_OUTPUT) +
                                     (SELECT VALUE FROM CO_STEAM2_OUTPUT) +
                                     (SELECT VALUE FROM CO_STEAM3_OUTPUT))) AS VALUE,
                                UNIT,
                                ''                                          AS INDEX_CODE,
                                COMPANY_CODE,
                                ''                                          AS REC_CREATE_TIME,
                                ''                                          AS REC_CREATOR,
                                ''                                          AS MAT_STATUS,
                                ''                                          AS WG_PRODUCT_CODE
                         FROM CO_DIST
                         GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                                  LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     CO_STEAM2_INPUT AS (SELECT HEX(RAND())                                 AS REC_ID,
                                BATCH_NUMBER,
                                START_YM,
                                END_YM,
                                'COZQ2'                                     AS LCA_PROC_CODE,
                                '炼铁厂-焦炉-高压蒸汽'                      AS LCA_PROC_NAME,
                                'HJ270'                                     AS PRODUCT_CODE,
                                '回收高压蒸汽_焦炉'                         AS PRODUCT_NAME,
                                LCA_DATA_ITEM_CAT_CODE,
                                LCA_DATA_ITEM_CAT_NAME,
                                LCA_DATA_ITEM_CODE,
                                LCA_DATA_ITEM_NAME,
                                SUM(VALUE * 0.0401 * (SELECT VALUE FROM CO_STEAM2_OUTPUT) /
                                    ((SELECT VALUE FROM CO_STEAM1_OUTPUT) +
                                     (SELECT VALUE FROM CO_STEAM2_OUTPUT) +
                                     (SELECT VALUE FROM CO_STEAM3_OUTPUT))) AS VALUE,
                                UNIT,
                                ''                                          AS INDEX_CODE,
                                COMPANY_CODE,
                                ''                                          AS REC_CREATE_TIME,
                                ''                                          AS REC_CREATOR,
                                ''                                          AS MAT_STATUS,
                                ''                                          AS WG_PRODUCT_CODE
                         FROM CO_DIST
                         GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                                  LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     CO_STEAM3_INPUT AS (SELECT HEX(RAND())                                 AS REC_ID,
                                BATCH_NUMBER,
                                START_YM,
                                END_YM,
                                'COZQ3'                                     AS LCA_PROC_CODE,
                                '炼铁厂-焦炉-中压蒸汽'                      AS LCA_PROC_NAME,
                                'HJ290'                                     AS PRODUCT_CODE,
                                '回收中压蒸汽_焦炉'                         AS PRODUCT_NAME,
                                LCA_DATA_ITEM_CAT_CODE,
                                LCA_DATA_ITEM_CAT_NAME,
                                LCA_DATA_ITEM_CODE,
                                LCA_DATA_ITEM_NAME,
                                SUM(VALUE * 0.0401 * (SELECT VALUE FROM CO_STEAM3_OUTPUT) /
                                    ((SELECT VALUE FROM CO_STEAM1_OUTPUT) +
                                     (SELECT VALUE FROM CO_STEAM2_OUTPUT) +
                                     (SELECT VALUE FROM CO_STEAM3_OUTPUT))) AS VALUE,
                                UNIT,
                                ''                                          AS INDEX_CODE,
                                COMPANY_CODE,
                                ''                                          AS REC_CREATE_TIME,
                                ''                                          AS REC_CREATOR,
                                ''                                          AS MAT_STATUS,
                                ''                                          AS WG_PRODUCT_CODE
                         FROM CO_DIST
                         GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
                                  LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     CO_STEAM1_DATA AS (SELECT * FROM CO_STEAM1_OUTPUT UNION SELECT * FROM CO_STEAM1_INPUT),
     CO_STEAM2_DATA AS (SELECT * FROM CO_STEAM2_OUTPUT UNION SELECT * FROM CO_STEAM2_INPUT),
     CO_STEAM3_DATA AS (SELECT * FROM CO_STEAM3_OUTPUT UNION SELECT * FROM CO_STEAM3_INPUT),
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
     BOF_STEAM_OUTPUT AS (SELECT HEX(RAND())            AS REC_ID,
                                 BATCH_NUMBER,
                                 START_YM,
                                 END_YM,
                                 'LDZQ'                 AS LCA_PROC_CODE,
                                 '炼钢厂-转炉-低压蒸汽' AS LCA_PROC_NAME,
                                 'HZ280'                AS PRODUCT_CODE,
                                 '回收低压蒸汽_转炉'    AS PRODUCT_NAME,
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
                            AND LCA_DATA_ITEM_CODE = 'HZ280'
                          GROUP BY BATCH_NUMBER, START_YM, END_YM, LCA_DATA_ITEM_CODE,
                                   LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE),
     BOF_STEAM_INPUT AS (SELECT HEX(RAND())            AS REC_ID,
                                BATCH_NUMBER,
                                START_YM,
                                END_YM,
                                'LDZQ'                 AS LCA_PROC_CODE,
                                '炼钢厂-转炉-低压蒸汽' AS LCA_PROC_NAME,
                                'HZ280'                AS PRODUCT_CODE,
                                '回收低压蒸汽_转炉'    AS PRODUCT_NAME,
                                LCA_DATA_ITEM_CAT_CODE,
                                LCA_DATA_ITEM_CAT_NAME,
                                LCA_DATA_ITEM_CODE,
                                LCA_DATA_ITEM_NAME,
                                SUM(VALUE * 0.0036)    AS VALUE,
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
     BOF_STEAM_DATA AS (SELECT * FROM BOF_STEAM_INPUT UNION SELECT * FROM BOF_STEAM_OUTPUT),
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
                   FROM CO_STEAM1_DATA
                   UNION
                   SELECT *
                   FROM CO_STEAM2_DATA
                   UNION
                   SELECT *
                   FROM CO_STEAM3_DATA
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
                   FROM LDG_DATA
                   UNION
                   SELECT *
                   FROM BOF_STEAM_DATA)
SELECT HEX(RAND())                                  AS REC_ID,
       ''                                           AS BATCH_NUMBER,
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
       VALUE,
       UNIT,
       NULL                                         AS INDEX_CODE,
       COMPANY_CODE,
       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI') AS REC_CREATE_TIME,
       NULL                                         AS REC_CREATOR,
       MAT_STATUS,
       WG_PRODUCT_CODE
FROM DATA_CONS;



