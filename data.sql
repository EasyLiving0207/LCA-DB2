INSERT INTO BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX (REC_ID, BATCH_NUMBER, START_YM, END_YM, COMPANY_CODE, PROC_KEY,
                                                       PROC_NAME, ITEM_CODE, ITEM_NAME, ITEM_CAT_NAME, SOURCE_PROC_KEY,
                                                       SOURCE_PROC_NAME, UNIT_COST, INV, REC_CREATE_TIME)
with v_batch_number as (SELECT batch_number
                        FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
                        WHERE COMPANY_CODE = 'TA'
                          AND FLAG = 'Y'
                          AND TIME_FLAG = 'Y'
                          AND YEAR = '2023'
                          AND MONTH = '01'
                          AND END_MONTH = '12'),
     DATA1 as (SELECT BATCH_NUMBER,
                      START_YM,
                      END_YM,
                      'TA'                                             AS COMPANY_CODE,
                      LCA_PROC_CODE                                    AS PROC_CODE,
                      LCA_PROC_NAME                                    AS PROC_NAME,
                      CONCAT(CONCAT(LCA_PROC_CODE, '_'), PRODUCT_CODE) AS PROC_KEY,
                      PRODUCT_CODE,
                      PRODUCT_NAME,
                      LCA_DATA_ITEM_CAT_NAME                           AS ITEM_CAT_NAME,
                      LCA_DATA_ITEM_CODE                               AS ITEM_CODE,
                      LCA_DATA_ITEM_NAME                               AS ITEM_NAME,
                      CASE
                          WHEN UNIT = '万度' THEN VALUE * 10000
                          WHEN UNIT = '吨' THEN VALUE * 1000
                          WHEN UNIT = '千立方米' THEN VALUE * 1000
                          ELSE VALUE
                          END
                                                                       AS VALUE
               FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
               WHERE COMPANY_CODE = 'TA'
                 AND BATCH_NUMBER in (select * from v_batch_number)),
     PROC_PRODUCT_LIST AS (SELECT DISTINCT PROC_KEY,
                                           PROC_NAME,
                                           PRODUCT_CODE,
                                           PRODUCT_NAME,
                                           VALUE
                           FROM DATA1
                           WHERE ITEM_CAT_NAME = '产品'),
     DATA AS (SELECT BATCH_NUMBER,
                     START_YM,
                     END_YM,
                     COMPANY_CODE,
                     A.PROC_KEY,
                     PROC_CODE,
                     A.PROC_NAME,
                     A.PRODUCT_CODE,
                     A.PRODUCT_NAME,
                     ITEM_CAT_NAME,
                     ITEM_CODE,
                     ITEM_NAME,
                     A.VALUE,
                     B.VALUE                                                           AS PRODUCT_VALUE,
                     CAST(A.VALUE AS DECIMAL(20, 4)) / CAST(B.VALUE AS DECIMAL(20, 4)) AS UNIT_COST
              FROM DATA1 A
                       JOIN PROC_PRODUCT_LIST B
                            ON A.PROC_KEY = B.PROC_KEY),
     RESOURCE as (select *
                  FROM DATA
                  WHERE ITEM_CAT_NAME IN ('原材料', '能源', '辅助材料')),
     MATRIX AS (SELECT HEX(RAND())                                  AS REC_ID,
                       BATCH_NUMBER,
                       START_YM,
                       END_YM,
                       COMPANY_CODE,
                       A.PROC_KEY,
                       A.PROC_NAME,
                       A.ITEM_CODE,
                       A.ITEM_NAME,
                       A.ITEM_CAT_NAME,
                       B.PROC_KEY                                   AS SOURCE_PROC_KEY,
                       B.PROC_NAME                                  AS SOURCE_PROC_NAME,
                       A.UNIT_COST,
                       'N'                                          AS INV,
                       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI') as REC_CREATE_TIME
                FROM RESOURCE A
                         INNER JOIN PROC_PRODUCT_LIST B ON A.ITEM_CODE = B.PRODUCT_CODE)
SELECT *
FROM MATRIX
ORDER BY PROC_KEY, SOURCE_PROC_KEY;

delete from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX where REC_CREATE_TIME = '202410250907';

SELECT * FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX;

