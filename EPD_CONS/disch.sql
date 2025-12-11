WITH DATA AS (SELECT REC_ID,
                     BATCH_NUMBER,
                     START_YM,
                     END_YM,
                     COMPANY_CODE,
                     CONCAT(CONCAT(LCA_PROC_CODE, '_'), PRODUCT_CODE) AS PROC_KEY,
                     LCA_PROC_CODE                                    AS PROC_CODE,
                     LCA_PROC_NAME                                    AS PROC_NAME,
                     PRODUCT_CODE,
                     PRODUCT_NAME,
                     LCA_DATA_ITEM_CAT_CODE                           AS ITEM_CAT_CODE,
                     LCA_DATA_ITEM_CAT_NAME                           AS ITEM_CAT_NAME,
                     LCA_DATA_ITEM_CODE                               AS ITEM_CODE,
                     LCA_DATA_ITEM_NAME                               AS ITEM_NAME,
                     CASE
                         WHEN UNIT = '万度' THEN VALUE * 10000
                         WHEN UNIT = '吨' THEN VALUE * 1000
                         WHEN UNIT = '千立方米' THEN VALUE * 1000
                         ELSE VALUE
                         END
                                                                      AS VALUE,
                     CASE
                         WHEN UNIT = '万度' THEN '度'
                         WHEN UNIT = '吨' THEN '千克'
                         WHEN UNIT = '千立方米' THEN '立方米'
                         ELSE UNIT
                         END
                                                                      AS UNIT
              FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA_0002_DT
              WHERE COMPANY_CODE = 'TA'
                AND BATCH_NUMBER IN ('20240120241220251118YS')),
     DISCH_FACTOR AS (SELECT *
                      FROM T_ADS_WH_LCA_MAT_DATA
                      WHERE ORG_CODE = 'TA'
                        AND START_TIME = '2025'),
     DISCH_DATA AS (SELECT A.*, B.DISCH_COEFF
                    FROM DATA A
                             JOIN DISCH_FACTOR B ON A.ITEM_CODE = B.ITEM_CODE),
     DISCH_RESULT AS (SELECT *,
                             CASE
                                 WHEN ITEM_CAT_CODE < '04' THEN VALUE * DISCH_COEFF
                                 ELSE - VALUE * DISCH_COEFF END AS DISCH_VALUE
                      FROM DISCH_DATA
                      ORDER BY PROC_KEY, ITEM_CAT_CODE, ITEM_CODE),
     DISCH_SUM AS (SELECT BATCH_NUMBER,
                          START_YM,
                          END_YM,
                          COMPANY_CODE,
                          PROC_KEY,
                          PROC_CODE,
                          PROC_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          SUM(DISCH_VALUE) AS DISCH_VALUE
                   FROM DISCH_RESULT
                   GROUP BY BATCH_NUMBER,
                            START_YM,
                            END_YM,
                            COMPANY_CODE,
                            PROC_KEY,
                            PROC_CODE,
                            PROC_NAME,
                            PRODUCT_CODE,
                            PRODUCT_NAME
                   ORDER BY PROC_KEY)
SELECT *
FROM DISCH_SUM






