select *
FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX;

WITH v_batch_number as (SELECT batch_number
                        FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
                        WHERE COMPANY_CODE = 'TA'
                          AND FLAG = 'Y'
                          AND TIME_FLAG = 'Y'
                          AND YEAR = '2023'
                          AND MONTH = '01'
                          AND END_MONTH = '12'),
     MATRIX_INV AS (SELECT PROC_KEY,
                           SOURCE_PROC_KEY,
                           CAST(UNIT_COST AS DOUBLE) AS UNIT_COST
                    FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX
                    WHERE BATCH_NUMBER IN (SELECT * FROM v_batch_number)
                      AND COMPANY_CODE = 'TA'
                      AND INV = 'Y'),
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
                                           PROC_CODE,
                                           PROC_NAME,
                                           ITEM_CODE AS PRODUCT_CODE,
                                           ITEM_NAME AS PRODUCT_NAME,
                                           VALUE
                           FROM DATA1
                           WHERE ITEM_CAT_NAME = '产品'),
     DATA AS (SELECT BATCH_NUMBER,
                     START_YM,
                     END_YM,
                     COMPANY_CODE,
                     A.PROC_KEY,
                     A.PROC_CODE,
                     A.PROC_NAME,
                     A.PRODUCT_CODE,
                     A.PRODUCT_NAME,
                     ITEM_CAT_NAME,
                     ITEM_CODE,
                     ITEM_NAME,
                     A.VALUE,
                     B.VALUE                                           AS PRODUCT_VALUE,
                     CAST(A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE) AS UNIT_COST
              FROM DATA1 A
                       JOIN PROC_PRODUCT_LIST B
                            ON A.PROC_KEY = B.PROC_KEY),
     FACTOR AS (SELECT *
                FROM (SELECT DISTINCT a.ITEM_CODE,
                                      a.ITEM_NAME,
                                      b.DISCH_COEFF,--直接排放系数
                                      b.DISCH_COEFF_UNIT,
                                      d.GWP,--间接排放系数
                                      d.DATA_ITEM_NAME,
                                      d.BACKGROUND_DATA,
                                      d.SOURCE,
                                      e.CUSTOMS_TRANS_VALUE, --海运运输距离
                                      e.TRAIN_TRANS_VALUE, --火车运输距离
                                      e.TRUCK_CAR_TRANS_VALUE,--卡车运输距离
                                      e.RIVER_CAR_TRANS_VALUE --河运运输距离
                      FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM DATA) a
                               LEFT JOIN
                           (SELECT DISTINCT *
                            FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                            WHERE ORG_CODE = 'TA'
                              AND START_TIME >= '2023'
                              AND END_TIME <= '2023') b ON a.ITEM_CODE = b.ITEM_CODE
                               LEFT JOIN
                           (SELECT *
                            FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                            WHERE BASE_CODE = 'TA'
                              AND START_TIME >= '2023'
                              AND END_TIME <= '2023') c ON a.ITEM_CODE = c.DATA_CODE
                               LEFT JOIN
                           (SELECT *
                            FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES
                            WHERE COMPANY_CODE = 'TA'
                              AND START_TIME >= '2023'
                              AND END_TIME <= '2023') d ON c.UUID = d.UUID
                               LEFT JOIN
                           (SELECT *
                            FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                            WHERE COMPANY_CODE = 'TA'
                              AND START_TIME >= '2023'
                              AND END_TIME <= '2023') e
                           ON a.ITEM_CODE = e.LCA_DATA_ITEM_CODE)),
     FACTOR_TRANSPORT AS (SELECT B.ITEM_CODE,
                                 CAST(SUM(B.FACTOR_TRANSPORT) AS DOUBLE) AS FACTOR_TRANSPORT
                          FROM FACTOR A,
                               TABLE
                               (VALUES (A.ITEM_CODE, A.CUSTOMS_TRANS_VALUE / 1000 * 0.00667220376406485),
                                       (A.ITEM_CODE, A.TRAIN_TRANS_VALUE / 1000 * 0.0438528213459938),
                                       (A.ITEM_CODE, A.TRUCK_CAR_TRANS_VALUE / 1000 * 0.138092140707126),
                                       (A.ITEM_CODE, A.RIVER_CAR_TRANS_VALUE / 1000 * 0.0487214598443683))
                                   AS B(ITEM_CODE, FACTOR_TRANSPORT)
                          WHERE B.FACTOR_TRANSPORT IS NOT NULL
                          GROUP BY B.ITEM_CODE),
     FACTOR_DIRECT AS (SELECT ITEM_CODE,
                              CAST(DISCH_COEFF AS DOUBLE) AS FACTOR_DIRECT
                       FROM FACTOR
                       WHERE DISCH_COEFF IS NOT NULL
                         AND DISCH_COEFF != 0),
     FACTOR_INDIRECT AS (SELECT ITEM_CODE,
                                CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
                         FROM FACTOR
                         WHERE GWP IS NOT NULL
                           AND GWP != 0),
     RESOURCE as (SELECT *
                  FROM DATA
                  WHERE ITEM_CAT_NAME IN ('原材料', '能源', '辅助材料')),
     PRODUCT_BY_PRODUCT AS (SELECT *
                            FROM DATA
                            WHERE ITEM_CAT_NAME IN ('产品', '副产品', '固废')),
     BY_PRODUCT AS (select *
                    FROM DATA
                    WHERE ITEM_CAT_NAME IN ('副产品', '固废')),
     RECYCLED_ITEM AS (SELECT DISTINCT ITEM_CODE
                       FROM RESOURCE
                       INTERSECT
                       SELECT DISTINCT ITEM_CODE
                       FROM BY_PRODUCT),
     OUTSITE_ITEM AS (SELECT DISTINCT ITEM_CODE
                      FROM RESOURCE
                      EXCEPT
                      SELECT DISTINCT ITEM_CODE
                      FROM BY_PRODUCT),
     RESOURCE_DIST AS (SELECT A.PROC_KEY                AS SOURCE_PROC_KEY,
                              A.PROC_CODE               AS SOURCE_PROC_CODE,
                              A.PROC_NAME               AS SOURCE_PROC_NAME,
                              ITEM_CAT_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              B.PROC_KEY                AS TARGET_PROC_KEY,
                              C.PRODUCT_CODE            AS TARGET_PRODUCT_CODE,
                              C.PRODUCT_NAME            AS TARGET_PRODUCT_NAME,
                              A.UNIT_COST * B.UNIT_COST AS COST
                       FROM RESOURCE A
                                CROSS JOIN MATRIX_INV B
                                LEFT JOIN PROC_PRODUCT_LIST C ON B.PROC_KEY = C.PROC_KEY
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     PRODUCT_BY_PRODUCT_DIST AS (SELECT A.PROC_KEY                AS SOURCE_PROC_KEY,
                                        A.PROC_CODE               AS SOURCE_PROC_CODE,
                                        A.PROC_NAME               AS SOURCE_PROC_NAME,
                                        ITEM_CAT_NAME,
                                        ITEM_CODE,
                                        ITEM_NAME,
                                        B.PROC_KEY                AS TARGET_PROC_KEY,
                                        C.PRODUCT_CODE            AS TARGET_PRODUCT_CODE,
                                        C.PRODUCT_NAME            AS TARGET_PRODUCT_NAME,
                                        A.UNIT_COST * B.UNIT_COST AS COST
                                 FROM PRODUCT_BY_PRODUCT A
                                          CROSS JOIN MATRIX_INV B
                                          LEFT JOIN PROC_PRODUCT_LIST C ON B.PROC_KEY = C.PROC_KEY
                                 WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     BY_PRODUCT_DIST AS (SELECT A.PROC_KEY                AS SOURCE_PROC_KEY,
                                A.PROC_CODE               AS SOURCE_PROC_CODE,
                                A.PROC_NAME               AS SOURCE_PROC_NAME,
                                ITEM_CAT_NAME,
                                ITEM_CODE,
                                ITEM_NAME,
                                B.PROC_KEY                AS TARGET_PROC_KEY,
                                C.PRODUCT_CODE            AS TARGET_PRODUCT_CODE,
                                C.PRODUCT_NAME            AS TARGET_PRODUCT_NAME,
                                A.UNIT_COST * B.UNIT_COST AS COST
                         FROM BY_PRODUCT A
                                  CROSS JOIN MATRIX_INV B
                                  LEFT JOIN PROC_PRODUCT_LIST C ON B.PROC_KEY = C.PROC_KEY
                         WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     DIRECT_POS AS (SELECT SOURCE_PROC_KEY,
                           SOURCE_PROC_CODE,
                           SOURCE_PROC_NAME,
                           ITEM_CAT_NAME,
                           A.ITEM_CODE,
                           ITEM_NAME,
                           TARGET_PROC_KEY,
                           TARGET_PRODUCT_CODE,
                           TARGET_PRODUCT_NAME,
                           'C1'                     AS LOAD_TYPE,
                           A.COST * B.FACTOR_DIRECT AS LOAD
                    FROM RESOURCE_DIST A
                             INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     DIRECT_NEG AS (SELECT SOURCE_PROC_KEY,
                           SOURCE_PROC_CODE,
                           SOURCE_PROC_NAME,
                           ITEM_CAT_NAME,
                           A.ITEM_CODE,
                           ITEM_NAME,
                           TARGET_PROC_KEY,
                           TARGET_PRODUCT_CODE,
                           TARGET_PRODUCT_NAME,
                           'C1'                     AS LOAD_TYPE,
                           - COST * B.FACTOR_DIRECT AS LOAD
                    FROM PRODUCT_BY_PRODUCT_DIST A
                             INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     DIRECT_POS_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS DIRECT_POS
                        FROM DIRECT_POS
                        GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
     DIRECT_NEG_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS DIRECT_NEG
                        FROM DIRECT_NEG
                        GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
     C2 AS (SELECT SOURCE_PROC_KEY,
                   SOURCE_PROC_CODE,
                   SOURCE_PROC_NAME,
                   ITEM_CAT_NAME,
                   A.ITEM_CODE,
                   ITEM_NAME,
                   TARGET_PROC_KEY,
                   TARGET_PRODUCT_CODE,
                   TARGET_PRODUCT_NAME,
                   'C2'                     AS LOAD_TYPE,
                   COST * B.FACTOR_INDIRECT AS LOAD
            FROM RESOURCE_DIST A
                     INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE
            WHERE A.ITEM_CODE IN (SELECT * FROM RECYCLED_ITEM)),
     C2_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS C2
                FROM C2
                GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
     C3 AS (SELECT SOURCE_PROC_KEY,
                   SOURCE_PROC_CODE,
                   SOURCE_PROC_NAME,
                   ITEM_CAT_NAME,
                   A.ITEM_CODE,
                   ITEM_NAME,
                   TARGET_PROC_KEY,
                   TARGET_PRODUCT_CODE,
                   TARGET_PRODUCT_NAME,
                   'C3'                     AS LOAD_TYPE,
                   COST * B.FACTOR_INDIRECT AS LOAD
            FROM RESOURCE_DIST A
                     INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE
            WHERE A.ITEM_CODE IN (SELECT * FROM OUTSITE_ITEM)),
     C3_AGG AS (SELECT TARGET_PROC_KEY,
                       TARGET_PRODUCT_CODE,
                       TARGET_PRODUCT_NAME,
                       SUM(LOAD) AS C3
                FROM C3
                GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
     C4 AS (SELECT SOURCE_PROC_KEY,
                   SOURCE_PROC_CODE,
                   SOURCE_PROC_NAME,
                   ITEM_CAT_NAME,
                   A.ITEM_CODE,
                   ITEM_NAME,
                   TARGET_PROC_KEY,
                   TARGET_PRODUCT_CODE,
                   TARGET_PRODUCT_NAME,
                   'C4'                      AS LOAD_TYPE,
                   -COST * B.FACTOR_INDIRECT AS LOAD
            FROM BY_PRODUCT_DIST A
                     INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     C4_AGG AS (SELECT TARGET_PROC_KEY,
                       TARGET_PRODUCT_CODE,
                       TARGET_PRODUCT_NAME,
                       SUM(LOAD) AS C4
                FROM C4
                GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
     C5 AS (SELECT SOURCE_PROC_KEY,
                   SOURCE_PROC_CODE,
                   SOURCE_PROC_NAME,
                   ITEM_CAT_NAME,
                   A.ITEM_CODE,
                   ITEM_NAME,
                   TARGET_PROC_KEY,
                   TARGET_PRODUCT_CODE,
                   TARGET_PRODUCT_NAME,
                   'C5'                      AS LOAD_TYPE,
                   COST * B.FACTOR_TRANSPORT AS LOAD
            FROM RESOURCE_DIST A
                     INNER JOIN FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE
            WHERE A.ITEM_CODE IN (SELECT * FROM OUTSITE_ITEM)),
     C5_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS C5
                FROM C5
                GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
     RESULT AS (SELECT PROC_KEY,
                       PROC_CODE,
                       PROC_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       '全球变暖潜力(GWP100):合计'                                         AS LCI_ELEMENT_NAME,
                       COALESCE(DIRECT_POS, 0)                                             AS DIRECT_POS,
                       COALESCE(DIRECT_NEG, 0)                                             AS DIRECT_NEG,
                       COALESCE(DIRECT_POS, 0) + COALESCE(DIRECT_NEG, 0)                   AS C1_DIRECT,
                       COALESCE(C2, 0)                                                     AS C2_BP,
                       COALESCE(C3, 0)                                                     AS C3_OUT,
                       COALESCE(C4, 0)                                                     AS C4_BP_NEG,
                       COALESCE(C5, 0)                                                     AS C5_TRANS,
                       COALESCE(DIRECT_POS, 0) + COALESCE(DIRECT_NEG, 0) + COALESCE(C2, 0) AS C_INSITE,
                       COALESCE(C3, 0) + COALESCE(C4, 0) + COALESCE(C5, 0)                 AS C_OUTSITE,
                       COALESCE(DIRECT_POS, 0) + COALESCE(DIRECT_NEG, 0) + COALESCE(C2, 0) +
                       COALESCE(C3, 0) + COALESCE(C4, 0) + COALESCE(C5, 0)                 AS C_CYCLE
                FROM PROC_PRODUCT_LIST A
                         LEFT JOIN DIRECT_POS_AGG DIRECT_POS ON A.PROC_KEY = DIRECT_POS.TARGET_PROC_KEY
                         LEFT JOIN DIRECT_NEG_AGG DIRECT_NEG ON A.PROC_KEY = DIRECT_NEG.TARGET_PROC_KEY
                         LEFT JOIN C2_AGG C2 ON A.PROC_KEY = C2.TARGET_PROC_KEY
                         LEFT JOIN C3_AGG C3 ON A.PROC_KEY = C3.TARGET_PROC_KEY
                         LEFT JOIN C4_AGG C4 ON A.PROC_KEY = C4.TARGET_PROC_KEY
                         LEFT JOIN C5_AGG C5 ON A.PROC_KEY = C5.TARGET_PROC_KEY
                ORDER BY PROC_KEY),
     RESULT_DIST AS (SELECT *
                     FROM DIRECT_POS
                     UNION
                     SELECT *
                     FROM DIRECT_NEG
                     UNION
                     SELECT *
                     FROM C2
                     UNION
                     SELECT *
                     FROM C3
                     UNION
                     SELECT *
                     FROM C4
                     UNION
                     SELECT *
                     FROM C5)
select * from DATA
left join FACTOR on DATA.ITEM_CODE = FACTOR.ITEM_CODE;
-- SELECT A.*, COALESCE(B.C_CYCLE, C.FACTOR_INDIRECT) AS FACTOR, COALESCE(A.UNIT_COST * B.C_CYCLE, A.UNIT_COST * C.FACTOR_INDIRECT) AS LOAD
-- FROM (select *
--       from DATA
--       where PRODUCT_CODE like 'DL%'
--         AND ITEM_CAT_NAME = '能源') A
--          left join RESULT B on A.ITEM_CODE = B.PRODUCT_CODE
--          left join FACTOR_INDIRECT C on A.ITEM_CODE = C.ITEM_CODE;
-- SELECT *
-- from RESULT_DIST
-- where TARGET_PRODUCT_CODE like 'DL%'
--   AND LOAD != 0
-- ORDER BY LOAD_TYPE, ABS(LOAD) DESC;


WITH v_batch_number as (SELECT batch_number
                        FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
                        WHERE COMPANY_CODE = 'TA'
                          AND FLAG = 'Y'
                          AND TIME_FLAG = 'Y'
                          AND YEAR = '2023'
                          AND MONTH = '01'
                          AND END_MONTH = '12')
SELECT DISTINCT a.ITEM_CODE,
                a.ITEM_NAME,
                b.DISCH_COEFF,
                d.GWP,
                e.CUSTOMS_TRANS_VALUE,
                e.TRAIN_TRANS_VALUE,
                e.TRUCK_CAR_TRANS_VALUE,
                e.RIVER_CAR_TRANS_VALUE
FROM (SELECT DISTINCT LCA_DATA_ITEM_CODE as ITEM_CODE, LCA_DATA_ITEM_NAME as ITEM_NAME
      FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
      WHERE BATCH_NUMBER in (select * from v_batch_number)

      UNION ALL
      SELECT DISTINCT ITEM_CODE, ITEM_NAME
      FROM BG00MAC102.T_ADS_FACT_LCA_CR0001
      WHERE SUBSTR(REC_CREATE_TIME, 1, 4) = '2023'
      UNION
      SELECT DISTINCT ITEM_CODE, ITEM_NAME
      FROM BG00MAC102.T_ADS_FACT_LCA_HP0001
      WHERE SUBSTR(REC_CREATE_TIME, 1, 4) = '2023'
      UNION
      SELECT DISTINCT ITEM_CODE, ITEM_NAME
      FROM BG00MAC102.T_ADS_FACT_LCA_SI0001
      WHERE SUBSTR(REC_CREATE_TIME, 1, 4) = '2023')
         AS a

         LEFT JOIN
     (SELECT DISTINCT *
      FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
      WHERE org_code = 'TA'
        and start_time >= '2023'
        and '2023' >= end_time)
         AS b
     ON a.ITEM_CODE = b.ITEM_CODE

         LEFT JOIN
     (SELECT *
      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
      WHERE BASE_CODE = 'TA'
        and start_time >= '2023'
        and '2023' >= end_time)
         AS c
     ON a.ITEM_CODE = c.data_code

         LEFT JOIN
     (SELECT *
      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES
      WHERE COMPANY_CODE = 'TA'
        and start_time >= '2023'
        and '2023' >= end_time)
         AS d
     ON c.uuid = d.uuid

         LEFT JOIN
     (SELECT *
      FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
      WHERE COMPANY_CODE = 'TA'
        and start_time >= '2023'
        and '2023' >= end_time)
         AS e
     ON a.ITEM_CODE = e.LCA_DATA_ITEM_CODE;

SELECT *
FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA;

WITH MATRIX_INV AS (SELECT PROC_KEY, SOURCE_PROC_KEY, CAST(UNIT_COST AS DOUBLE) AS UNIT_COST
                    FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX
                    WHERE BATCH_NUMBER = '20230120231220240903YS'
                      AND COMPANY_CODE = 'TA'
                      AND INV = 'Y'),
     DATA1 as (SELECT BATCH_NUMBER,
                      START_YM,
                      END_YM,
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
                          ELSE VALUE END                               AS VALUE
               FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
               WHERE COMPANY_CODE = 'TA'
                 AND BATCH_NUMBER = '20230120231220240903YS'),
     PROC_PRODUCT_LIST AS (SELECT DISTINCT PROC_KEY,
                                           PROC_CODE,
                                           PROC_NAME,
                                           ITEM_CODE AS PRODUCT_CODE,
                                           ITEM_NAME AS PRODUCT_NAME,
                                           VALUE
                           FROM DATA1
                           WHERE ITEM_CAT_NAME = '产品'),
     DATA AS (SELECT BATCH_NUMBER,
                     START_YM,
                     END_YM,
                     A.PROC_KEY,
                     A.PROC_CODE,
                     A.PROC_NAME,
                     A.PRODUCT_CODE,
                     A.PRODUCT_NAME,
                     ITEM_CAT_NAME,
                     ITEM_CODE,
                     ITEM_NAME,
                     A.VALUE,
                     B.VALUE                                           AS PRODUCT_VALUE,
                     CAST(A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE) AS UNIT_COST
              FROM DATA1 A
                       JOIN PROC_PRODUCT_LIST B ON A.PROC_KEY = B.PROC_KEY),
     FACTOR AS (SELECT DISTINCT a.ITEM_CODE,
                                a.ITEM_NAME,
                                b.DISCH_COEFF,
                                d.GWP,
                                e.CUSTOMS_TRANS_VALUE,
                                e.TRAIN_TRANS_VALUE,
                                e.TRUCK_CAR_TRANS_VALUE,
                                e.RIVER_CAR_TRANS_VALUE
                FROM (SELECT DISTINCT LCA_DATA_ITEM_CODE as ITEM_CODE, LCA_DATA_ITEM_NAME as ITEM_NAME
                      FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
                      WHERE BATCH_NUMBER = '20230120231220240903YS'
                      UNION ALL
                      SELECT DISTINCT ITEM_CODE, ITEM_NAME
                      FROM BG00MAC102.T_ADS_FACT_LCA_CR0001
                      WHERE TRUE
                        AND SUBSTR(REC_CREATE_TIME, 1, 4) = '2023'
                      UNION
                      SELECT DISTINCT ITEM_CODE, ITEM_NAME
                      FROM BG00MAC102.T_ADS_FACT_LCA_HP0001
                      WHERE TRUE
                        AND SUBSTR(REC_CREATE_TIME, 1, 4) = '2023'
                      UNION
                      SELECT DISTINCT ITEM_CODE, ITEM_NAME
                      FROM BG00MAC102.T_ADS_FACT_LCA_SI0001
                      WHERE TRUE
                        AND SUBSTR(REC_CREATE_TIME, 1, 4) = '2023') AS a
                         LEFT JOIN (SELECT DISTINCT *
                                    FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                                    WHERE TRUE
                                      AND org_code = 'TA'
                                      AND start_time >= '2023'
                                      and '2023' >= end_time) AS b ON a.ITEM_CODE = b.ITEM_CODE
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                                    WHERE TRUE
                                      AND base_code = 'TA'
                                      AND start_time >= '2023'
                                      and '2023' >= end_time) AS c ON a.ITEM_CODE = c.data_code
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES
                                    WHERE company_code = 'TA'
                                      AND start_time >= '2023'
                                      and '2023' >= end_time) AS d ON c.uuid = d.uuid
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                                    WHERE TRUE
                                      AND company_code = 'TA'
                                      AND start_time >= '2023'
                                      and '2023' >= end_time) AS e ON a.ITEM_CODE = e.LCA_DATA_ITEM_CODE),
     FACTOR_TRANSPORT AS (SELECT B.ITEM_CODE, CAST(SUM(B.FACTOR_TRANSPORT) AS DOUBLE) AS FACTOR_TRANSPORT
                          FROM FACTOR A,
                               TABLE
                               (VALUES (A.ITEM_CODE, A.CUSTOMS_TRANS_VALUE / 1000 * 0.00667220376406485),
                                       (A.ITEM_CODE, A.TRAIN_TRANS_VALUE / 1000 * 0.0438528213459938),
                                       (A.ITEM_CODE, A.TRUCK_CAR_TRANS_VALUE / 1000 * 0.138092140707126),
                                       (A.ITEM_CODE, A.RIVER_CAR_TRANS_VALUE / 1000 *
                                                     0.0487214598443683)) AS B(ITEM_CODE, FACTOR_TRANSPORT)
                          WHERE B.FACTOR_TRANSPORT IS NOT NULL
                          GROUP BY B.ITEM_CODE),
     FACTOR_DIRECT AS (SELECT ITEM_CODE, CAST(DISCH_COEFF AS DOUBLE) AS FACTOR_DIRECT
                       FROM FACTOR
                       WHERE DISCH_COEFF IS NOT NULL
                         AND DISCH_COEFF != 0),
     FACTOR_INDIRECT AS (SELECT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
                         FROM FACTOR
                         WHERE GWP IS NOT NULL
                           AND GWP != 0),
     RESOURCE as (SELECT * FROM DATA WHERE ITEM_CAT_NAME IN ('原材料', '能源', '辅助材料')),
     PRODUCT_BY_PRODUCT AS (SELECT * FROM DATA WHERE ITEM_CAT_NAME IN ('产品', '副产品')),
     BY_PRODUCT AS (select * FROM DATA WHERE ITEM_CAT_NAME IN ('副产品')),
     RECYCLED_ITEM AS (SELECT DISTINCT ITEM_CODE FROM RESOURCE INTERSECT SELECT DISTINCT ITEM_CODE FROM BY_PRODUCT),
     OUTSITE_ITEM AS (SELECT DISTINCT ITEM_CODE FROM RESOURCE EXCEPT SELECT DISTINCT ITEM_CODE FROM BY_PRODUCT),
--      RESOURCE_DIST AS (SELECT A.PROC_KEY                AS SOURCE_PROC_KEY,
--                               A.PROC_CODE               AS SOURCE_PROC_CODE,
--                               A.PROC_NAME               AS SOURCE_PROC_NAME,
--                               ITEM_CAT_NAME,
--                               ITEM_CODE,
--                               ITEM_NAME,
--                               B.PROC_KEY                AS TARGET_PROC_KEY,
--                               C.PRODUCT_CODE            AS TARGET_PRODUCT_CODE,
--                               C.PRODUCT_NAME            AS TARGET_PRODUCT_NAME,
--                               A.UNIT_COST * B.UNIT_COST AS COST
--                        FROM RESOURCE A
--                                 CROSS JOIN MATRIX_INV B
--                                 LEFT JOIN PROC_PRODUCT_LIST C ON B.PROC_KEY = C.PROC_KEY
--                        WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
--      PRODUCT_BY_PRODUCT_DIST AS (SELECT A.PROC_KEY                AS SOURCE_PROC_KEY,
--                                         A.PROC_CODE               AS SOURCE_PROC_CODE,
--                                         A.PROC_NAME               AS SOURCE_PROC_NAME,
--                                         ITEM_CAT_NAME,
--                                         ITEM_CODE,
--                                         ITEM_NAME,
--                                         B.PROC_KEY                AS TARGET_PROC_KEY,
--                                         C.PRODUCT_CODE            AS TARGET_PRODUCT_CODE,
--                                         C.PRODUCT_NAME            AS TARGET_PRODUCT_NAME,
--                                         A.UNIT_COST * B.UNIT_COST AS COST
--                                  FROM PRODUCT_BY_PRODUCT A
--                                           CROSS JOIN MATRIX_INV B
--                                           LEFT JOIN PROC_PRODUCT_LIST C ON B.PROC_KEY = C.PROC_KEY
--                                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
--      BY_PRODUCT_DIST AS (SELECT A.PROC_KEY                AS SOURCE_PROC_KEY,
--                                 A.PROC_CODE               AS SOURCE_PROC_CODE,
--                                 A.PROC_NAME               AS SOURCE_PROC_NAME,
--                                 ITEM_CAT_NAME,
--                                 ITEM_CODE,
--                                 ITEM_NAME,
--                                 B.PROC_KEY                AS TARGET_PROC_KEY,
--                                 C.PRODUCT_CODE            AS TARGET_PRODUCT_CODE,
--                                 C.PRODUCT_NAME            AS TARGET_PRODUCT_NAME,
--                                 A.UNIT_COST * B.UNIT_COST AS COST
--                          FROM BY_PRODUCT A
--                                   CROSS JOIN MATRIX_INV B
--                                   LEFT JOIN PROC_PRODUCT_LIST C ON B.PROC_KEY = C.PROC_KEY
--                          WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C1_DIST AS (SELECT PROC_KEY,
                        A.ITEM_CODE,
                        A.UNIT_COST * B.FACTOR_DIRECT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE
                 UNION
                 SELECT PROC_KEY,
                        A.ITEM_CODE,
                        -A.UNIT_COST * B.FACTOR_DIRECT AS LOAD
                 FROM PRODUCT_BY_PRODUCT A
                          INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     C1_SUM AS (SELECT PROC_KEY,
                       SUM(LOAD) AS LOAD
                FROM C1_DIST
                GROUP BY PROC_KEY),
     C1_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C1_SUM A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C1_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                       SUM(COST)       AS C1
                FROM C1_CYCLE
                GROUP BY TARGET_PROC_KEY),
     C2_DIST AS (SELECT PROC_KEY,
                        A.ITEM_CODE,
                        A.UNIT_COST * B.FACTOR_INDIRECT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE
                 WHERE A.ITEM_CODE IN (SELECT * FROM RECYCLED_ITEM)),
     C2_SUM AS (SELECT PROC_KEY,
                       SUM(LOAD) AS LOAD
                FROM C2_DIST
                GROUP BY PROC_KEY),
     C2_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C2_SUM A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C2_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                       SUM(COST)       AS C2
                FROM C2_CYCLE
                GROUP BY TARGET_PROC_KEY),
     C3_DIST AS (SELECT PROC_KEY,
                        A.ITEM_CODE,
                        A.UNIT_COST * B.FACTOR_INDIRECT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE
                 WHERE A.ITEM_CODE IN (SELECT * FROM OUTSITE_ITEM)),
     C3_SUM AS (SELECT PROC_KEY,
                       SUM(LOAD) AS LOAD
                FROM C3_DIST
                GROUP BY PROC_KEY),
     C3_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C3_SUM A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C3_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                       SUM(COST)       AS C3
                FROM C3_CYCLE
                GROUP BY TARGET_PROC_KEY),
     C4_DIST AS (SELECT PROC_KEY,
                        A.ITEM_CODE,
                        -A.UNIT_COST * B.FACTOR_INDIRECT AS LOAD
                 FROM BY_PRODUCT A
                          INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     C4_SUM AS (SELECT PROC_KEY,
                       SUM(LOAD) AS LOAD
                FROM C4_DIST
                GROUP BY PROC_KEY),
     C4_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C4_SUM A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C4_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                       SUM(COST)       AS C4
                FROM C4_CYCLE
                GROUP BY TARGET_PROC_KEY),
     C5_DIST AS (SELECT PROC_KEY,
                        A.ITEM_CODE,
                        A.UNIT_COST * B.FACTOR_TRANSPORT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE
                 WHERE A.ITEM_CODE IN (SELECT * FROM OUTSITE_ITEM)),
     C5_SUM AS (SELECT PROC_KEY,
                       SUM(LOAD) AS LOAD
                FROM C5_DIST
                GROUP BY PROC_KEY),
     C5_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C5_SUM A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C5_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                       SUM(COST)       AS C5
                FROM C5_CYCLE
                GROUP BY TARGET_PROC_KEY),
     RESULT AS (SELECT HEX(RAND())                                         AS REC_ID,
                       '20230120231220240903YS'                            AS BATCH_NUMBER,
                       '202301'                                            AS START_YM,
                       '202312'                                            AS END_YM,
                       'TA'                                                AS COMPANY_CODE,
                       A.PROC_KEY,
                       PROC_CODE,
                       PROC_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       '全球变暖潜力(GWP100):合计'                         AS LCI_ELEMENT_NAME,
                       COALESCE(C1, 0)                                     AS C1_DIRECT,
                       COALESCE(C2, 0)                                     AS C2_BP,
                       COALESCE(C3, 0)                                     AS C3_OUT,
                       COALESCE(C4, 0)                                     AS C4_BP_NEG,
                       COALESCE(C5, 0)                                     AS C5_TRANS,
                       COALESCE(C1, 0) + COALESCE(C2, 0)                   AS C_INSITE,
                       COALESCE(C3, 0) + COALESCE(C4, 0) + COALESCE(C5, 0) AS C_OUTSITE,
                       COALESCE(C1, 0) + COALESCE(C2, 0) + COALESCE(C3, 0) +
                       COALESCE(C4, 0) + COALESCE(C5, 0)                   AS C_CYCLE,
                       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI')        AS REC_CREATE_TIME
                FROM PROC_PRODUCT_LIST A
                         LEFT JOIN C1_AGG C1 ON A.PROC_KEY = C1.PROC_KEY
                         LEFT JOIN C2_AGG C2 ON A.PROC_KEY = C2.PROC_KEY
                         LEFT JOIN C3_AGG C3 ON A.PROC_KEY = C3.PROC_KEY
                         LEFT JOIN C4_AGG C4 ON A.PROC_KEY = C4.PROC_KEY
                         LEFT JOIN C5_AGG C5 ON A.PROC_KEY = C5.PROC_KEY
                ORDER BY PROC_KEY)
SELECT *
FROM RESULT;

select *
from BG00MAC102.T_ADS_FACT_LCA_BIG_CLASS_CARBON_FOOT_MAIN


--      C2 AS (SELECT SOURCE_PROC_KEY,
--                    SOURCE_PROC_CODE,
--                    SOURCE_PROC_NAME,
--                    ITEM_CAT_NAME,
--                    A.ITEM_CODE,
--                    ITEM_NAME,
--                    TARGET_PROC_KEY,
--                    TARGET_PRODUCT_CODE,
--                    TARGET_PRODUCT_NAME,
--                    'C2'                     AS LOAD_TYPE,
--                    COST * B.FACTOR_INDIRECT AS LOAD
--             FROM RESOURCE_DIST A
--                      INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE
--             WHERE A.ITEM_CODE IN (SELECT * FROM RECYCLED_ITEM)),
--      C2_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS C2
--                 FROM C2
--                 GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
--      C3 AS (SELECT SOURCE_PROC_KEY,
--                    SOURCE_PROC_CODE,
--                    SOURCE_PROC_NAME,
--                    ITEM_CAT_NAME,
--                    A.ITEM_CODE,
--                    ITEM_NAME,
--                    TARGET_PROC_KEY,
--                    TARGET_PRODUCT_CODE,
--                    TARGET_PRODUCT_NAME,
--                    'C3'                     AS LOAD_TYPE,
--                    COST * B.FACTOR_INDIRECT AS LOAD
--             FROM RESOURCE_DIST A
--                      INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE
--             WHERE A.ITEM_CODE IN (SELECT * FROM OUTSITE_ITEM)),
--      C3_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS C3
--                 FROM C3
--                 GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
--      C4 AS (SELECT SOURCE_PROC_KEY,
--                    SOURCE_PROC_CODE,
--                    SOURCE_PROC_NAME,
--                    ITEM_CAT_NAME,
--                    A.ITEM_CODE,
--                    ITEM_NAME,
--                    TARGET_PROC_KEY,
--                    TARGET_PRODUCT_CODE,
--                    TARGET_PRODUCT_NAME,
--                    'C4'                      AS LOAD_TYPE,
--                    -COST * B.FACTOR_INDIRECT AS LOAD
--             FROM BY_PRODUCT_DIST A
--                      INNER JOIN FACTOR_INDIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
--      C4_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS C4
--                 FROM C4
--                 GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
--      C5 AS (SELECT SOURCE_PROC_KEY,
--                    SOURCE_PROC_CODE,
--                    SOURCE_PROC_NAME,
--                    ITEM_CAT_NAME,
--                    A.ITEM_CODE,
--                    ITEM_NAME,
--                    TARGET_PROC_KEY,
--                    TARGET_PRODUCT_CODE,
--                    TARGET_PRODUCT_NAME,
--                    'C5'                      AS LOAD_TYPE,
--                    COST * B.FACTOR_TRANSPORT AS LOAD
--             FROM RESOURCE_DIST A
--                      INNER JOIN FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE
--             WHERE A.ITEM_CODE IN (SELECT * FROM OUTSITE_ITEM)),
--      C5_AGG AS (SELECT TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME, SUM(LOAD) AS C5
--                 FROM C5
--                 GROUP BY TARGET_PROC_KEY, TARGET_PRODUCT_CODE, TARGET_PRODUCT_NAME),
--      RESULT AS (SELECT HEX(RAND())                                                         AS REC_ID,
--                        '20230120231220240903YS'                                            AS BATCH_NUMBER,
--                        '202301'                                                            AS START_YM,
--                        '202312'                                                            AS END_YM,
--                        'TA'                                                                AS COMPANY_CODE,
--                        PROC_KEY,
--                        PROC_CODE,
--                        PROC_NAME,
--                        PRODUCT_CODE,
--                        PRODUCT_NAME,
--                        '全球变暖潜力(GWP100):合计'                                         AS LCI_ELEMENT_NAME,
--                        COALESCE(DIRECT_POS, 0)                                             AS DIRECT_POS,
--                        COALESCE(DIRECT_NEG, 0)                                             AS DIRECT_NEG,
--                        COALESCE(DIRECT_POS, 0) + COALESCE(DIRECT_NEG, 0)                   AS C1_DIRECT,
--                        COALESCE(C2, 0)                                                     AS C2_BP,
--                        COALESCE(C3, 0)                                                     AS C3_OUT,
--                        COALESCE(C4, 0)                                                     AS C4_BP_NEG,
--                        COALESCE(C5, 0)                                                     AS C5_TRANS,
--                        COALESCE(DIRECT_POS, 0) + COALESCE(DIRECT_NEG, 0) + COALESCE(C2, 0) AS C_INSITE,
--                        COALESCE(C3, 0) + COALESCE(C4, 0) + COALESCE(C5, 0)                 AS C_OUTSITE,
--                        COALESCE(DIRECT_POS, 0) + COALESCE(DIRECT_NEG, 0) + COALESCE(C2, 0) + COALESCE(C3, 0) +
--                        COALESCE(C4, 0) + COALESCE(C5, 0)                                   AS C_CYCLE,
--                        TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI')                        AS REC_CREATE_TIME
--                 FROM PROC_PRODUCT_LIST A
--                          LEFT JOIN DIRECT_POS_AGG DIRECT_POS ON A.PROC_KEY = DIRECT_POS.TARGET_PROC_KEY
--                          LEFT JOIN DIRECT_NEG_AGG DIRECT_NEG ON A.PROC_KEY = DIRECT_NEG.TARGET_PROC_KEY
--                          LEFT JOIN C2_AGG C2 ON A.PROC_KEY = C2.TARGET_PROC_KEY
--                          LEFT JOIN C3_AGG C3 ON A.PROC_KEY = C3.TARGET_PROC_KEY
--                          LEFT JOIN C4_AGG C4 ON A.PROC_KEY = C4.TARGET_PROC_KEY
--                          LEFT JOIN C5_AGG C5 ON A.PROC_KEY = C5.TARGET_PROC_KEY
--                 ORDER BY PROC_KEY)


