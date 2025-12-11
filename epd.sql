WITH DATA1 as (SELECT REC_ID,
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
               FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
               WHERE COMPANY_CODE = 'TA'
                 AND BATCH_NUMBER = '20240120241220250107YS'),
     DATA2 AS (SELECT REC_ID,
                      BATCH_NUMBER,
                      START_YM,
                      END_YM,
                      COMPANY_CODE,
                      PROC_KEY,
                      PROC_CODE,
                      PROC_NAME,
                      PRODUCT_CODE,
                      PRODUCT_NAME,
                      ITEM_CAT_CODE,
                      ITEM_CAT_NAME,
                      ITEM_CODE,
                      ITEM_NAME,
                      ABS(VALUE) AS VALUE,
                      UNIT
               FROM DATA1),
     PROC_PRODUCT_LIST AS (SELECT DISTINCT PROC_KEY,
                                           PROC_CODE,
                                           PROC_NAME,
                                           ITEM_CODE AS PRODUCT_CODE,
                                           ITEM_NAME AS PRODUCT_NAME,
                                           VALUE
                           FROM DATA2
                           WHERE ITEM_CAT_NAME = '产品'),
     DATA AS (SELECT A.*,
                     B.VALUE                                           AS PRODUCT_VALUE,
                     CAST(A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE) AS UNIT_COST
              FROM DATA2 A
                       JOIN PROC_PRODUCT_LIST B
                            ON A.PROC_KEY = B.PROC_KEY),
     MATRIX_INV AS (SELECT PROC_KEY,
                           SOURCE_PROC_KEY,
                           UNIT_COST
                    FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX_ENERGY
                    WHERE BATCH_NUMBER = '20240120241220250107YS'
                      AND COMPANY_CODE = 'TA'
                      AND INV = 'Y'
                      AND UNIT_COST != 0
                      AND PROC_KEY IN (SELECT PROC_KEY FROM PROC_PRODUCT_LIST)
                      AND SOURCE_PROC_KEY IN (SELECT PROC_KEY FROM PROC_PRODUCT_LIST)),
     FCP AS (SELECT DISTINCT *
             FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
             WHERE FLAG = 'FCP'
               AND BASE_CODE = 'TA'
               AND START_TIME = '2025'),
     SY AS (SELECT DISTINCT *
            FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
            WHERE FLAG = 'SY'
              AND BASE_CODE = 'TA'
              AND START_TIME = '2025'
              AND DATA_CODE NOT IN (SELECT DATA_CODE FROM FCP)),
     UUID AS (SELECT DATA_CODE, UUID, FLAG
              FROM FCP
              UNION
              (SELECT DATA_CODE, UUID, FLAG FROM SY)),
     GWP_DIST AS (SELECT A.DATA_CODE,
                         A.FLAG,
                         B.DATA_ITEM_NAME,
                         B.LCI_ELEMENT_VALUE AS GWP,
                         C.LCI_ELEMENT_VALUE AS GWP_FOSSIL_ENERGY,
                         D.LCI_ELEMENT_VALUE AS GWP_LAND_USE,
                         E.LCI_ELEMENT_VALUE AS GWP_BIOMASS
                  FROM UUID A
                           LEFT JOIN (SELECT *
                                      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                      WHERE YEAR = '2025'
                                        AND LCI_ELEMENT = 'GWP') B ON A.UUID = B.UUID
                           LEFT JOIN (SELECT *
                                      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                      WHERE YEAR = '2025'
                                        AND LCI_ELEMENT = 'GWP_FOSSIL_ENERGY') C ON A.UUID = C.UUID
                           LEFT JOIN (SELECT *
                                      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                      WHERE YEAR = '2025'
                                        AND LCI_ELEMENT = 'GWP_LAND_USE') D ON A.UUID = D.UUID
                           LEFT JOIN (SELECT *
                                      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                      WHERE YEAR = '2025'
                                        AND LCI_ELEMENT = 'GWP_BIOMASS') E ON A.UUID = E.UUID),
     GWP_SUBSET AS (SELECT A.DATA_CODE,
                           A.FLAG,
                           B.DATA_ITEM_NAME,
                           B.LCI_ELEMENT,
                           B.LCI_ELEMENT_NAME,
                           B.LCI_ELEMENT_VALUE
                    FROM UUID A
                             JOIN (SELECT *
                                   FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                   WHERE YEAR = '2025'
                                     AND LCI_ELEMENT LIKE '%GWP%') B ON A.UUID = B.UUID),
     FACTOR_DIRECT AS (SELECT A.ITEM_CODE, A.ITEM_NAME, B.DISCH_COEFF
                       FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM DATA) A
                                LEFT JOIN
                            (SELECT DISTINCT *
                             FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                             WHERE ORG_CODE = 'TA'
                               AND START_TIME = '2025')
                                AS B
                            ON A.ITEM_CODE = B.ITEM_CODE
                       WHERE B.DISCH_COEFF IS NOT NULL),
     FACTOR_DISTANCE AS (SELECT A.ITEM_CODE,
                                A.ITEM_NAME,
                                B.RIVER_CAR_TRANS_VALUE / 1000 AS RIVER_CAR_TRANS_VALUE,
                                B.TRUCK_CAR_TRANS_VALUE / 1000 AS TRUCK_CAR_TRANS_VALUE,
                                B.TRAIN_TRANS_VALUE / 1000     AS TRAIN_TRANS_VALUE,
                                B.CUSTOMS_TRANS_VALUE / 1000   AS CUSTOMS_TRANS_VALUE
                         FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM DATA) A
                                  LEFT JOIN
                              (SELECT *
                               FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                               WHERE COMPANY_CODE = 'TA'
                                 AND START_TIME = '2025')
                                  AS B
                              ON A.ITEM_CODE = B.LCA_DATA_ITEM_CODE),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                           WHERE YEAR = '2025'
                             AND LCI_ELEMENT LIKE '%GWP%'
                             AND DATA_ITEM_NAME IN ('海运', '河运', '铁运', '汽运')),
     RIVER_CAR_TRANS_VALUE AS (SELECT ITEM_CODE,
                                      ITEM_NAME,
                                      B.LCI_ELEMENT,
                                      B.LCI_ELEMENT_NAME,
                                      COALESCE(A.RIVER_CAR_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                               FROM FACTOR_DISTANCE A
                                        JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '河运') B ON 1 = 1),
     CUSTOMS_TRANS_VALUE AS (SELECT ITEM_CODE,
                                    ITEM_NAME,
                                    B.LCI_ELEMENT,
                                    B.LCI_ELEMENT_NAME,
                                    COALESCE(A.CUSTOMS_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                             FROM FACTOR_DISTANCE A
                                      JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '海运') B ON 1 = 1),
     TRAIN_TRANS_VALUE AS (SELECT ITEM_CODE,
                                  ITEM_NAME,
                                  B.LCI_ELEMENT,
                                  B.LCI_ELEMENT_NAME,
                                  COALESCE(A.TRAIN_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                           FROM FACTOR_DISTANCE A
                                    JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '铁运') B ON 1 = 1),
     TRUCK_CAR_TRANS_VALUE AS (SELECT ITEM_CODE,
                                      ITEM_NAME,
                                      B.LCI_ELEMENT,
                                      B.LCI_ELEMENT_NAME,
                                      COALESCE(A.TRUCK_CAR_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                               FROM FACTOR_DISTANCE A
                                        JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '汽运') B ON 1 = 1),
     TRANS_VALUE AS (SELECT *
                     FROM RIVER_CAR_TRANS_VALUE
                     UNION
                     (SELECT * FROM CUSTOMS_TRANS_VALUE)
                     UNION
                     (SELECT * FROM TRAIN_TRANS_VALUE)
                     UNION
                     (SELECT * FROM TRUCK_CAR_TRANS_VALUE)),
     FACTOR_TRANSPORT AS (SELECT ITEM_CODE,
                                 ITEM_NAME,
                                 LCI_ELEMENT,
                                 LCI_ELEMENT_NAME,
                                 SUM(LCI_ELEMENT_VALUE) AS LCI_ELEMENT_VALUE
                          FROM TRANS_VALUE
                          GROUP BY ITEM_CODE, ITEM_NAME, LCI_ELEMENT, LCI_ELEMENT_NAME),
     FACTOR_GWP AS (SELECT DISTINCT A.ITEM_CODE,
                                    A.ITEM_NAME,
                                    B.FLAG,
                                    B.LCI_ELEMENT,
                                    B.LCI_ELEMENT_NAME,
                                    B.LCI_ELEMENT_VALUE
                    FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME
                          FROM DATA) A
                             JOIN
                             (SELECT * FROM GWP_SUBSET)
                             AS B
                             ON A.ITEM_CODE = B.DATA_CODE
                    order by a.ITEM_CODE),
     FACTOR_FCP AS (SELECT ITEM_CODE, LCI_ELEMENT, LCI_ELEMENT_NAME, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS GWP_FCP
                    FROM FACTOR_GWP
                    WHERE FLAG = 'FCP'
                      AND LCI_ELEMENT_VALUE != 0),
     FACTOR_SY AS (SELECT ITEM_CODE, LCI_ELEMENT, LCI_ELEMENT_NAME, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS GWP_SY
                   FROM FACTOR_GWP
                   WHERE FLAG = 'SY'
                     AND LCI_ELEMENT_VALUE != 0),
     INPUT as (SELECT * FROM DATA WHERE ITEM_CAT_CODE < '04'),
     OUTPUT AS (SELECT * FROM DATA WHERE ITEM_CAT_CODE >= '04'),
     BY_PRODUCT AS (SELECT * FROM DATA WHERE ITEM_CAT_CODE > '04'),
     C1_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.DISCH_COEFF,
                        A.UNIT_COST * B.DISCH_COEFF AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE
                 UNION
                 SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.DISCH_COEFF,
                        -A.UNIT_COST * B.DISCH_COEFF AS LOAD
                 FROM OUTPUT A
                          INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     C1_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                  FROM C1_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C1_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C1 FROM C1_CYCLE GROUP BY TARGET_PROC_KEY),
     C2_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.GWP_FCP,
                        A.UNIT_COST * B.GWP_FCP AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_FCP B ON A.ITEM_CODE = B.ITEM_CODE),
     C2_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C2_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C2_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C2
                FROM C2_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C3_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.GWP_SY,
                        A.UNIT_COST * B.GWP_SY AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_SY B
                                     ON A.ITEM_CODE = B.ITEM_CODE),
     C3_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C3_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C3_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C3
                FROM C3_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C4_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE,
                        -A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                 FROM BY_PRODUCT A
                          INNER JOIN FACTOR_GWP B
                                     ON A.ITEM_CODE = B.ITEM_CODE),
     C4_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C4_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C4_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C4
                FROM C4_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C5_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE,
                        A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE),
     C5_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C5_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C5_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C5
                FROM C5_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     RESULT1 AS (SELECT HEX(RAND())              AS REC_ID,
                        '20240120241220250107YS' AS BATCH_NUMBER,
                        'TA'                     AS COMPANY_CODE,
                        A.PROC_KEY,
                        A.PROC_CODE,
                        A.PROC_NAME,
                        A.PRODUCT_CODE,
                        A.PRODUCT_NAME,
                        A.LCI_ELEMENT,
                        A.LCI_ELEMENT_NAME,
                        CASE
                            WHEN A.LCI_ELEMENT IN ('GWP', 'GWP_FOSSIL_ENERGY') THEN COALESCE(C1, 0)
                            ELSE 0 END           AS C1_DIRECT,
                        COALESCE(C2, 0)          AS C2_BP,
                        COALESCE(C3, 0)          AS C3_OUT,
                        COALESCE(C4, 0)          AS C4_BP_NEG,
                        COALESCE(C5, 0)          AS C5_TRANS
                 FROM (SELECT *
                       FROM PROC_PRODUCT_LIST A
                                LEFT JOIN (SELECT DISTINCT LCI_ELEMENT, LCI_ELEMENT_NAME FROM FACTOR_GWP) ON 1 = 1) A
                          LEFT JOIN C1_AGG ON A.PROC_KEY = C1_AGG.PROC_KEY
                          LEFT JOIN C2_AGG ON A.PROC_KEY = C2_AGG.PROC_KEY AND A.LCI_ELEMENT = C2_AGG.LCI_ELEMENT
                          LEFT JOIN C3_AGG ON A.PROC_KEY = C3_AGG.PROC_KEY AND A.LCI_ELEMENT = C3_AGG.LCI_ELEMENT
                          LEFT JOIN C4_AGG ON A.PROC_KEY = C4_AGG.PROC_KEY AND A.LCI_ELEMENT = C4_AGG.LCI_ELEMENT
                          LEFT JOIN C5_AGG ON A.PROC_KEY = C5_AGG.PROC_KEY AND A.LCI_ELEMENT = C5_AGG.LCI_ELEMENT
                 ORDER BY PROC_KEY),
     RESULT AS (SELECT *,
                       C1_DIRECT + C2_BP                                 AS C_INSITE,
                       C3_OUT + C4_BP_NEG + C5_TRANS                     AS C_OUTSITE,
                       C1_DIRECT + C2_BP + C3_OUT + C4_BP_NEG + C5_TRANS AS C_CYCLE,
                       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI')      AS REC_CREATE_TIME
                FROM RESULT1),
     RESULT_DIFF AS (SELECT A.PROC_KEY,
                            A.PROC_NAME,
                            A.PRODUCT_NAME,
                            A.LCI_ELEMENT_NAME,
                            A.C_CYCLE,
                            B.G_CYCLE,
                            ABS(A.C_CYCLE - B.G_CYCLE)               AS DIFF,
                            SUM(ABS(A.C_CYCLE - B.G_CYCLE)) OVER ( ) as SUM_DIFF,
                            A.C1_DIRECT,
                            A.C2_BP,
                            A.C3_OUT,
                            A.C4_BP_NEG,
                            A.C5_TRANS,
                            B.G1,
                            B.G2,
                            B.G3,
                            B.G4,
                            B.G5,
                            B.G6,
                            B.G7
                     FROM RESULT A
                              LEFT JOIN (select *
                                         from T_CALC_MAIN_RESULT
                                         where BATCH_NUMBER = '20240120241220250107YS'
                                           and COMPANY_CODE = 'TA') B
                                        ON A.PRODUCT_NAME = B.PRODUCT_NAME AND A.PROC_CODE = B.PROC_CODE AND
                                           A.LCI_ELEMENT_NAME = B.LCI_ELEMENT_NAME)
select *
from RESULT_DIFF
;

select *
from T_CALC_MAIN_RESULT
where BATCH_NUMBER = '20240120241220250107YS'
  and COMPANY_CODE = 'TA'
  and PRODUCT_CODE = 'C411N026004N0260';


SELECT *
FROM T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
WHERE YEAR = '2025'
  AND COMPANY_CODE = 'TA';

SELECT *
FROM T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI_LOOKUP;

SELECT *
FROM T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_NEW
WHERE START_TIME = '2025'
  AND COMPANY_CODE = 'TA';


SELECT *
FROM T_ADS_WH_LCA_MAT_DATA
WHERE START_TIME = '2025';


WITH DATA1 as (SELECT REC_ID,
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
               FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
               WHERE COMPANY_CODE = 'TA'
                 AND BATCH_NUMBER = '20240120241220250107YS'),
     DATA2 AS (SELECT REC_ID,
                      BATCH_NUMBER,
                      START_YM,
                      END_YM,
                      COMPANY_CODE,
                      PROC_KEY,
                      PROC_CODE,
                      PROC_NAME,
                      PRODUCT_CODE,
                      PRODUCT_NAME,
                      ITEM_CAT_CODE,
                      ITEM_CAT_NAME,
                      ITEM_CODE,
                      ITEM_NAME,
                      VALUE AS VALUE,
                      UNIT
               FROM DATA1),
     PROC_PRODUCT_LIST AS (SELECT DISTINCT PROC_KEY,
                                           PROC_CODE,
                                           PROC_NAME,
                                           ITEM_CODE AS PRODUCT_CODE,
                                           ITEM_NAME AS PRODUCT_NAME,
                                           VALUE
                           FROM DATA2
                           WHERE ITEM_CAT_NAME = '产品'),
     DATA AS (SELECT A.*,
                     B.VALUE                                           AS PRODUCT_VALUE,
                     CAST(A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE) AS UNIT_COST
              FROM DATA2 A
                       JOIN PROC_PRODUCT_LIST B
                            ON A.PROC_KEY = B.PROC_KEY),
     MATRIX_INV AS (SELECT PROC_KEY,
                           SOURCE_PROC_KEY,
                           UNIT_COST
                    FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX_ENERGY
                    WHERE BATCH_NUMBER = '20240120241220250107YS'
                      AND COMPANY_CODE = 'TA'
                      AND INV = 'Y'
                      AND UNIT_COST != 0
                      AND PROC_KEY IN (SELECT PROC_KEY FROM PROC_PRODUCT_LIST)
                      AND SOURCE_PROC_KEY IN (SELECT PROC_KEY FROM PROC_PRODUCT_LIST)),
     FCP AS (SELECT DISTINCT *
             FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
             WHERE FLAG = 'FCP'
               AND BASE_CODE = 'TA'
               AND START_TIME = '2025'),
     SY AS (SELECT DISTINCT *
            FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
            WHERE FLAG = 'SY'
              AND BASE_CODE = 'TA'
              AND START_TIME = '2025'
              AND DATA_CODE NOT IN (SELECT DATA_CODE FROM FCP)),
     PF AS (SELECT DISTINCT *
            FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
            WHERE START_TIME = '2025'
              AND BASE_CODE = 'TA'
              AND FLAG = 'PF'),
     LCI AS (SELECT DISTINCT *
             FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
             WHERE START_TIME = '2025'
               AND BASE_CODE = 'TA'
               AND FLAG = 'LCI'),
     UUID AS (SELECT DATA_CODE, UUID, FLAG
              FROM FCP
              UNION
              (SELECT DATA_CODE, UUID, FLAG FROM SY)),
     STREAM AS (SELECT DISTINCT *
                FROM (SELECT A.STREAM_ID,
                             A.STREAM_NAME,
                             B.LCI_ELEMENT_ID,
                             A.LCI_ELEMENT_NAME,
                             A.LCI_ELEMENT_VALUE
                      FROM (SELECT UUID           AS STREAM_ID,
                                   DATA_ITEM_NAME AS STREAM_NAME,
                                   LCI_ELEMENT_NAME,
                                   LCI_ELEMENT_VALUE
                            FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                            WHERE YEAR = '2025'
                              AND COMPANY_CODE = 'TA'
                              AND LCI_ELEMENT_VALUE != 0
                              AND LCI_ELEMENT_VALUE IS NOT NULL
                              AND UUID IN (SELECT DISTINCT LCI_ELEMENT_ID
                                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_LOOKUP)) A
                               LEFT JOIN (SELECT DISTINCT LCI_ELEMENT_ID, LCI_ELEMENT_NAME
                                          FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI) B
                                         ON A.LCI_ELEMENT_NAME = B.LCI_ELEMENT_NAME
                      UNION
                      SELECT UUID           AS STREAM_ID,
                             DATA_ITEM_NAME AS STREAM_NAME,
                             LCI_ELEMENT_ID,
                             LCI_ELEMENT_NAME,
                             LCI_ELEMENT_VALUE
                      FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI
                      WHERE YEAR = '2025'
                        AND UUID IN (SELECT LCI_ELEMENT_ID FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_LOOKUP))),
     LCI_STREAM AS (SELECT DISTINCT A.DATA_CODE,
                                    A.UUID                                      AS STREAM_ID,
                                    COALESCE(B.STREAM_NAME, C.LCI_ELEMENT_NAME) AS STREAM_NAME,
                                    B.LCI_ELEMENT_ID,
                                    B.LCI_ELEMENT_NAME,
                                    B.LCI_ELEMENT_VALUE
                    FROM LCI A
                             LEFT JOIN STREAM B ON A.UUID = B.STREAM_ID
                             LEFT JOIN BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_LOOKUP C ON A.UUID = C.LCI_ELEMENT_ID
                    WHERE A.DATA_CODE IN (SELECT DISTINCT ITEM_CODE FROM DATA)),
     LCI_SUBSET AS (SELECT A.DATA_CODE,
                           A.FLAG,
                           B.DATA_ITEM_NAME,
                           B.LCI_ELEMENT,
                           B.LCI_ELEMENT_NAME,
                           B.LCI_ELEMENT_VALUE
                    FROM UUID A
                             JOIN (SELECT *
                                   FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                   WHERE YEAR = '2025'
                                     AND LCI_ELEMENT NOT LIKE '%GWP%') B ON A.UUID = B.UUID),
     FACTOR_DISCH AS (SELECT A.ITEM_CODE, A.ITEM_NAME, B.HOTVALUE, B.HOTVALUE_UNIT
                      FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM DATA) A
                               LEFT JOIN
                           (SELECT DISTINCT *
                            FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                            WHERE ORG_CODE = 'TA'
                              AND START_TIME = '2025')
                               AS B
                           ON A.ITEM_CODE = B.ITEM_CODE
                      WHERE B.DISCH_COEFF IS NOT NULL),
     FACTOR_DISTANCE AS (SELECT A.ITEM_CODE,
                                A.ITEM_NAME,
                                B.RIVER_CAR_TRANS_VALUE / 1000 AS RIVER_CAR_TRANS_VALUE,
                                B.TRUCK_CAR_TRANS_VALUE / 1000 AS TRUCK_CAR_TRANS_VALUE,
                                B.TRAIN_TRANS_VALUE / 1000     AS TRAIN_TRANS_VALUE,
                                B.CUSTOMS_TRANS_VALUE / 1000   AS CUSTOMS_TRANS_VALUE
                         FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM DATA) A
                                  LEFT JOIN
                              (SELECT *
                               FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                               WHERE COMPANY_CODE = 'TA'
                                 AND START_TIME = '2025')
                                  AS B
                              ON A.ITEM_CODE = B.LCA_DATA_ITEM_CODE),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                           WHERE YEAR = '2025'
                             AND LCI_ELEMENT NOT LIKE '%GWP%'
                             AND DATA_ITEM_NAME IN ('海运', '河运', '铁运', '汽运')),
     RIVER_CAR_TRANS_VALUE AS (SELECT ITEM_CODE,
                                      ITEM_NAME,
                                      B.LCI_ELEMENT,
                                      B.LCI_ELEMENT_NAME,
                                      COALESCE(A.RIVER_CAR_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                               FROM FACTOR_DISTANCE A
                                        JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '河运') B ON 1 = 1),
     CUSTOMS_TRANS_VALUE AS (SELECT ITEM_CODE,
                                    ITEM_NAME,
                                    B.LCI_ELEMENT,
                                    B.LCI_ELEMENT_NAME,
                                    COALESCE(A.CUSTOMS_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                             FROM FACTOR_DISTANCE A
                                      JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '海运') B ON 1 = 1),
     TRAIN_TRANS_VALUE AS (SELECT ITEM_CODE,
                                  ITEM_NAME,
                                  B.LCI_ELEMENT,
                                  B.LCI_ELEMENT_NAME,
                                  COALESCE(A.TRAIN_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                           FROM FACTOR_DISTANCE A
                                    JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '铁运') B ON 1 = 1),
     TRUCK_CAR_TRANS_VALUE AS (SELECT ITEM_CODE,
                                      ITEM_NAME,
                                      B.LCI_ELEMENT,
                                      B.LCI_ELEMENT_NAME,
                                      COALESCE(A.TRUCK_CAR_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                               FROM FACTOR_DISTANCE A
                                        JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE DATA_ITEM_NAME = '汽运') B ON 1 = 1),
     TRANS_VALUE AS (SELECT *
                     FROM RIVER_CAR_TRANS_VALUE
                     UNION
                     (SELECT * FROM CUSTOMS_TRANS_VALUE)
                     UNION
                     (SELECT * FROM TRAIN_TRANS_VALUE)
                     UNION
                     (SELECT * FROM TRUCK_CAR_TRANS_VALUE)),
     FACTOR_TRANSPORT AS (SELECT ITEM_CODE,
                                 ITEM_NAME,
                                 LCI_ELEMENT,
                                 LCI_ELEMENT_NAME,
                                 SUM(LCI_ELEMENT_VALUE) AS LCI_ELEMENT_VALUE
                          FROM TRANS_VALUE
                          GROUP BY ITEM_CODE, ITEM_NAME, LCI_ELEMENT, LCI_ELEMENT_NAME),
     FACTOR_LCI AS (SELECT DISTINCT A.ITEM_CODE,
                                    A.ITEM_NAME,
                                    B.FLAG,
                                    B.DATA_ITEM_NAME,
                                    B.LCI_ELEMENT,
                                    B.LCI_ELEMENT_NAME,
                                    B.LCI_ELEMENT_VALUE
                    FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME
                          FROM DATA) A
                             JOIN
                             (SELECT * FROM LCI_SUBSET)
                             AS B
                             ON A.ITEM_CODE = B.DATA_CODE
                    order by a.ITEM_CODE),
     FACTOR_FCP AS (SELECT ITEM_CODE, LCI_ELEMENT, LCI_ELEMENT_NAME, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS LCI_FCP
                    FROM FACTOR_LCI
                    WHERE FLAG = 'FCP'
                      AND LCI_ELEMENT_VALUE != 0),
     FACTOR_SY AS (SELECT ITEM_CODE, LCI_ELEMENT, LCI_ELEMENT_NAME, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS LCI_SY
                   FROM FACTOR_LCI
                   WHERE FLAG = 'SY'
                     AND LCI_ELEMENT_VALUE != 0),
     FACTOR_PF AS (SELECT A.DATA_CODE                         AS ITEM_CODE,
                          B.DATA_ITEM_NAME,
                          B.LCI_ELEMENT,
                          B.LCI_ELEMENT_NAME,
                          CAST(B.LCI_ELEMENT_VALUE AS DOUBLE) AS LCI_ELEMENT_VALUE
                   FROM PF A
                            JOIN (SELECT *
                                  FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                  WHERE YEAR = '2025'
                                    AND LCI_ELEMENT NOT LIKE '%GWP%'
                                    AND LCI_ELEMENT_VALUE != 0) B ON A.UUID = B.UUID),
     FACTOR_STREAM AS (SELECT A.DATA_CODE                         AS ITEM_CODE,
                              B.DATA_ITEM_NAME,
                              B.LCI_ELEMENT,
                              B.LCI_ELEMENT_NAME,
                              CAST(B.LCI_ELEMENT_VALUE AS DOUBLE) AS LCI_ELEMENT_VALUE
                       FROM LCI A
                                JOIN (SELECT *
                                      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
                                      WHERE YEAR = '2025'
                                        AND LCI_ELEMENT NOT LIKE '%GWP%'
                                        AND LCI_ELEMENT_VALUE != 0) B ON A.UUID = B.UUID),
     INPUT as (SELECT *
               FROM DATA
               WHERE ITEM_CAT_CODE < '04'
                 AND ITEM_CODE NOT IN (SELECT PRODUCT_CODE FROM PROC_PRODUCT_LIST)),
     OUTPUT AS (SELECT * FROM DATA WHERE ITEM_CAT_CODE IN ('04', '05', '08')),
     EMISSION AS (SELECT * FROM DATA WHERE ITEM_CAT_CODE IN ('06', '07')),
     BY_PRODUCT AS (SELECT * FROM DATA WHERE ITEM_CAT_CODE IN ('05', '08')),
     HOT_DIST AS (SELECT PROC_KEY,
                         PROC_NAME,
                         PRODUCT_NAME,
                         A.ITEM_CODE,
                         A.ITEM_NAME,
                         A.UNIT_COST,
                         COALESCE(B.HOTVALUE, 0)               AS HOTVALUE,
                         B.HOTVALUE_UNIT,
                         A.UNIT_COST * COALESCE(B.HOTVALUE, 0) AS LOAD
                  FROM INPUT A
                           INNER JOIN FACTOR_DISCH B ON A.ITEM_CODE = B.ITEM_CODE
                  UNION
                  SELECT PROC_KEY,
                         PROC_NAME,
                         PRODUCT_NAME,
                         A.ITEM_CODE,
                         A.ITEM_NAME,
                         A.UNIT_COST,
                         COALESCE(B.HOTVALUE, 0)                AS HOTVALUE,
                         B.HOTVALUE_UNIT,
                         -A.UNIT_COST * COALESCE(B.HOTVALUE, 0) AS LOAD
                  FROM OUTPUT A
                           INNER JOIN FACTOR_DISCH B ON A.ITEM_CODE = B.ITEM_CODE),
     HOT_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                   FROM HOT_DIST A
                            CROSS JOIN MATRIX_INV B
                   WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     HOT_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C1 FROM HOT_CYCLE GROUP BY TARGET_PROC_KEY),
     PF_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE,
                        A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                 FROM EMISSION A
                          INNER JOIN FACTOR_PF B ON A.ITEM_CODE = B.ITEM_CODE),
     PF_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM PF_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     PF_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C1
                FROM PF_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C1_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE,
                        A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                 FROM DATA A
                          INNER JOIN FACTOR_STREAM B ON A.ITEM_CODE = B.ITEM_CODE),
     C1_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C1_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C1_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C1
                FROM C1_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C2_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_FCP,
                        A.UNIT_COST * B.LCI_FCP AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_FCP B ON A.ITEM_CODE = B.ITEM_CODE),
     C2_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C2_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C2_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C2
                FROM C2_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C3_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_SY,
                        A.UNIT_COST * B.LCI_SY AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_SY B
                                     ON A.ITEM_CODE = B.ITEM_CODE),
     C3_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C3_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C3_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C3
                FROM C3_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C4_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE,
                        -A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                 FROM BY_PRODUCT A
                          INNER JOIN FACTOR_LCI B
                                     ON A.ITEM_CODE = B.ITEM_CODE),
     C4_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C4_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C4_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C4
                FROM C4_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     C5_DIST AS (SELECT PROC_KEY,
                        PROC_NAME,
                        PRODUCT_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.UNIT_COST,
                        B.LCI_ELEMENT,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE,
                        A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                 FROM INPUT A
                          INNER JOIN FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE),
     C5_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                         B.PROC_KEY           AS TARGET_PROC_KEY,
                         A.LCI_ELEMENT,
                         A.LCI_ELEMENT_NAME,
                         A.LOAD * B.UNIT_COST AS COST
                  FROM C5_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C5_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME, SUM(COST) AS C5
                FROM C5_CYCLE
                GROUP BY TARGET_PROC_KEY, LCI_ELEMENT, LCI_ELEMENT_NAME),
     RESULT1 AS (SELECT HEX(RAND())              AS REC_ID,
                        '20240120241220250107YS' AS BATCH_NUMBER,
                        'TA'                     AS COMPANY_CODE,
                        A.PROC_KEY,
                        A.PROC_CODE,
                        A.PROC_NAME,
                        A.PRODUCT_CODE,
                        A.PRODUCT_NAME,
                        A.LCI_ELEMENT,
                        A.LCI_ELEMENT_NAME,
                        COALESCE(C1, 0)          AS C1_DIRECT,
                        COALESCE(C2, 0)          AS C2_BP,
                        COALESCE(C3, 0)          AS C3_OUT,
                        COALESCE(C4, 0)          AS C4_BP_NEG,
                        COALESCE(C5, 0)          AS C5_TRANS
                 FROM (SELECT *
                       FROM PROC_PRODUCT_LIST A
                                LEFT JOIN (SELECT DISTINCT LCI_ELEMENT, LCI_ELEMENT_NAME FROM FACTOR_LCI) ON 1 = 1) A
                          LEFT JOIN C1_AGG ON A.PROC_KEY = C1_AGG.PROC_KEY AND A.LCI_ELEMENT = C1_AGG.LCI_ELEMENT
                          LEFT JOIN C2_AGG ON A.PROC_KEY = C2_AGG.PROC_KEY AND A.LCI_ELEMENT = C2_AGG.LCI_ELEMENT
                          LEFT JOIN C3_AGG ON A.PROC_KEY = C3_AGG.PROC_KEY AND A.LCI_ELEMENT = C3_AGG.LCI_ELEMENT
                          LEFT JOIN C4_AGG ON A.PROC_KEY = C4_AGG.PROC_KEY AND A.LCI_ELEMENT = C4_AGG.LCI_ELEMENT
                          LEFT JOIN C5_AGG ON A.PROC_KEY = C5_AGG.PROC_KEY AND A.LCI_ELEMENT = C5_AGG.LCI_ELEMENT
                 ORDER BY PROC_KEY),
     RESULT AS (SELECT *,
                       C1_DIRECT + C2_BP                                 AS C_INSITE,
                       C3_OUT + C4_BP_NEG + C5_TRANS                     AS C_OUTSITE,
                       C1_DIRECT + C2_BP + C3_OUT + C4_BP_NEG + C5_TRANS AS C_CYCLE,
                       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI')      AS REC_CREATE_TIME
                FROM RESULT1),
     RESULT_DIFF AS (SELECT A.PROC_KEY,
                            A.PROC_NAME,
                            A.PRODUCT_NAME,
                            A.LCI_ELEMENT_NAME,
                            A.C_CYCLE,
                            B.G_CYCLE,
                            ABS(A.C_CYCLE - B.G_CYCLE)                          AS DIFF,
                            CASE
                                WHEN A.C_CYCLE = 0 THEN NULL
                                ELSE ABS(A.C_CYCLE - B.G_CYCLE) / A.C_CYCLE END AS PERCENTAGE,
                            SUM(ABS(A.C_CYCLE - B.G_CYCLE)) OVER ( )            as SUM_DIFF,
                            A.C1_DIRECT,
                            A.C2_BP,
                            A.C3_OUT,
                            A.C4_BP_NEG,
                            A.C5_TRANS,
                            B.G1,
                            B.G2,
                            B.G3,
                            B.G4,
                            B.G5,
                            B.G6,
                            B.G7
                     FROM RESULT A
                              LEFT JOIN (select *
                                         from T_CALC_MAIN_RESULT
                                         where BATCH_NUMBER = '20240120241220250107YS'
                                           and COMPANY_CODE = 'TA') B
                                        ON A.PRODUCT_NAME = B.PRODUCT_NAME AND A.PROC_CODE = B.PROC_CODE AND
                                           A.LCI_ELEMENT_NAME = B.LCI_ELEMENT_NAME),
     RESULT_DIFF_AVG AS (SELECT LCI_ELEMENT_NAME, AVG(DIFF) AS DIFF, AVG(PERCENTAGE) AS PERCENTAGE
                         FROM RESULT_DIFF
                         GROUP BY LCI_ELEMENT_NAME)
select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI;



SELECT *
FROM C3_DIST
where LCI_ELEMENT_NAME = '不可再生资源耗竭潜力（ADP）-化石燃料'
  and PROC_KEY = 'Q518_SS250';

select *
from T_CALC_MAIN_RESULT
where BATCH_NUMBER = '20240120241220250107YS'
  and COMPANY_CODE = 'TA';


SELECT *
FROM T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
WHERE YEAR = '2025'
  AND COMPANY_CODE = 'TA'
  AND LCI_ELEMENT_NAME = '一次能源-可再生能源:合计';

SELECT *
FROM T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_LCI
WHERE DATA_ITEM_NAME like '%PCR%';

select *
from T_ADS_WH_LCA_MAT_DATA
where START_TIME = '2025'
  and HOTVALUE_UNIT = 5.0;


-- WHERE (PROC_CODE like 'D%' or PROC_CODE like 'GF%' or
--                       PROC_NAME like '能环部%')


select *
from T_ADS_FACT_LCA_PROC_DATA
where BATCH_NUMBER = '20240120241220250411YS'
  and COMPANY_CODE = 'TA';

select *
from T_ADS_FACT_LCA_PROC_DATA
where BATCH_NUMBER = '20240120241220250107YS'
  and COMPANY_CODE = 'TA';

SELECT *
FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
WHERE FLAG = 'Y'
  AND COMPANY_CODE = 'TA'
  AND TIME_FLAG = 'Y';



