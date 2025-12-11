WITH MATRIX_INV AS (SELECT PROC_KEY, SOURCE_PROC_KEY, CAST(UNIT_COST AS DOUBLE) AS UNIT_COST
                    FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_MATRIX_ENERGY
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
                 AND BATCH_NUMBER = '20230120231220240903YS'
                 AND (LCA_PROC_CODE LIKE 'BF%' OR LCA_PROC_CODE LIKE 'CO%' OR LCA_PROC_CODE LIKE 'SI%' OR
                      LCA_PROC_NAME LIKE '能环部%')),
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
--                      A.VALUE,
                     CASE
                         WHEN A.VALUE < 0 THEN -A.VALUE
                         ELSE A.VALUE END                                           AS VALUE,
                     B.VALUE                                                        AS PRODUCT_VALUE,
                     CASE
                         WHEN A.VALUE < 0 THEN CAST(-A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE)
                         ELSE CAST(A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE) END AS UNIT_COST
              FROM DATA1 A
                       JOIN PROC_PRODUCT_LIST B ON A.PROC_KEY = B.PROC_KEY),
     FACTOR AS (SELECT DISTINCT a.ITEM_CODE,
                                b.DISCH_COEFF,
                                d.GWP,
                                e.CUSTOMS_TRANS_VALUE,
                                e.TRAIN_TRANS_VALUE,
                                e.TRUCK_CAR_TRANS_VALUE,
                                e.RIVER_CAR_TRANS_VALUE,
                                c.FLAG
                FROM (SELECT DISTINCT LCA_DATA_ITEM_CODE as ITEM_CODE
                      FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
                      WHERE BATCH_NUMBER = '20230120231220240903YS'
                      UNION
                      SELECT DISTINCT ITEM_CODE
                      FROM BG00MAC102.T_ADS_FACT_LCA_CR0001
                      WHERE SUBSTR(REC_CREATE_TIME, 1, 4) = '2023'
                      UNION
                      SELECT DISTINCT ITEM_CODE
                      FROM BG00MAC102.T_ADS_FACT_LCA_HP0001
                      WHERE SUBSTR(REC_CREATE_TIME, 1, 4) = '2023'
                      UNION
                      SELECT DISTINCT ITEM_CODE
                      FROM BG00MAC102.T_ADS_FACT_LCA_SI0001
                      WHERE SUBSTR(REC_CREATE_TIME, 1, 4) = '2023') AS a
                         LEFT JOIN (SELECT DISTINCT *
                                    FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                                    WHERE org_code = 'TA'
                                      AND start_time = '2024') AS b ON a.ITEM_CODE = b.ITEM_CODE
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                                    WHERE base_code = 'TA'
                                      AND start_time = '2024') AS c ON a.ITEM_CODE = c.data_code
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES
                                    WHERE company_code = 'TA'
                                      AND start_time = '2024') AS d ON c.uuid = d.uuid
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                                    WHERE company_code = 'TA'
                                      AND start_time = '2024') AS e ON a.ITEM_CODE = e.LCA_DATA_ITEM_CODE),
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
     FACTOR_BP AS (SELECT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
                   FROM FACTOR
                   WHERE FLAG = 'FCP'
                     AND GWP IS NOT NULL
                     AND GWP != 0),
     FACTOR_UP AS (SELECT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
                   FROM FACTOR
                   WHERE FLAG = 'SY'
                     AND GWP IS NOT NULL
                     AND GWP != 0
                   EXCEPT
                   SELECT *
                   FROM FACTOR_BP),
     RESOURCE as (SELECT * FROM DATA WHERE ITEM_CAT_NAME IN ('原材料', '能源', '辅助材料')),
     PRODUCT_BY_PRODUCT AS (SELECT * FROM DATA WHERE ITEM_CAT_NAME IN ('产品', '副产品', '固废')),
     BY_PRODUCT AS (select * FROM DATA WHERE ITEM_CAT_NAME IN ('副产品', '固废')),
     RECYCLED_ITEM AS (SELECT DISTINCT ITEM_CODE FROM RESOURCE INTERSECT SELECT DISTINCT ITEM_CODE FROM BY_PRODUCT),
     OUTSITE_ITEM AS (SELECT DISTINCT ITEM_CODE FROM RESOURCE EXCEPT SELECT DISTINCT ITEM_CODE FROM BY_PRODUCT),
     C1_DIST AS (SELECT PROC_KEY, A.ITEM_CODE, A.UNIT_COST * B.FACTOR_DIRECT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE
                 UNION
                 SELECT PROC_KEY, A.ITEM_CODE, -A.UNIT_COST * B.FACTOR_DIRECT AS LOAD
                 FROM PRODUCT_BY_PRODUCT A
                          INNER JOIN FACTOR_DIRECT B ON A.ITEM_CODE = B.ITEM_CODE),
     C1_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                  FROM C1_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C1_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C1 FROM C1_CYCLE GROUP BY TARGET_PROC_KEY),
     C2_DIST AS (SELECT PROC_KEY, A.ITEM_CODE, A.UNIT_COST * B.FACTOR_INDIRECT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_BP B ON A.ITEM_CODE = B.ITEM_CODE),
     C2_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                  FROM C2_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C2_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C2 FROM C2_CYCLE GROUP BY TARGET_PROC_KEY),
     C3_DIST AS (SELECT PROC_KEY, A.ITEM_CODE, A.UNIT_COST * B.FACTOR_INDIRECT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_UP B ON A.ITEM_CODE = B.ITEM_CODE),
     C3_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                  FROM C3_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C3_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C3 FROM C3_CYCLE GROUP BY TARGET_PROC_KEY),
     C4_DIST AS (SELECT PROC_KEY, A.ITEM_CODE, -A.UNIT_COST * B.FACTOR_INDIRECT AS LOAD
                 FROM BY_PRODUCT A
                          INNER JOIN FACTOR_BP B ON A.ITEM_CODE = B.ITEM_CODE),
     C4_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                  FROM C4_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C4_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C4 FROM C4_CYCLE GROUP BY TARGET_PROC_KEY),
     C5_DIST AS (SELECT PROC_KEY, A.ITEM_CODE, A.UNIT_COST * B.FACTOR_TRANSPORT AS LOAD
                 FROM RESOURCE A
                          INNER JOIN FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE),
     C5_CYCLE AS (SELECT A.PROC_KEY AS SOURCE_PROC_KEY, B.PROC_KEY AS TARGET_PROC_KEY, A.LOAD * B.UNIT_COST AS COST
                  FROM C5_DIST A
                           CROSS JOIN MATRIX_INV B
                  WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
     C5_AGG AS (SELECT TARGET_PROC_KEY AS PROC_KEY, SUM(COST) AS C5 FROM C5_CYCLE GROUP BY TARGET_PROC_KEY),
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
                       COALESCE(C1, 0) + COALESCE(C2, 0) + COALESCE(C3, 0) + COALESCE(C4, 0) +
                       COALESCE(C5, 0)                                     AS C_CYCLE,
                       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI')        AS REC_CREATE_TIME
                FROM PROC_PRODUCT_LIST A
                         LEFT JOIN C1_AGG C1 ON A.PROC_KEY = C1.PROC_KEY
                         LEFT JOIN C2_AGG C2 ON A.PROC_KEY = C2.PROC_KEY
                         LEFT JOIN C3_AGG C3 ON A.PROC_KEY = C3.PROC_KEY
                         LEFT JOIN C4_AGG C4 ON A.PROC_KEY = C4.PROC_KEY
                         LEFT JOIN C5_AGG C5 ON A.PROC_KEY = C5.PROC_KEY
                ORDER BY PROC_KEY),
     GROUND_RESULT AS (SELECT *
                       FROM BG00MAC102.T_CALC_MAIN_RESULT
                       where BATCH_NUMBER = '20230120231220240903YS'
                         and LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计'),
     DIFF AS (select A.BATCH_NUMBER,
                     A.COMPANY_CODE,
                     A.PROC_KEY,
                     A.PROC_CODE,
                     A.PROC_NAME,
                     A.PRODUCT_CODE,
                     A.PRODUCT_NAME,
                     A.C_CYCLE,
                     B.G_CYCLE,
                     ABS(A.C_CYCLE - B.G_CYCLE) AS DIFF
              from RESULT A
                       JOIN GROUND_RESULT B ON A.PRODUCT_NAME = B.PRODUCT_NAME)
select *
from DIFF;
-- SELECT PROC_KEY, PROC_NAME, PRODUCT_NAME, C_CYCLE
-- FROM RESULT;


-- select A.PROC_NAME, A.PRODUCT_NAME, A.C_CYCLE, B.G_CYCLE, A.C_CYCLE - B.G_CYCLE
select *, ABS(A.C_CYCLE - B.G_CYCLE) AS DIFF
from (select *
      from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_ENERGY_RESULT
      where COMPANY_CODE = 'TA'
        and BATCH_NUMBER = '20230120231220240903YS') A
         join (select *
               from BG00MAC102.T_CALC_MAIN_RESULT
               where COMPANY_CODE = 'TA'
                 and BATCH_NUMBER = '20230120231220240903YS'
                 and LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计') B
              on concat(concat(concat(A.PROC_CODE, A.PRODUCT_CODE), '04'), A.PRODUCT_CODE) = B.PRODUCT_CODE;

select *
from BG00MAC102.T_CALC_MAIN_RESULT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20230120231220240903YS'
  and LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计';


select *
from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_ENERGY_RESULT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20230120231220240903YS'
  and PRODUCT_NAME = '余热发电量';


select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20230120231220240903YS'
  and LCA_PROC_NAME = '能环部-动力分厂-余热发电';

select *
from BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where BASE_CODE = 'TA';


SELECT *
FROM BG00MAC102.T_CALC_MAIN_RESULT
where BATCH_NUMBER = '20230120231220240903YS'
  and LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计'

