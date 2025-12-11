
WITH TRANS_DATA AS (SELECT LCA_DATA_ITEM_CODE           AS ITEM_CODE,
                           RIVER_CAR_TRANS_VALUE / 1000 AS RIVER_CAR,
                           TRUCK_CAR_TRANS_VALUE / 1000 AS TRUCK_CAR,
                           TRAIN_TRANS_VALUE / 1000     AS TRAIN,
                           CUSTOMS_TRANS_VALUE / 1000   AS CUSTOMS
                    FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                    WHERE COMPANY_CODE = 'TA'
                      AND START_TIME = '2025'),
     FACTOR_DISTANCE AS (SELECT A.ITEM_CODE,
                                A.ITEM_NAME,
                                COALESCE(B.RIVER_CAR, 0) AS RIVER_CAR,
                                COALESCE(B.TRUCK_CAR, 0) AS TRUCK_CAR,
                                COALESCE(B.TRAIN, 0)     AS TRAIN,
                                COALESCE(B.CUSTOMS, 0)   AS CUSTOMS
                         FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_ITEM_BASE A
                                  LEFT JOIN TRANS_DATA B
                                            ON A.ITEM_CODE = B.ITEM_CODE),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                           WHERE VERSION = 'EN15804_Ecoinvent3.11'
                             AND LCI_ELEMENT_CODE IN (SELECT LCI_ELEMENT_CODE
                                                      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_LCI_LIST)
                             AND NAME IN ('海运', '河运', '铁运', '汽运')),
     ALL_COMBINED AS (SELECT F.ITEM_CODE,
                             F.ITEM_NAME,
                             T.LCI_ELEMENT_CODE,
                             T.LCI_ELEMENT_NAME,
                             CASE
                                 WHEN T.NAME = '河运' THEN F.RIVER_CAR * T.LCI_ELEMENT_VALUE
                                 WHEN T.NAME = '汽运' THEN F.TRUCK_CAR * T.LCI_ELEMENT_VALUE
                                 WHEN T.NAME = '铁运' THEN F.TRAIN * T.LCI_ELEMENT_VALUE
                                 WHEN T.NAME = '海运' THEN F.CUSTOMS * T.LCI_ELEMENT_VALUE
                                 ELSE 0
                                 END AS LCI_ELEMENT_VALUE
                      FROM FACTOR_DISTANCE F
                               JOIN FACTOR_TRANSPORT1 T ON 1 = 1)
select *
from ALL_COMBINED;


select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where NAME = '海运'
  and VERSION = 'EN15804_Ecoinvent3.11';


SELECT *
FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
WHERE COMPANY_CODE = 'TA'
  AND START_TIME = '2025';



WITH RAW AS (SELECT COALESCE(A.LCA_DATA_ITEM_CODE, B.LCA_DATA_ITEM_CODE, C.LCA_DATA_ITEM_CODE,
                             D.LCA_DATA_ITEM_CODE) AS ITEM_CODE,
                    COALESCE(A.LCA_DATA_ITEM_NAME, B.LCA_DATA_ITEM_NAME, C.LCA_DATA_ITEM_NAME,
                             D.LCA_DATA_ITEM_NAME) AS ITEM_NAME,
                    RIVER_CAR,
                    TRUCK_CAR,
                    TRAIN,
                    CUSTOMS
             FROM (SELECT DISTINCT LCA_DATA_ITEM_CODE,
                                   LCA_DATA_ITEM_NAME,
                                   CASE
                                       WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                                       ELSE TRANS_DISTANCE END AS CUSTOMS
                   FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
                   WHERE ORG_CODE = 'TA'
                     AND YEAR = '2024'
                     AND LCA_DATA_ITEM_CODE IS NOT NULL
                     AND TRANS_MTHD_CODE = 'seaTrans') A
                      FULL OUTER JOIN
                  (SELECT DISTINCT LCA_DATA_ITEM_CODE,
                                   LCA_DATA_ITEM_NAME,
                                   TRANS_DISTANCE AS TRUCK_CAR
                   FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
                   WHERE ORG_CODE = 'TA'
                     AND YEAR = '2024'
                     AND LCA_DATA_ITEM_CODE IS NOT NULL
                     AND TRANS_MTHD_CODE = 'carTrans') B
                  ON A.LCA_DATA_ITEM_CODE = B.LCA_DATA_ITEM_CODE
                      FULL OUTER JOIN (SELECT DISTINCT LCA_DATA_ITEM_CODE,
                                                       LCA_DATA_ITEM_NAME,
                                                       TRANS_DISTANCE AS RIVER_CAR
                                       FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
                                       WHERE ORG_CODE = 'TA'
                                         AND YEAR = '2024'
                                         AND LCA_DATA_ITEM_CODE IS NOT NULL
                                         AND TRANS_MTHD_CODE = 'riversTrans') C
                                      ON COALESCE(A.LCA_DATA_ITEM_CODE, B.LCA_DATA_ITEM_CODE) = C.LCA_DATA_ITEM_CODE
                      FULL OUTER JOIN (SELECT DISTINCT LCA_DATA_ITEM_CODE,
                                                       LCA_DATA_ITEM_NAME,
                                                       TRANS_DISTANCE AS TRAIN
                                       FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
                                       WHERE ORG_CODE = 'TA'
                                         AND YEAR = '2024'
                                         AND LCA_DATA_ITEM_CODE IS NOT NULL
                                         AND TRANS_MTHD_CODE = 'railwayTrans') D
                                      ON COALESCE(A.LCA_DATA_ITEM_CODE, B.LCA_DATA_ITEM_CODE, C.LCA_DATA_ITEM_CODE) =
                                         D.LCA_DATA_ITEM_CODE),
     TRANS_DATA AS (SELECT A.*
                    FROM (SELECT LCA_DATA_ITEM_CODE           AS ITEM_CODE,
                                 LCA_DATA_ITEM_NAME           AS ITEM_NAME,
                                 CLASS_CODE,
                                 RIVER_CAR_TRANS_VALUE / 1000 AS RIVER_CAR,
                                 TRUCK_CAR_TRANS_VALUE / 1000 AS TRUCK_CAR,
                                 TRAIN_TRANS_VALUE / 1000     AS TRAIN,
                                 CUSTOMS_TRANS_VALUE / 1000   AS CUSTOMS
                          FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                          WHERE COMPANY_CODE = 'TA'
                            AND START_TIME = '2025') A
                             JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_ITEM_BASE B
                                  ON A.ITEM_CODE = B.ITEM_CODE),
     OTHER AS (SELECT ITEM_CODE, ITEM_NAME, CLASS_CODE
               FROM TRANS_DATA
               WHERE ITEM_CODE NOT IN (SELECT ITEM_CODE FROM RAW))
SELECT *
FROM RAW
;


--TA
WITH RAW AS (SELECT DISTINCT LCA_DATA_ITEM_CODE          AS ITEM_CODE,
                             LCA_DATA_ITEM_NAME          AS ITEM_NAME,
                             BIGCLASS_NAME,
                             TRANS_MTHD_CODE,
                             CASE
                                 WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                                 ELSE TRANS_DISTANCE END AS TRANS_DISTANCE,
                             CASE
                                 WHEN UNIT = 'nm' THEN 'km'
                                 ELSE UNIT END           AS UNIT
             FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
             WHERE ORG_CODE = 'TA'
               AND YEAR = '2024'
               AND LCA_DATA_ITEM_CODE IS NOT NULL),
     TRANS_DATA AS (SELECT A.*
                    FROM (SELECT LCA_DATA_ITEM_CODE    AS ITEM_CODE,
                                 LCA_DATA_ITEM_NAME    AS ITEM_NAME,
                                 CASE
                                     WHEN CLASS_CODE IS NOT NULL THEN CLASS_CODE
                                     WHEN CLASS_CODE IS NULL AND LCA_DATA_ITEM_CODE LIKE '125%' THEN 'THJ001'
                                     ELSE 'FYL001' END AS CLASS_CODE
                          FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                          WHERE COMPANY_CODE = 'TA'
                            AND START_TIME = '2025') A
                             JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_ITEM_BASE B
                                  ON A.ITEM_CODE = B.ITEM_CODE),
     OTHER AS (SELECT *
               FROM TRANS_DATA
               WHERE ITEM_CODE NOT IN (SELECT ITEM_CODE FROM RAW)),
     TRANS_AVG AS (SELECT CASE
                              WHEN BIGCLASS_NAME = '铁矿石' THEN 'TKS001'
                              WHEN BIGCLASS_NAME = '废钢' THEN 'FG001'
                              WHEN BIGCLASS_NAME = '铁合金' THEN 'THJ001'
                              ELSE 'FYL001' END       AS CLASS_CODE,
                          TRANS_MTHD_CODE,
                          CASE
                              WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                              ELSE TRANS_DISTANCE END AS TRANS_DISTANCE,
                          CASE
                              WHEN UNIT = 'nm' THEN 'km'
                              ELSE UNIT END           AS UNIT
                   FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION
                   WHERE ORG_CODE = 'TA'
                     AND YEAR = '2024'
                     AND BIGCLASS_NAME IN ('副原料', '铁矿石', '铁合金', '废钢')),
     TRANS_OTHER AS (SELECT ITEM_CODE,
                            ITEM_NAME,
                            A.CLASS_CODE,
                            TRANS_MTHD_CODE,
                            TRANS_DISTANCE,
                            UNIT
                     FROM OTHER A
                              JOIN TRANS_AVG B ON A.CLASS_CODE = B.CLASS_CODE),
     TRANS AS (SELECT ITEM_CODE,
                      ITEM_NAME,
                      CASE
                          WHEN TRANS_MTHD_CODE = 'carTrans' THEN '汽运'
                          WHEN TRANS_MTHD_CODE = 'riversTrans' THEN '河运'
                          WHEN TRANS_MTHD_CODE = 'seaTrans' THEN '海运'
                          WHEN TRANS_MTHD_CODE = 'railwayTrans' THEN '铁运'
                          ELSE TRANS_MTHD_CODE END AS DATA_CODE,
                      TRANS_DISTANCE,
                      UNIT
               FROM (SELECT *
                     FROM RAW
                     UNION
                     SELECT *
                     FROM TRANS_OTHER)),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                           WHERE VERSION = 'EN15804_Ecoinvent3.11'
                             AND LCI_ELEMENT_CODE IN (SELECT LCI_ELEMENT_CODE
                                                      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_LCI_LIST)
                             AND NAME IN ('海运', '河运', '铁运', '汽运', '柴油')),
     FACTOR_TRANSPORT AS (SELECT ITEM_CODE,
                                 ITEM_NAME,
                                 LCI_ELEMENT_CODE,
                                 LCI_ELEMENT_NAME,
                                 SUM(A.TRANS_DISTANCE * LCI_ELEMENT_VALUE / 1000) AS LCI_ELEMENT_VALUE
                          FROM TRANS A
                                   JOIN (SELECT NAME,
                                                LCI_ELEMENT_CODE,
                                                LCI_ELEMENT_NAME,
                                                LCI_ELEMENT_VALUE
                                         FROM FACTOR_TRANSPORT1) B ON A.DATA_CODE = B.NAME
                          GROUP BY ITEM_CODE,
                                   ITEM_NAME,
                                   LCI_ELEMENT_CODE,
                                   LCI_ELEMENT_NAME),
     TRANS_DESC1 AS (SELECT COALESCE(A.ITEM_NAME, B.ITEM_NAME, C.ITEM_NAME, D.ITEM_NAME)                                            AS ITEM_NAME,
                            CONCAT(COALESCE(A.DATA_CODE, ''), CONCAT(COALESCE(B.DATA_CODE, ''),
                                                                     CONCAT(COALESCE(C.DATA_CODE, ''), COALESCE(D.DATA_CODE, '')))) AS DATA_CODE
                     FROM (SELECT DISTINCT ITEM_NAME, DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '海运') A
                              FULL OUTER JOIN
                          (SELECT DISTINCT ITEM_NAME, '+火车' AS DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '铁运') B ON A.ITEM_NAME = B.ITEM_NAME
                              FULL OUTER JOIN
                          (SELECT DISTINCT ITEM_NAME, '+汽运' AS DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '汽运') C ON COALESCE(A.ITEM_NAME, B.ITEM_NAME) = C.ITEM_NAME
                              FULL OUTER JOIN
                          (SELECT DISTINCT ITEM_NAME, '+河运' AS DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '河运') D ON COALESCE(A.ITEM_NAME, B.ITEM_NAME, C.ITEM_NAME) = D.ITEM_NAME)
SELECT ITEM_NAME              AS 名称,
       CASE
           WHEN DATA_CODE LIKE '+%' THEN SUBSTR(DATA_CODE, 2)
           ELSE DATA_CODE END AS 运输方式,
       '运输里程'             AS 距离,
       '宝钢现场'             AS 数据来源,
       '2024年'               AS 年限
FROM TRANS_DESC1
ORDER BY ITEM_NAME
;



--ZG
WITH RAW AS (SELECT DISTINCT LCA_DATA_ITEM_CODE          AS ITEM_CODE,
                             LCA_DATA_ITEM_NAME          AS ITEM_NAME,
                             BIGCLASS_NAME,
                             TRANS_MTHD_CODE,
                             CASE
                                 WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                                 ELSE TRANS_DISTANCE END AS TRANS_DISTANCE,
                             CASE
                                 WHEN UNIT = 'nm' THEN 'km'
                                 ELSE UNIT END           AS UNIT
             FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
             WHERE ORG_CODE = 'ZG'
               AND YEAR = '2024'
               AND LCA_DATA_ITEM_CODE IS NOT NULL),
     TRANS_DATA AS (SELECT A.*
                    FROM (SELECT LCA_DATA_ITEM_CODE    AS ITEM_CODE,
                                 LCA_DATA_ITEM_NAME    AS ITEM_NAME,
                                 CASE
                                     WHEN CLASS_CODE IS NOT NULL THEN CLASS_CODE
                                     WHEN CLASS_CODE IS NULL AND LCA_DATA_ITEM_CODE LIKE '125%' THEN 'THJ001'
                                     ELSE 'FYL001' END AS CLASS_CODE
                          FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                          WHERE COMPANY_CODE = 'ZG'
                            AND START_TIME = '2025') A
                             JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_ITEM_BASE B
                                  ON A.ITEM_CODE = B.ITEM_CODE),
     OTHER AS (SELECT *
               FROM TRANS_DATA
               WHERE ITEM_CODE NOT IN (SELECT ITEM_CODE FROM RAW)),
     TRANS_AVG AS (SELECT CASE
                              WHEN BIGCLASS_NAME = '铁矿石' THEN 'TKS001'
                              WHEN BIGCLASS_NAME = '废钢' THEN 'FG001'
                              WHEN BIGCLASS_NAME = '铁合金' THEN 'THJ001'
                              ELSE 'FYL001' END       AS CLASS_CODE,
                          TRANS_MTHD_CODE,
                          CASE
                              WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                              ELSE TRANS_DISTANCE END AS TRANS_DISTANCE,
                          CASE
                              WHEN UNIT = 'nm' THEN 'km'
                              ELSE UNIT END           AS UNIT
                   FROM T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION
                   WHERE ORG_CODE = 'ZG'
                     AND YEAR = '2024'
                     AND BIGCLASS_NAME IN ('副原料', '铁矿石', '铁合金', '废钢')),
     TRANS_OTHER AS (SELECT ITEM_CODE,
                            ITEM_NAME,
                            A.CLASS_CODE,
                            TRANS_MTHD_CODE,
                            TRANS_DISTANCE,
                            UNIT
                     FROM OTHER A
                              JOIN TRANS_AVG B ON A.CLASS_CODE = B.CLASS_CODE),
     TRANS AS (SELECT ITEM_CODE,
                      ITEM_NAME,
                      CASE
                          WHEN TRANS_MTHD_CODE = 'carTrans' THEN '汽运'
                          WHEN TRANS_MTHD_CODE = 'riversTrans' THEN '河运'
                          WHEN TRANS_MTHD_CODE = 'seaTrans' THEN '海运'
                          WHEN TRANS_MTHD_CODE = 'railwayTrans' THEN '铁运'
                          ELSE TRANS_MTHD_CODE END AS DATA_CODE,
                      TRANS_DISTANCE,
                      UNIT
               FROM (SELECT *
                     FROM RAW
                     UNION
                     SELECT *
                     FROM TRANS_OTHER)),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                           WHERE VERSION = 'EN15804_Ecoinvent3.11'
                             AND LCI_ELEMENT_CODE IN (SELECT LCI_ELEMENT_CODE
                                                      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_LCI_LIST)
                             AND NAME IN ('海运', '河运', '铁运', '汽运', '柴油')),
     FACTOR_TRANSPORT AS (SELECT ITEM_CODE,
                                 ITEM_NAME,
                                 LCI_ELEMENT_CODE,
                                 LCI_ELEMENT_NAME,
                                 SUM(A.TRANS_DISTANCE * LCI_ELEMENT_VALUE / 1000) AS LCI_ELEMENT_VALUE
                          FROM TRANS A
                                   JOIN (SELECT NAME,
                                                LCI_ELEMENT_CODE,
                                                LCI_ELEMENT_NAME,
                                                LCI_ELEMENT_VALUE
                                         FROM FACTOR_TRANSPORT1) B ON A.DATA_CODE = B.NAME
                          GROUP BY ITEM_CODE,
                                   ITEM_NAME,
                                   LCI_ELEMENT_CODE,
                                   LCI_ELEMENT_NAME),
     TRANS_DESC1 AS (SELECT COALESCE(A.ITEM_NAME, B.ITEM_NAME, C.ITEM_NAME, D.ITEM_NAME)                                            AS ITEM_NAME,
                            CONCAT(COALESCE(A.DATA_CODE, ''), CONCAT(COALESCE(B.DATA_CODE, ''),
                                                                     CONCAT(COALESCE(C.DATA_CODE, ''), COALESCE(D.DATA_CODE, '')))) AS DATA_CODE
                     FROM (SELECT DISTINCT ITEM_NAME, DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '海运') A
                              FULL OUTER JOIN
                          (SELECT DISTINCT ITEM_NAME, '+火车' AS DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '铁运') B ON A.ITEM_NAME = B.ITEM_NAME
                              FULL OUTER JOIN
                          (SELECT DISTINCT ITEM_NAME, '+汽运' AS DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '汽运') C ON COALESCE(A.ITEM_NAME, B.ITEM_NAME) = C.ITEM_NAME
                              FULL OUTER JOIN
                          (SELECT DISTINCT ITEM_NAME, '+河运' AS DATA_CODE
                           FROM TRANS
                           WHERE DATA_CODE = '河运') D ON COALESCE(A.ITEM_NAME, B.ITEM_NAME, C.ITEM_NAME) = D.ITEM_NAME)
SELECT ITEM_NAME              AS 名称,
       CASE
           WHEN DATA_CODE LIKE '+%' THEN SUBSTR(DATA_CODE, 2)
           ELSE DATA_CODE END AS 运输方式,
       '运输里程'             AS 距离,
       '宝钢现场'             AS 数据来源,
       '2024年'               AS 年限
FROM TRANS_DESC1
ORDER BY ITEM_NAME
;






