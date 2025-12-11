WITH DATA_NORM AS (SELECT *
                   FROM (SELECT PROC_KEY,
                                PROC_CODE,
                                PROC_NAME,
                                PRODUCT_NAME,
                                A.ITEM_CODE,
                                A.ITEM_NAME,
                                A.VALUE,
                                A.UNIT,
                                B.LCI_ELEMENT_CODE,
                                B.LCI_ELEMENT_NAME,
                                B.LCI_ELEMENT_VALUE,
                                CASE
                                    WHEN ITEM_CAT_CODE < '04' THEN A.VALUE * B.LCI_ELEMENT_VALUE
                                    ELSE -ABS(A.VALUE * B.LCI_ELEMENT_VALUE) END AS LOAD
                         FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA_NORM A
                                  INNER JOIN (SELECT *
                                              FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_FUEL) B
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
                                B.LCI_ELEMENT_CODE,
                                B.LCI_ELEMENT_NAME,
                                B.LCI_ELEMENT_VALUE,
                                ABS(A.VALUE) * B.LCI_ELEMENT_VALUE AS LOAD
                         FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA_NORM A
                                  INNER JOIN (SELECT *
                                              FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_EP) B
                                             ON A.ITEM_CODE = B.ITEM_CODE)
                   WHERE LOAD IS NOT NULL),
     DIST_NORM AS (SELECT *
                   FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST_NORM
                   WHERE LOAD IS NOT NULL),
     PROC_PRODUCT_LIST AS (SELECT *
                           FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST),
     CO_DIST AS (SELECT * FROM DIST_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')),
     COKE_LOAD AS (SELECT PROC_KEY,
                          PROC_CODE,
                          PROC_NAME,
                          PRODUCT_NAME,
                          ITEM_CODE,
                          ITEM_NAME,
                          UNIT_COST * 0.7638 AS UNIT_COST,
                          LCI_ELEMENT_CODE,
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
                         LCI_ELEMENT_CODE,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.1063  AS LOAD
                  FROM (SELECT *
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COMQ') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')) B ON 1 = 1),
     COZQ1 AS (SELECT A.PROC_KEY,
                      A.PROC_CODE,
                      A.PROC_NAME,
                      A.PRODUCT_NAME,
                      ITEM_CODE,
                      ITEM_NAME,
                      CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0401 *
                      (SELECT PRODUCT_VALUE
                       FROM PROC_PRODUCT_LIST
                       WHERE PROC_CODE = 'COZQ1') /
                      ((SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ1') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ2') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ3')) AS UNIT_COST,
                      LCI_ELEMENT_CODE,
                      LCI_ELEMENT_NAME,
                      LCI_ELEMENT_VALUE,
                      CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0401 *
                      (SELECT PRODUCT_VALUE
                       FROM PROC_PRODUCT_LIST
                       WHERE PROC_CODE = 'COZQ1') /
                      ((SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ1') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ2') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ3')) AS LOAD
               FROM (SELECT *
                     FROM PROC_PRODUCT_LIST
                     WHERE PROC_CODE = 'COZQ1') A
                        JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')) B
                             ON 1 = 1),
     COZQ2 AS (SELECT A.PROC_KEY,
                      A.PROC_CODE,
                      A.PROC_NAME,
                      A.PRODUCT_NAME,
                      ITEM_CODE,
                      ITEM_NAME,
                      CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0401 *
                      (SELECT PRODUCT_VALUE
                       FROM PROC_PRODUCT_LIST
                       WHERE PROC_CODE = 'COZQ2') /
                      ((SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ1') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ2') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ3')) AS UNIT_COST,
                      LCI_ELEMENT_CODE,
                      LCI_ELEMENT_NAME,
                      LCI_ELEMENT_VALUE,
                      CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0401 *
                      (SELECT PRODUCT_VALUE
                       FROM PROC_PRODUCT_LIST
                       WHERE PROC_CODE = 'COZQ2') /
                      ((SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ1') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ2') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ3')) AS LOAD
               FROM (SELECT *
                     FROM PROC_PRODUCT_LIST
                     WHERE PROC_CODE = 'COZQ2') A
                        JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')) B
                             ON 1 = 1),
     COZQ3 AS (SELECT A.PROC_KEY,
                      A.PROC_CODE,
                      A.PROC_NAME,
                      A.PRODUCT_NAME,
                      ITEM_CODE,
                      ITEM_NAME,
                      CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0401 *
                      (SELECT PRODUCT_VALUE
                       FROM PROC_PRODUCT_LIST
                       WHERE PROC_CODE = 'COZQ3') /
                      ((SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ1') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ2') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ3')) AS UNIT_COST,
                      LCI_ELEMENT_CODE,
                      LCI_ELEMENT_NAME,
                      LCI_ELEMENT_VALUE,
                      CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0401 *
                      (SELECT PRODUCT_VALUE
                       FROM PROC_PRODUCT_LIST
                       WHERE PROC_CODE = 'COZQ3') /
                      ((SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ1') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ2') +
                       (SELECT PRODUCT_VALUE
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'COZQ3')) AS LOAD
               FROM (SELECT *
                     FROM PROC_PRODUCT_LIST
                     WHERE PROC_CODE = 'COZQ3') A
                        JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('CO01', 'CO04', 'CO03')) B
                             ON 1 = 1),
     BF_DIST AS (SELECT *
                 FROM DIST_NORM
                 WHERE PROC_CODE IN ('BF01', 'BF02', 'BF03', 'BF04'))
        ,
     IRON_LOAD AS (SELECT PROC_KEY,
                          PROC_CODE,
                          PROC_NAME,
                          PRODUCT_NAME,
                          ITEM_CODE,
                          ITEM_NAME,
                          UNIT_COST * 0.9322 AS UNIT_COST,
                          LCI_ELEMENT_CODE,
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
                         LCI_ELEMENT_CODE,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0545  AS LOAD
                  FROM (SELECT *
                        FROM PROC_PRODUCT_LIST
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
                         LCI_ELEMENT_CODE,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0094  AS LOAD
                  FROM (SELECT *
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = 'BFDL') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('BF01', 'BF02', 'BF03', 'BF04')) B
                                ON 1 = 1),
     BOF_DIST AS (SELECT *
                  FROM DIST_NORM
                  WHERE PROC_CODE IN ('1YLT', '2YLT'))
        ,
     STEEL_LOAD AS (SELECT PROC_KEY,
                           PROC_CODE,
                           PROC_NAME,
                           PRODUCT_NAME,
                           ITEM_CODE,
                           ITEM_NAME,
                           UNIT_COST * 0.9877 AS UNIT_COST,
                           LCI_ELEMENT_CODE,
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
                         LCI_ELEMENT_CODE,
                         LCI_ELEMENT_NAME,
                         LCI_ELEMENT_VALUE,
                         CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0084  AS LOAD
                  FROM (SELECT *
                        FROM PROC_PRODUCT_LIST
                        WHERE PROC_CODE = '1LDG') A
                           JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('1YLT', '2YLT')) B
                                ON 1 = 1),
     LDZQ AS (SELECT A.PROC_KEY,
                     A.PROC_CODE,
                     A.PROC_NAME,
                     A.PRODUCT_NAME,
                     ITEM_CODE,
                     ITEM_NAME,
                     CAST(B.VALUE AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0036 AS UNIT_COST,
                     LCI_ELEMENT_CODE,
                     LCI_ELEMENT_NAME,
                     LCI_ELEMENT_VALUE,
                     CAST(B.LOAD AS DOUBLE) / CAST(A.PRODUCT_VALUE AS DOUBLE) * 0.0036  AS LOAD
              FROM (SELECT *
                    FROM PROC_PRODUCT_LIST
                    WHERE PROC_CODE = 'LDZQ') A
                       JOIN (SELECT * FROM DATA_NORM WHERE PROC_CODE IN ('1YLT', '2YLT')) B
                            ON 1 = 1),
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
                  FROM LDG_LOAD
                  UNION
                  SELECT *
                  FROM COZQ1
                  UNION
                  SELECT *
                  FROM COZQ2
                  UNION
                  SELECT *
                  FROM COZQ3
                  UNION
                  SELECT *
                  FROM LDZQ),
     REST_LOAD AS (SELECT *
                   FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_C1_DIST_CONS
                   WHERE LOAD IS NOT NULL
                     AND PROC_KEY NOT IN (SELECT DISTINCT PROC_KEY FROM SUM_LOAD))
        ,
     RESULT AS (SELECT *
                FROM REST_LOAD
                UNION
                SELECT *
                FROM SUM_LOAD)
SELECT *
FROM RESULT;

select * from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT;
