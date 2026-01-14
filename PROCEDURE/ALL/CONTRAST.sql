WITH ITEM as (select distinct LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME, LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
              from T_ADS_FACT_LCA_PROC_DATA
              where COMPANY_CODE = 'TA'
                and START_YM > '202401'),
     PRODUCT AS (SELECT *
                 FROM ITEM
                 WHERE LCA_DATA_ITEM_CAT_CODE = '04'),
     ITEM_RAW AS (SELECT *
                  FROM ITEM
                  WHERE LCA_DATA_ITEM_CODE NOT IN (SELECT LCA_DATA_ITEM_CODE FROM PRODUCT)),
     SUBCLASS_ITEM AS (SELECT DISTINCT TYPE_CODE,
                                       TYPE_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME
                       FROM BG00MAC102.T_ADS_FACT_LCA_CR0001_2024
                       UNION
                       SELECT DISTINCT TYPE_CODE,
                                       TYPE_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME
                       FROM BG00MAC102.T_ADS_FACT_LCA_SI0001_2024
                       UNION
                       SELECT DISTINCT TYPE_CODE,
                                       TYPE_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME
                       FROM BG00MAC102.T_ADS_FACT_LCA_HP0001_2024
                       UNION
                       SELECT DISTINCT TYPE_CODE,
                                       TYPE_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME
                       FROM BG00MAC102.T_ADS_FACT_LCA_CR0001_2025
                       UNION
                       SELECT DISTINCT TYPE_CODE,
                                       TYPE_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME
                       FROM BG00MAC102.T_ADS_FACT_LCA_SI0001_2025
                       UNION
                       SELECT DISTINCT TYPE_CODE,
                                       TYPE_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME
                       FROM BG00MAC102.T_ADS_FACT_LCA_HP0001_2025),
     SUBCLASS_PRODUCT AS (SELECT *
                          FROM SUBCLASS_ITEM
                          WHERE TYPE_CODE = '04'),
     SUBCLASS_ITEM_RAW AS (SELECT *
                           FROM SUBCLASS_ITEM
                           WHERE ITEM_CODE NOT IN (SELECT ITEM_CODE
                                                   FROM SUBCLASS_PRODUCT
                                                   UNION
                                                   SELECT ITEM_CODE
                                                   FROM PRODUCT)),
     ITEM_ALL AS (SELECT *
                  FROM SUBCLASS_ITEM_RAW
                  UNION
                  SELECT *
                  FROM ITEM_RAW),
     ITEM_AGG AS (SELECT LISTAGG(TYPE_CODE, ',') AS TYPE_CODE,
                         LISTAGG(TYPE_NAME, ',') AS TYPE_NAME,
                         ITEM_CODE,
                         LISTAGG(ITEM_NAME, ',') AS ITEM_NAME
                  FROM ITEM_ALL
                  GROUP BY ITEM_CODE),
     CONTRAST AS (SELECT DISTINCT DATA_CODE, MAX(UUID) AS UUID
                  FROM (SELECT DISTINCT DATA_CODE, UUID, FLAG
                        FROM T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                        WHERE BASE_CODE = 'TA'
                          AND START_TIME = '2025')
                  GROUP BY DATA_CODE),
     FACTOR AS (SELECT DISTINCT UUID, NAME
                FROM T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                WHERE VERSION LIKE '易碳%')
SELECT *
FROM ITEM_AGG A
         LEFT JOIN CONTRAST B ON A.ITEM_CODE = B.DATA_CODE
         LEFT JOIN FACTOR C ON B.UUID = C.UUID


