;

WITH BY_PRODUCT_CODE AS (SELECT DISTINCT ITEM_CODE
                         FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA
                         WHERE FLAG = 'BY_PRODUCT'),
     DIST_RESOURCE AS (SELECT A.*,
                              B.LCI_ELEMENT_CODE,
                              0                                              AS C1,
                              COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C2,
                              0                                              AS C3,
                              0                                              AS C4,
                              0                                              AS C5
                       FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA A
                                JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_SY B
                                     ON A.FLAG = 'RESOURCE'
                                         AND A.ITEM_CODE IN (SELECT DISTINCT ITEM_CODE
                                                             FROM BY_PRODUCT_CODE)
                                         AND A.ITEM_CODE = B.ITEM_CODE
                       UNION ALL
                       SELECT A.*,
                              B.LCI_ELEMENT_CODE,
                              0                                              AS C1,
                              0                                              AS C2,
                              COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C3,
                              0                                              AS C4,
                              0                                              AS C5
                       FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA A
                                JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_SY B
                                     ON A.FLAG = 'RESOURCE'
                                         AND A.ITEM_CODE NOT IN (SELECT DISTINCT ITEM_CODE
                                                                 FROM BY_PRODUCT_CODE)
                                         AND A.ITEM_CODE = B.ITEM_CODE
                       UNION ALL
                       SELECT A.*,
                              B.LCI_ELEMENT_CODE,
                              0                                              AS C1,
                              0                                              AS C2,
                              0                                              AS C3,
                              0                                              AS C4,
                              COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C5
                       FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA A
                                JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_TRANSPORT B
                                     ON A.FLAG = 'RESOURCE' AND A.ITEM_CODE = B.ITEM_CODE),
     DIST_ENERGY AS (SELECT A.*,
                            C.LCI_ELEMENT_CODE,
                            COALESCE(A.UNIT_COST * C.C1_DIRECT, 0) AS C1,
                            COALESCE(A.UNIT_COST * C.C2_BP, 0)     AS C2,
                            COALESCE(A.UNIT_COST * C.C3_OUT, 0)    AS C3,
                            COALESCE(A.UNIT_COST * C.C4_BP_NEG, 0) AS C4,
                            COALESCE(A.UNIT_COST * C.C5_TRANS, 0)  AS C5
                     FROM (SELECT *
                           FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA
                           WHERE FLAG = 'ENERGY') A
                              JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_DATE_BATCH B
                                   ON A.UPDATE_DATE = B.UPDATE_DATE
                              JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_ENERGY_RESULT C
                                   ON B.BATCH_NUMBER = C.BATCH_NUMBER AND A.ITEM_CODE = C.PRODUCT_CODE),
     DIST_FUEL AS (SELECT A.UPDATE_DATE,
                          INDEX_CODE,
                          MAT_NO,
                          MAT_TRACK_NO,
                          MAT_SEQ_NO,
                          MAT_WT,
                          MAT_STATUS,
                          FAMILY_CODE,
                          UNIT_CODE,
                          UNIT_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          PRODUCT_VALUE,
                          TYPE_CODE,
                          TYPE_NAME,
                          A.ITEM_CODE,
                          ITEM_NAME,
                          VALUE,
                          UNITM_AC,
                          UNIT_COST,
                          'FUEL'                                         AS FLAG,
                          B.LCI_ELEMENT_CODE,
                          COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C1,
                          0                                              AS C2,
                          0                                              AS C3,
                          0                                              AS C4,
                          0                                              AS C5
                   FROM (SELECT *
                         FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA
                         WHERE TYPE_CODE < '04') A
                            JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_FUEL B
                                 ON A.ITEM_CODE = B.ITEM_CODE
                   UNION ALL
                   SELECT A.UPDATE_DATE,
                          INDEX_CODE,
                          MAT_NO,
                          MAT_TRACK_NO,
                          MAT_SEQ_NO,
                          MAT_WT,
                          MAT_STATUS,
                          FAMILY_CODE,
                          UNIT_CODE,
                          UNIT_NAME,
                          PRODUCT_CODE,
                          PRODUCT_NAME,
                          PRODUCT_VALUE,
                          TYPE_CODE,
                          TYPE_NAME,
                          A.ITEM_CODE,
                          ITEM_NAME,
                          VALUE,
                          UNITM_AC,
                          UNIT_COST,
                          'FUEL'                                               AS FLAG,
                          B.LCI_ELEMENT_CODE,
                          COALESCE(-ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C1,
                          0                                                    AS C2,
                          0                                                    AS C3,
                          0                                                    AS C4,
                          0                                                    AS C5
                   FROM (SELECT *
                         FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA
                         WHERE TYPE_CODE >= '04') A
                            JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_FUEL B
                                 ON A.ITEM_CODE = B.ITEM_CODE),
     DIST_EP AS (SELECT A.UPDATE_DATE,
                        INDEX_CODE,
                        MAT_NO,
                        MAT_TRACK_NO,
                        MAT_SEQ_NO,
                        MAT_WT,
                        MAT_STATUS,
                        FAMILY_CODE,
                        UNIT_CODE,
                        UNIT_NAME,
                        PRODUCT_CODE,
                        PRODUCT_NAME,
                        PRODUCT_VALUE,
                        TYPE_CODE,
                        TYPE_NAME,
                        A.ITEM_CODE,
                        ITEM_NAME,
                        VALUE,
                        UNITM_AC,
                        UNIT_COST,
                        CASE
                            WHEN FLAG <> 'EMISSION' THEN 'STREAM'
                            ELSE FLAG END                                   AS FLAG,
                        B.LCI_ELEMENT_CODE,
                        COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C1,
                        0                                                   AS C2,
                        0                                                   AS C3,
                        0                                                   AS C4,
                        0                                                   AS C5
                 FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA A
                          JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_EP B
                               ON A.ITEM_CODE = B.ITEM_CODE),
     DIST_WASTE AS (SELECT A.*,
                           B.LCI_ELEMENT_CODE,
                           0                                                   AS C1,
                           0                                                   AS C2,
                           0                                                   AS C3,
                           COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C4,
                           0                                                   AS C5
                    FROM (SELECT *
                          FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DATA
                          WHERE FLAG = 'WASTE') A
                             JOIN T_ADS_TEMP_LCA_SUBCLASS_CALC_FACTOR_FCP B
                                  ON A.ITEM_CODE = B.ITEM_CODE)
SELECT *
FROM DIST_RESOURCE
UNION ALL
SELECT *
FROM DIST_ENERGY
UNION ALL
SELECT *
FROM DIST_FUEL
UNION ALL
SELECT *
FROM DIST_EP
UNION ALL
SELECT *
FROM DIST_WASTE;


WITH EDGE AS (SELECT * FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_EDGE),
     PROC_META AS (SELECT DISTINCT UPDATE_DATE,
                                   INDEX_CODE,
                                   MAT_NO,
                                   MAT_TRACK_NO,
                                   MAT_SEQ_NO,
                                   MAT_WT,
                                   MAT_STATUS,
                                   FAMILY_CODE,
                                   UNIT_CODE,
                                   UNIT_NAME,
                                   PRODUCT_CODE,
                                   PRODUCT_NAME,
                                   PRODUCT_VALUE
                   FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_PROC_PRODUCT_LIST),
     DIRECT AS (SELECT INDEX_CODE,
                       LCI_ELEMENT_CODE,
                       SUM(C1) AS C1,
                       SUM(C2) AS C2,
                       SUM(C3) AS C3,
                       SUM(C4) AS C4,
                       SUM(C5) AS C5
                FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_DIST
                GROUP BY INDEX_CODE, LCI_ELEMENT_CODE),
     FLOW(NODE_INDEX,
          SOURCE_INDEX,
          LCI_ELEMENT_CODE,
          C1,
          C2,
          C3,
          C4,
          C5)
         AS (SELECT INDEX_CODE AS NODE_INDEX,
                    INDEX_CODE AS SOURCE_INDEX,
                    LCI_ELEMENT_CODE,
                    C1,
                    C2,
                    C3,
                    C4,
                    C5
             FROM DIRECT
             UNION ALL
             SELECT E.CHILD_INDEX AS NODE_INDEX,
                    SOURCE_INDEX,
                    LCI_ELEMENT_CODE,
                    C1 * E.UNIT_COST,
                    C2 * E.UNIT_COST,
                    C3 * E.UNIT_COST,
                    C4 * E.UNIT_COST,
                    C5 * E.UNIT_COST
             FROM FLOW F,
                  EDGE E
             WHERE E.PARENT_INDEX = F.NODE_INDEX),
     FLOW_SUM AS (SELECT NODE_INDEX,
                         LCI_ELEMENT_CODE,
                         SUM(C1) AS C1,
                         SUM(C2) AS C2,
                         SUM(C3) AS C3,
                         SUM(C4) AS C4,
                         SUM(C5) AS C5
                  FROM FLOW
                  GROUP BY NODE_INDEX, LCI_ELEMENT_CODE)
SELECT A.NODE_INDEX,
       B.UPDATE_DATE,
       B.MAT_NO,
       B.MAT_TRACK_NO,
       B.MAT_SEQ_NO,
       B.MAT_WT,
       B.MAT_STATUS,
       B.FAMILY_CODE,
       B.UNIT_CODE,
       B.UNIT_NAME,
       B.PRODUCT_CODE,
       B.PRODUCT_NAME,
       B.PRODUCT_VALUE,
       A.SOURCE_INDEX,
       C.UNIT_CODE            AS SOURCE_UNIT_CODE,
       C.UNIT_NAME            AS SOURCE_UNIT_NAME,
       LCI_ELEMENT_CODE,
       C1 + C2 + C3 + C4 + C5 AS C_CYCLE,
       C1 + C2                AS C_INSITE,
       C3 + C4 + C5           AS C_OUTSITE,
       C1,
       C2,
       C3,
       C4,
       C5
FROM FLOW A
         LEFT JOIN PROC_META B ON A.NODE_INDEX = B.INDEX_CODE
         LEFT JOIN PROC_META C ON A.SOURCE_INDEX = C.INDEX_CODE
;


--AUTO BATCH
--CONS --GWP

SELECT *
FROM T_ADS_WH_LCA_ITEM_CONTRAST A
         JOIN (SELECT *
               FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
               WHERE VERSION = 'NORM_Ecoinvent3.11') B ON A.UUID = B.UUID AND A.FLAG = B.FLAG;


SELECT *
FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY;



WITH NORM AS (select DISTINCT ITEM_CODE, ITEM_NAME, NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_VALUE
              from (SELECT *
                    FROM T_ADS_WH_LCA_ITEM_CONTRAST
                    WHERE PCR = 'NORM'
                      AND VERSION = 'NORM_Ecoinvent3.11'
                      AND COMPANY_CODE = 'TA'
                      AND FLAG = 'MAT'
                      AND UUID IS NOT NULL) A
                       join (SELECT *
                             FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                             WHERE PCR = 'NORM'
                               AND VERSION = 'NORM_Ecoinvent3.11'
                               AND FLAG = 'MAT'
                               AND LCI_ELEMENT_CODE = 'GWP-total') B
                            on A.UUID = B.UUID),
     CML AS (SELECT DISTINCT DATA_CODE AS ITEM_CODE, ITEM_NAME, NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_VALUE
             from (SELECT *
                   FROM T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                   WHERE BASE_CODE = 'TA'
                     AND START_TIME = '2025'
                     AND UUID IS NOT NULL) A
                      join (SELECT *
                            FROM T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                            WHERE VERSION = 'CML_Ecoinvent3.11'
                              AND FLAG = 'MAT'
                              AND LCI_ELEMENT_CODE = 'GWP-total') B
                           on A.UUID = B.UUID)
SELECT DISTINCT A.ITEM_CODE,
                A.ITEM_NAME,
                A.NAME                                         AS NORM_NAME,
                A.LCI_ELEMENT_VALUE                            AS NORM_GWP,
                B.NAME                                         AS CML_NAME,
                B.LCI_ELEMENT_VALUE                            AS CML_GWP,
                ABS(A.LCI_ELEMENT_VALUE - B.LCI_ELEMENT_VALUE) AS DIFF
FROM NORM A
         JOIN CML B ON A.ITEM_CODE = B.ITEM_CODE
ORDER BY DIFF DESC;



select distinct *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA'
  and LCI_ELEMENT_CODE = 'GWP-total'
  and FACTOR_VERSION like 'NORM%'
  and BATCH_NUMBER = '20250120250120251201YS';


2YLT_80220

select *
from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_C3_DIST
where LCI_ELEMENT_CODE = 'GWP-total'
  and PROC_KEY = '2YLT_80220';


select DISTINCT LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and LCA_PROC_CODE LIKE 'BF%'
  AND BATCH_NUMBER LIKE '2025%'
  and BATCH_NUMBER <> '20250620250620251201YS'
  AND LCA_DATA_ITEM_CODE NOT IN (select DISTINCT LCA_DATA_ITEM_CODE
                                 from T_ADS_FACT_LCA_PROC_DATA
                                 where COMPANY_CODE = 'TA'
                                   and LCA_PROC_CODE LIKE 'BF%'
                                   and BATCH_NUMBER = '20250620250620251201YS')
;
select DISTINCT *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and LCA_PROC_CODE LIKE 'BF%'
  AND BATCH_NUMBER LIKE '2025%'
  and BATCH_NUMBER <> '20250620250620251201YS'
  AND LCA_DATA_ITEM_CODE = '12100';

select *
from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
where LCI_ELEMENT_CODE = 'GWP-total';

select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where COMPANY_CODE = 'TA'
  and ITEM_CODE = 'FG01';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA'
  and FACTOR_VERSION like 'NORM%'
  and LCI_ELEMENT_CODE = 'GWP-total'
--   and PRODUCT_CODE in ('70202',
--                        'DL010',
--                        '80210',
--                        '80220',
--                        '36200',
--                        'S32201')
ORDER BY PRODUCT_CODE, START_YM;

SELECT 1
FROM SYSCAT.TABLES
WHERE TABSCHEMA = 'BG00MAC102'
  AND TABNAME = 'T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT';

SELECT NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE
FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
WHERE VERSION = 'NORM_Ecoinvent3.11'
  AND PCR = 'NORM'
  AND NAME IN ('海运', '河运', '铁运', '汽运', '柴油')
  AND LCI_ELEMENT_VALUE <> 0;

SELECT *
FROM T_ADS_TEMP_LCA_SUBCLASS_CALC_RECURSION
WHERE MAT_TRACK_NO = '20241220021312199195';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA'
  and FACTOR_VERSION like 'CML%'
  and LCI_ELEMENT_CODE = 'GWP-total'
  and BATCH_NUMBER = '20250120250120251201YS';

select *
from (select DISTINCT ITEM_CODE, ITEM_NAME, NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_VALUE
      from (SELECT *
            FROM T_ADS_WH_LCA_ITEM_CONTRAST
            WHERE PCR = 'NORM'
              AND VERSION = 'NORM_Ecoinvent3.11'
              AND COMPANY_CODE = 'TA'
              AND FLAG = 'MAT'
              AND UUID IS NOT NULL) A
               join (SELECT *
                     FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                     WHERE PCR = 'NORM'
                       AND VERSION = 'NORM_Ecoinvent3.11'
                       AND FLAG = 'MAT'
                       AND LCI_ELEMENT_CODE = 'GWP-total') B
                    on A.UUID = B.UUID);


UPDATE T_ADS_WH_LCA_ITEM_CONTRAST I
SET UUID   = 'DB2C3F5E6224E5DC0EB58798B8BE06',
    REMARK = '废铁/系统拓展 - non-default'
WHERE EXISTS (SELECT 1
              FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY L
              WHERE I.COMPANY_CODE = 'TA'
                AND I.FLAG = 'MAT'
                AND I.UUID = L.UUID
                AND L.LCI_ELEMENT_CODE = 'GWP-total'
                AND I.PCR = 'NORM'
                AND I.VERSION LIKE 'NORM%'
                AND L.NAME IN ('低合金钢', '废钢/系统拓展 - non-default')
                AND ITEM_CODE NOT IN ('63700', 'A2271'));

SELECT *
FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
WHERE NAME = '废铁/系统拓展 - non-default';



CREATE TABLE DIM_ITEM
(
    -- Surrogate key (technical)
    item_key     BIGINT
        GENERATED ALWAYS AS IDENTITY
            (START WITH 1 INCREMENT BY 1),

    -- Business key (stable, cross-environment)
    company_code VARCHAR(20) NOT NULL,
    item_code    VARCHAR(50) NOT NULL,

    -- Descriptive attributes
    item_name    VARCHAR(200),

    is_current   boolean     NOT NULL DEFAULT TRUE,

    load_time    TIMESTAMP            DEFAULT CURRENT TIMESTAMP,

    -- Constraints
    CONSTRAINT PK_DIM_ITEM
        PRIMARY KEY (item_key),

    CONSTRAINT UK_DIM_ITEM_BK
        UNIQUE (company_code, item_code)
);



CREATE TABLE EMP_UPDATE
(
    UNIQUE_ID CHAR(13) FOR BIT DATA,
    EMPNO     CHAR(6),
    TEXT      VARCHAR(1000)
);


INSERT INTO EMP_UPDATE
VALUES (GENERATE_UNIQUE(), '000020', 'Update entry...'),
       (GENERATE_UNIQUE(), '000050', 'Update entry...')

drop table EMP_UPDATE;


select sum(MAT_WT) as MAT_WT,
       sum(MAT_WT * C_CYCLE) / sum(MAT_WT) AS C_CYCLE,
       sum(MAT_WT * C_INSITE) / sum(MAT_WT) AS C_INSITE,
       sum(MAT_WT * C_OUTSITE) / sum(MAT_WT) AS C_OUTSITE,
       sum(MAT_WT * C1) / sum(MAT_WT) AS C1,
       sum(MAT_WT * C2) / sum(MAT_WT) AS C2,
       sum(MAT_WT * C3) / sum(MAT_WT) AS C3,
       sum(MAT_WT * C4) / sum(MAT_WT) AS C4,
       sum(MAT_WT * C5) / sum(MAT_WT) AS C5
from T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_GWP_RESULT_HP0001_2025_NEW
where MAT_STATUS = '27';











    CALL BG00MAC102.P_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_CALC('TA',
    '202406',
    '202406',
    'T_ADS_FACT_LCA_PROC_DATA_0002_DT',
    'T_ADS_FACT_LCA_MAIN_CAT_MATRIX_ENERGY',
    '20240620240620251125YS_CONS',
    '2025',
    'EN15804_';

SELECT * FROM T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
WHERE BATCH_NUMBER = '20240620240620251125YS_CONS';