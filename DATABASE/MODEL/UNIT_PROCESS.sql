SELECT DISTINCT NAME, BACKGROUND, UNIT
FROM BG00MAC102.T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
WHERE VERSION = 'NORM_Ecoinvent3.11'
  AND FLAG IN ('MAT', 'TRANSPORT');


SELECT DATASET_ID,
       SYSTEM_MODEL,
       ACTIVITY_NAME,
       PRODUCT_NAME,
       GEOGRAPHY_SHORTNAME,
       UNIT_NAME,
       UPPER(SUBSTR(BACKGROUND, 1, 1)) || SUBSTR(BACKGROUND, 2) AS BACK_INFO
FROM (SELECT DISTINCT DATASET_ID,
                      SYSTEM_MODEL,
                      ACTIVITY_NAME,
                      PRODUCT_NAME,
                      GEOGRAPHY_SHORTNAME,
                      UNIT_NAME,
                      PRODUCT_NAME || ' {' || GEOGRAPHY_SHORTNAME || '}| ' || ACTIVITY_NAME ||
                      ' | Cut-off, S' AS BACKGROUND
      FROM BG00MAC102.V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW
      WHERE DATABASE_VERSION = 'ecoinvent-v3.11'
        AND SYSTEM_MODEL = 'EN15804');


DROP TABLE BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS;

create table BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS
(
    UNIT_PROCESS_ID      VARCHAR(36)                        not null
        primary key,
    PCR                  VARCHAR(100)                       not null
        constraint FK_UNIT_PROCESS_PCR
            references BG00MAC102.T_ADS_DIM_LCA_PCR,
    DATABASE_VERSION     VARCHAR(100)                       not null
        constraint FK_UNIT_PROCESS_DATABASE_VERSION
            references BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION,
    SOURCE               VARCHAR(255),
    CATEGORY             VARCHAR(255)    default 'UPSTREAM' not null,
    UNIT_PROCESS_NAME    VARCHAR(255)                       not null,
    BACKGROUND_INFO      VARCHAR(255),
    UNIT_NAME            VARCHAR(255),
    REFERENCE_DATASET_ID VARCHAR(73)
        constraint FK_UNIT_PROCESS_DATASET_ID
            references BG00MAC102.T_ADS_DIM_LCA_DATASET,
    CONVERSION_FACTOR    DECIMAL(20, 10) default 1,
    IS_DIESEL            BOOLEAN         default FALSE      not null
)
    distribute by hash (UNIT_PROCESS_ID);

comment on table BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS is 'LCA单元过程';



DROP TABLE BG00MAC102.T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT;

create table BG00MAC102.T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT
(
    UNIT_PROCESS_ID     VARCHAR(36) not null
        constraint FK_UNIT_PROCESS_ID
            references BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS,
    IMPACT_INDICATOR_ID VARCHAR(36) not null
        constraint FK_IMPACT_INDICATOR_ID
            references BG00MAC102.T_ADS_DIM_LCA_IMPACT_INDICATOR,
    AMOUNT              DECIMAL(30, 16),
    primary key (UNIT_PROCESS_ID, IMPACT_INDICATOR_ID)
)
    distribute by hash (UNIT_PROCESS_ID, IMPACT_INDICATOR_ID);

comment on table BG00MAC102.T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT is 'LCA单元过程环境影响';


DROP VIEW BG00MAC102.V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT;

CREATE VIEW BG00MAC102.V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT AS
SELECT UP.UNIT_PROCESS_ID,
       PI.PCR_INDICATOR_ID,
       UP.PCR,
       UP.DATABASE_VERSION,
       UP.SOURCE,
       UP.CATEGORY,
       UP.UNIT_PROCESS_NAME,
       UP.BACKGROUND_INFO,
       UP.UNIT_NAME,
       UP.CONVERSION_FACTOR,
       UP.IS_DIESEL,
       PI.INDICATOR_CODE,
       PI.INDICATOR_NAME_IN_PCR,
       PI.INDICATOR_CNAME_IN_PCR,
       PI.INDICATOR_UNIT_IN_PCR,
       UPI.AMOUNT,
       UP.REFERENCE_DATASET_ID,
       PI.REFERENCE_IMPACT_INDICATOR_ID
FROM (SELECT * FROM BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS) UP
         LEFT JOIN (SELECT * FROM BG00MAC102.T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT) UPI
                   ON UP.UNIT_PROCESS_ID = UPI.UNIT_PROCESS_ID
         LEFT JOIN (SELECT * FROM T_ADS_DIM_LCA_PCR_INDICATOR) PI
                   ON UPI.IMPACT_INDICATOR_ID = PI.REFERENCE_IMPACT_INDICATOR_ID AND UP.PCR = PI.PCR;



SELECT *
FROM T_ADS_DIM_LCA_PCR_INDICATOR PI
WHERE PCR = 'BASIC'
  AND EXISTS(SELECT *
             FROM T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION FV
             WHERE VERSION = 'NORM_Ecoinvent3.11'
               AND FV.LCI_ELEMENT_CODE = PI.INDICATOR_CODE);


-- INSERT INTO T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT (UNIT_PROCESS_ID, IMPACT_INDICATOR_ID, AMOUNT)
SELECT DISTINCT UNIT_PROCESS_ID, REFERENCE_IMPACT_INDICATOR_ID, LCI_ELEMENT_VALUE AS AMOUNT
FROM (SELECT *
      FROM T_ADS_DIM_LCA_UNIT_PROCESS
      WHERE PCR = 'BASIC') UP
         LEFT JOIN (SELECT DISTINCT *
                    FROM T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                    WHERE VERSION = 'NORM_Ecoinvent3.11'
                      AND FLAG in ('MAT', 'TRANSPORT')) BF ON UP.UNIT_PROCESS_NAME = BF.NAME
         LEFT JOIN (SELECT *
                    FROM T_ADS_DIM_LCA_PCR_INDICATOR
                    WHERE PCR = 'BASIC') PI ON BF.LCI_ELEMENT_CODE = PI.INDICATOR_CODE
;


SELECT *
FROM T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT UI
WHERE EXISTS(SELECT 1
             FROM T_ADS_DIM_LCA_PCR_INDICATOR PI
             WHERE UI.IMPACT_INDICATOR_ID = PI.REFERENCE_IMPACT_INDICATOR_ID
               AND PI.PCR = 'BASIC'
               AND PI.INDICATOR_CODE = 'GWP-total')
;



create table BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS
(
    PCR              VARCHAR(100) not null
        constraint FK_ITEM_UNIT_PROCESS_PCR
            references BG00MAC102.T_ADS_DIM_LCA_PCR,
    DATABASE_VERSION VARCHAR(100) not null
        constraint FK_ITEM_UNIT_PROCESS_DATABASE_VERSION
            references BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION,
    COMPANY_CODE     VARCHAR(20)  not null,
    ITEM_CODE        VARCHAR(100) not null,
    ITEM_NAME        VARCHAR(100),
    UNIT_PROCESS_ID  VARCHAR(36)  not null
        constraint FK_ITEM_UNIT_PROCESS_UNIT_PROCESS_ID
            references BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS,
    REC_CREATOR      VARCHAR(255) default 'admin',
    REC_CREATE_TIME  TIMESTAMP(6) default CURRENT TIMESTAMP,
    REC_REVISOR      VARCHAR(255),
    REC_REVISE_TIME  TIMESTAMP(6),
    primary key (PCR, DATABASE_VERSION, COMPANY_CODE, ITEM_CODE, UNIT_PROCESS_ID)
)
    distribute by hash (PCR, DATABASE_VERSION, COMPANY_CODE, ITEM_CODE, UNIT_PROCESS_ID);

comment on table BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS is 'LCA物料单元过程匹配关系';

comment on column BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS.REC_CREATOR is '记录创建人';

comment on column BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS.REC_CREATE_TIME is '记录创建时间';

comment on column BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS.REC_REVISOR is '记录修改人';

comment on column BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS.REC_REVISE_TIME is '记录修改时间';



insert into T_ADS_BR_LCA_ITEM_UNIT_PROCESS (PCR, DATABASE_VERSION, COMPANY_CODE, ITEM_CODE, ITEM_NAME, UNIT_PROCESS_ID)
with item as (select distinct COMPANY_CODE, ITEM_CODE, ITEM_NAME, UUID
              from T_ADS_WH_LCA_ITEM_CONTRAST
              where PCR = 'NORM'
                and VERSION = 'NORM_Ecoinvent3.11'
                and FLAG = 'MAT'),
     lib as (select distinct UUID, NAME, BACKGROUND
             from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
             where PCR = 'NORM'
               and VERSION = 'NORM_Ecoinvent3.11'
               and FLAG = 'MAT'),
     up as (select UNIT_PROCESS_ID, CATEGORY, UNIT_PROCESS_NAME, BACKGROUND_INFO
            from T_ADS_DIM_LCA_UNIT_PROCESS
            where PCR = 'BASIC'
              and DATABASE_VERSION = 'ecoinvent-v3.11'),
     res as (select distinct 'BASIC'           as PCR,
                             'ecoinvent-v3.11' as DATABASE_VERSION,
                             COMPANY_CODE,
                             ITEM_CODE,
                             ITEM_NAME,
                             UNIT_PROCESS_ID
             from item
                      join lib on item.UUID = lib.UUID
                      join up on lib.NAME = up.UNIT_PROCESS_NAME)
select *
from res
;


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where FLAG = 'STREAM';

select *
from T_ADS_DIM_LCA_UNIT_PROCESS;



WITH OLD AS (select distinct UUID, NAME, BACKGROUND_DATA, LCI_UNIT AS UNIT, FLAG, LCI_ELEMENT_CODE, LCI_ELEMENT_VALUE
             from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
             WHERE VERSION = 'EN15804_Ecoinvent3.11'
               AND FLAG = 'MAT'
               AND LCI_ELEMENT_CODE = 'GWP-total'
             ORDER BY UUID),
     OLD_UNIT AS (SELECT DISTINCT NAME, BACKGROUND_DATA, UNIT, FLAG, LCI_ELEMENT_CODE, LCI_ELEMENT_VALUE
                  FROM OLD),
     ITEM as (select *
              from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
              where START_TIME = '2025'
                and FLAG in ('FCP', 'SY')
                and BASE_CODE in ('TA', 'ZG')),
     REC AS (SELECT DISTINCT BASE_CODE, DATA_CODE, NAME, BACKGROUND_DATA, LCI_ELEMENT_CODE, LCI_ELEMENT_VALUE
             FROM ITEM
                      JOIN OLD ON ITEM.UUID = OLD.UUID),
     UNIT_PROCESS as (select * from T_ADS_DIM_LCA_UNIT_PROCESS),
     DATASET AS (SELECT DATASET_ID,
                        SYSTEM_MODEL,
                        ACTIVITY_NAME,
                        PRODUCT_NAME,
                        GEOGRAPHY_SHORTNAME,
                        UNIT_NAME,
                        UPPER(SUBSTR(BACKGROUND, 1, 1)) || SUBSTR(BACKGROUND, 2) AS BACK_INFO
                 FROM (SELECT DISTINCT DATASET_ID,
                                       SYSTEM_MODEL,
                                       ACTIVITY_NAME,
                                       PRODUCT_NAME,
                                       GEOGRAPHY_SHORTNAME,
                                       UNIT_NAME,
                                       PRODUCT_NAME || ' {' || GEOGRAPHY_SHORTNAME || '}| ' || ACTIVITY_NAME ||
                                       ' | EN15804, S' AS BACKGROUND
                       FROM BG00MAC102.V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW
                       WHERE DATABASE_VERSION = 'ecoinvent-v3.11'
                         AND SYSTEM_MODEL = 'EN15804')),
     LINKED AS (SELECT *
                FROM OLD_UNIT
                         JOIN DATASET ON OLD_UNIT.BACKGROUND_DATA = DATASET.BACK_INFO)
SELECT null              as UNIT_PROCESS_ID,
       'CONS'            as PCR,
       'ecoinvent-v3.11' as DATABASE_VERSION,
       'SimaPro-v10.2'      SOURCE,
       null              as CATEGORY,
       NAME              AS UNIT_PROCESS_NAME,
       BACKGROUND_DATA   AS BACKGROUND_INFO,
       UNIT              AS UNIT_NAME,
       DATASET_ID        AS REFERENCE_DATASET_ID,
       1                 AS CONVERSION_FACTOR,
       FALSE             AS IS_DIESEL
FROM LINKED
;

SELECT *
FROM T_ADS_FACT_LCA_PROC_DATA_0002_DT
WHERE LCA_DATA_ITEM_CODE = '18064'
  AND COMPANY_CODE = 'ZG';



SELECT DISTINCT LCA_DATA_ITEM_CAT_NAME, LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
FROM T_ADS_FACT_LCA_PROC_DATA_0002_DT
WHERE COMPANY_CODE = 'TA';


SELECT *
FROM (SELECT *, COUNT(*) OVER ( PARTITION BY CATEGORY, BACKGROUND_INFO, AMOUNT, REFERENCE_DATASET_ID) AS REC_COUNT
      FROM V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT
      WHERE INDICATOR_CODE = 'GWP-total')
WHERE REC_COUNT > 1
ORDER BY REFERENCE_DATASET_ID, UNIT_PROCESS_NAME
;



WITH AMOUNT AS (SELECT DISTINCT NAME,
                                BACKGROUND_DATA,
                                CASE
                                    WHEN LCI_ELEMENT_CODE = 'EET-heat' THEN 'EET'
                                    WHEN LCI_ELEMENT_CODE = 'EET-electricity' THEN 'EEE'
                                    ELSE LCI_ELEMENT_CODE END AS LCI_ELEMENT_CODE,
                                LCI_ELEMENT_VALUE
                FROM T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                WHERE VERSION = 'EN15804_Ecoinvent3.11'
                  AND FLAG = 'MAT'),
     AMOUNT_ALL AS (SELECT *
                    FROM AMOUNT
                    UNION
                    SELECT NAME, BACKGROUND_DATA, 'EE' AS LCI_ELEMENT_CODE, SUM(LCI_ELEMENT_VALUE) AS LCI_ELEMENT_VALUE
                    FROM AMOUNT
                    WHERE LCI_ELEMENT_CODE IN ('EET', 'EEE')
                    GROUP BY NAME, BACKGROUND_DATA),
     UNIT_PROCESS AS (SELECT *
                      FROM T_ADS_DIM_LCA_UNIT_PROCESS
                      WHERE PCR = 'CONS'),
     INDICATOR AS (SELECT *
                   FROM T_ADS_DIM_LCA_PCR_INDICATOR
                   WHERE PCR = 'CONS')
SELECT UNIT_PROCESS_ID, REFERENCE_IMPACT_INDICATOR_ID, LCI_ELEMENT_VALUE AS AMOUNT
FROM UNIT_PROCESS
         JOIN AMOUNT_ALL ON UNIT_PROCESS.UNIT_PROCESS_NAME = AMOUNT_ALL.NAME
         JOIN INDICATOR ON AMOUNT_ALL.LCI_ELEMENT_CODE = INDICATOR.INDICATOR_CODE
;


with zg as (select distinct *
            from T_ADS_FACT_LCA_PROC_DATA_0002_DT
            where BATCH_NUMBER in ('20250120251220260120YS', '20240120241220260122YS')
              and COMPANY_CODE = 'ZG'),
     item1 as (select distinct LCA_DATA_ITEM_CAT_NAME,
                               LCA_DATA_ITEM_CODE,
                               LCA_DATA_ITEM_NAME
               from zg),
     PRODUCT as (select distinct PRODUCT_CODE from zg),
     ITEM AS (SELECT *, COUNT(*) over ( partition by LCA_DATA_ITEM_CODE) as ITEM_COUNT
              FROM item1
              where LCA_DATA_ITEM_CODE not in (select PRODUCT_CODE from product)),
     BYPROD AS (SELECT * FROM ITEM WHERE LCA_DATA_ITEM_CAT_NAME = '副产品'),
     SUBCLASS_PROD AS (select distinct item_code, item_name
                       from (select distinct item_code, item_name
                             from T_ADS_FACT_LCA_BGZG_CR0001_2024_NEW
                             where TYPE_CODE in ('04', '05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_BGZG_CR0001_2025_NEW
                             where TYPE_CODE in ('04', '05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_BGZG_CR0001_2026_NEW
                             where TYPE_CODE in ('04', '05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_BGZG_HP0001_2024_NEW
                             where TYPE_CODE in ('04', '05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_BGZG_HP0001_2025_NEW
                             where TYPE_CODE in ('04', '05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_BGZG_HP0001_2026_NEW
                             where TYPE_CODE in ('04', '05'))),
     CONTRAST1 AS (SELECT *
                   FROM T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                   WHERE START_TIME = '2025'
                     AND BASE_CODE = 'ZG'
                     AND FLAG IN ('FCP', 'SY')),
     CONSTRAST AS (select DISTINCT DATA_CODE, UUID, FLAG
                   from CONTRAST1
                   WHERE DATA_CODE NOT IN (SELECT LCA_DATA_ITEM_CODE FROM ITEM WHERE ITEM_COUNT > 1)
                     AND DATA_CODE NOT IN (SELECT LCA_DATA_ITEM_CODE FROM BYPROD)
                     AND DATA_CODE NOT IN (SELECT PRODUCT_CODE FROM PRODUCT)
                     AND DATA_CODE NOT IN (SELECT ITEM_CODE FROM SUBCLASS_PROD))
SELECT distinct DATA_CODE, UUID
FROM CONSTRAST;


with TA as (select distinct *
            from T_ADS_FACT_LCA_PROC_DATA_0002_DT
            where BATCH_NUMBER in ('20240120241220251118YS')
              and COMPANY_CODE = 'TA'),
     item1 as (select distinct LCA_DATA_ITEM_CAT_NAME,
                               LCA_DATA_ITEM_CODE,
                               LCA_DATA_ITEM_NAME
               from TA),
     PRODUCT as (select distinct PRODUCT_CODE from TA),
     ITEM AS (SELECT *, COUNT(*) over ( partition by LCA_DATA_ITEM_CODE) as ITEM_COUNT
              FROM item1
              where LCA_DATA_ITEM_CODE not in (select PRODUCT_CODE from product)),
     BYPROD AS (SELECT * FROM ITEM WHERE LCA_DATA_ITEM_CAT_NAME = '副产品'),
     SUBCLASS_PROD AS (select distinct item_code, item_name
                       from (select distinct item_code, item_name
                             from T_ADS_FACT_LCA_CR0001_2024_NEW
                             where TYPE_CODE in ('05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_CR0001_2025_NEW
                             where TYPE_CODE in ('05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_HP0001_2024_NEW
                             where TYPE_CODE in ('05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_HP0001_2025_NEW
                             where TYPE_CODE in ('05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_SI0001_2024_NEW
                             where TYPE_CODE in ('05')
                             union
                             select distinct item_code, item_name
                             from T_ADS_FACT_LCA_SI0001_2025_NEW
                             where TYPE_CODE in ('05'))),
     CONTRAST1 AS (SELECT *
                   FROM T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                   WHERE START_TIME = '2025'
                     AND BASE_CODE = 'TA'
                     AND FLAG IN ('FCP', 'SY')),
     CONSTRAST AS (select DISTINCT DATA_CODE, UUID, FLAG
                   from CONTRAST1
                   WHERE DATA_CODE NOT IN (SELECT LCA_DATA_ITEM_CODE FROM ITEM WHERE ITEM_COUNT > 1)
                     AND DATA_CODE NOT IN (SELECT LCA_DATA_ITEM_CODE FROM BYPROD)
                     AND DATA_CODE NOT IN (SELECT PRODUCT_CODE FROM PRODUCT)
                     AND DATA_CODE NOT IN (SELECT ITEM_CODE FROM SUBCLASS_PROD))
SELECT distinct DATA_CODE, UUID
FROM CONSTRAST;


WITH BASIC AS (SELECT DISTINCT COMPANY_CODE, ITEM_CODE, ITEM_NAME
               FROM T_ADS_BR_LCA_ITEM_UNIT_PROCESS
               WHERE PCR = 'BASIC'),
     CONS AS (SELECT DISTINCT COMPANY_CODE, ITEM_CODE, ITEM_NAME
              FROM T_ADS_BR_LCA_ITEM_UNIT_PROCESS
              WHERE PCR = 'CONS')
SELECT *
FROM BASIC
         JOIN CONS ON BASIC.COMPANY_CODE = CONS.COMPANY_CODE AND BASIC.ITEM_CODE = CONS.ITEM_CODE;


-- UPDATE T_ADS_BR_LCA_ITEM_UNIT_PROCESS A
SET A.ITEM_NAME = (SELECT B.ITEM_NAME
                   FROM (SELECT MAIN_CAT.COMPANY_CODE, MAIN_CAT.ITEM_CODE, MAIN_CAT.ITEM_NAME
                         FROM (SELECT DISTINCT COMPANY_CODE,
                                               LCA_DATA_ITEM_CODE               AS ITEM_CODE,
                                               LCA_DATA_ITEM_NAME               AS ITEM_NAME,
                                               RANK() over (PARTITION BY
                                                   COMPANY_CODE, LCA_DATA_ITEM_CODE
                                                   ORDER BY LCA_DATA_ITEM_NAME) AS NAME_RANK
                               FROM (SELECT DISTINCT COMPANY_CODE,
                                                     LCA_DATA_ITEM_CODE,
                                                     LCA_DATA_ITEM_NAME
                                     FROM T_ADS_FACT_LCA_PROC_DATA
                                     WHERE START_YM > '202401'
                                     UNION
                                     SELECT DISTINCT COMPANY_CODE,
                                                     LCA_DATA_ITEM_CODE,
                                                     LCA_DATA_ITEM_NAME
                                     FROM T_ADS_FACT_LCA_PROC_DATA_0002_DT
                                     WHERE START_YM > '202401')) MAIN_CAT
                                  JOIN (SELECT DISTINCT COMPANY_CODE, ITEM_CODE FROM T_ADS_BR_LCA_ITEM_UNIT_PROCESS) BR
                                       ON MAIN_CAT.COMPANY_CODE = BR.COMPANY_CODE AND MAIN_CAT.ITEM_CODE = BR.ITEM_CODE
                         WHERE MAIN_CAT.NAME_RANK = 1) B
                   WHERE A.ITEM_CODE = B.ITEM_CODE
                     AND A.COMPANY_CODE = B.COMPANY_CODE);
;




