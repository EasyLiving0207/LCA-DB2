CALL BG00MAC102.P_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_CALC(
        'TA',
        '202401',
        '202412',
        'T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT',
        'T_ADS_FACT_LCA_MAIN_CAT_MATRIX',
        '20240120241220250819YS',
        '2025',
        'CML_Ecoinvent3.11'
     );


CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_GWP_NORM_CALC(
        V_COMPANY_CODE => 'TA',
        V_START_MONTH => NULL,
        V_END_MONTH => NULL,
        V_FACTOR_YEAR => '2025',
        V_FACTOR_VERSION => 'CML_Ecoinvent3.11',
        V_MAIN_CAT_RESULT_TAB_NAME => 'T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT',
        V_MAIN_CAT_BATCH_NUMBER => '20240120241220250909YS',
        V_SUBCLASS_TAB_NAME => 'T_ADS_FACT_LCA_HP0001_2024_TEST',
        V_SUBCLASS_RESULT_TAB_NAME => 'T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_RESULT_TA_HP0001'
     );

CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_EPD_NORM_CALC(
        'TA',
        NULL,
        NULL,
        '2025',
        'CML_Ecoinvent3.11',
        'T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT',
        '20240120241220250909YS',
        'T_ADS_FACT_LCA_HP0001_2024_TEST',--活动数据表
        'T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_RESULT_TA_HP0001_2024'--结果表
     );



update T_ADS_FACT_LCA_PROC_DATA_0002_DT
set LCA_DATA_ITEM_CODE = '50110',
    LCA_DATA_ITEM_NAME = '电-外购-光伏电'
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220251118YS_GREEN'
  and LCA_DATA_ITEM_CODE = 'DL010';

select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220251118YS_GREEN';


select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and LCA_DATA_ITEM_CODE in ('50110', '595', '58081', '58381');

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where NAME like '%光伏电%';

select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where BASE_CODE = 'TA'
  and UUID = '4e3f11812ef545e49243520a59ad15ab';

create table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_RESULT_TA_HP0001_2024
(
    REC_ID                VARCHAR(64),
    SUBCLASS_TAB_NAME     VARCHAR(64),
    COMPANY_CODE          VARCHAR(8),
    MAIN_CAT_BATCH_NUMBER VARCHAR(64),
    FACTOR_VERSION        VARCHAR(100),
    UPDATE_DATE           VARCHAR(6),
    INDEX_CODE            VARCHAR(1000),
    MAT_NO                VARCHAR(64),
    MAT_TRACK_NO          VARCHAR(64),
    MAT_SEQ_NO            BIGINT,
    MAT_WT                DECIMAL(18, 3),
    MAT_STATUS            VARCHAR(10),
    FAMILY_CODE           VARCHAR(1000),
    UNIT_CODE             VARCHAR(100),
    UNIT_NAME             VARCHAR(256),
    PRODUCT_CODE          VARCHAR(100),
    PRODUCT_NAME          VARCHAR(256),
    PRODUCT_VALUE         DECIMAL(27, 6),
    LCI_ELEMENT_CODE      VARCHAR(256),
    LCI_ELEMENT_CNAME     VARCHAR(256),
    C_CYCLE               DOUBLE,
    C1                    DOUBLE,
    C2                    DOUBLE,
    C3                    DOUBLE,
    C4                    DOUBLE,
    C5                    DOUBLE,
    C_INSITE              DOUBLE,
    C_OUTSITE             DOUBLE,
    REC_CREATOR           VARCHAR(32),
    REC_CREATE_TIME       VARCHAR(32),
    REC_REVISOR           VARCHAR(32),
    REC_REVISE_TIME       VARCHAR(32)
)
    distribute by hash (REC_ID);



select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA';

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA';

select distinct BATCH_NUMBER
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'RG'
  and BATCH_NUMBER = '20250120250920251011YS'
  and LCI_ELEMENT_CODE = 'GWP-total';

select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'RG'
  and BATCH_NUMBER = '20250120250920251011YS'
order by LCA_PROC_CODE, PRODUCT_CODE, LCA_DATA_ITEM_CAT_CODE;

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where LCI_ELEMENT_CODE = 'GWP-total';

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where VERSION like 'EF%';


drop table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_GWP_NORM_MONTH_RESULT_TA_CR0001_2024;

create table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_GWP_NORM_MONTH_RESULT_TA_CR0001_2024
(
    REC_ID                VARCHAR(64),
    SUBCLASS_TAB_NAME     VARCHAR(64),
    COMPANY_CODE          VARCHAR(8),
    MAIN_CAT_BATCH_NUMBER VARCHAR(64),
    FACTOR_VERSION        VARCHAR(100),
    UPDATE_DATE           VARCHAR(6),
    INDEX_CODE            VARCHAR(1000),
    MAT_NO                VARCHAR(64),
    MAT_TRACK_NO          VARCHAR(64),
    MAT_SEQ_NO            BIGINT,
    FAMILY_CODE           VARCHAR(1000),
    UNIT_CODE             VARCHAR(100),
    UNIT_NAME             VARCHAR(256),
    PRODUCT_CODE          VARCHAR(100),
    PRODUCT_NAME          VARCHAR(256),
    PRODUCT_VALUE         DECIMAL(27, 6),
    MAT_WT                DECIMAL(18, 3),
    MAT_STATUS            VARCHAR(10),
    DEPT_NAME             VARCHAR(256),
    DEPT_CODE             VARCHAR(10),
    DEPT_MID_NAME         VARCHAR(20),
    LCI_ELEMENT_CODE      VARCHAR(256),
    LCI_ELEMENT_CNAME     VARCHAR(256),
    C_CYCLE               DOUBLE,
    C1                    DOUBLE,
    C2                    DOUBLE,
    C3                    DOUBLE,
    C4                    DOUBLE,
    C5                    DOUBLE,
    C_INSITE              DOUBLE,
    C_OUTSITE             DOUBLE,
    REC_CREATOR           VARCHAR(32),
    REC_CREATE_TIME       VARCHAR(32),
    REC_REVISOR           VARCHAR(32),
    REC_REVISE_TIME       VARCHAR(32)
)
    distribute by hash (REC_ID);

comment on table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_GWP_NORM_MONTH_RESULT_TA_CR0001_2024 is 'EPD普通钢宝山细类月度结果';

drop table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_GWP_NORM_MONTH_RESULT_TA_CR0001_2024_PARALLEL;

create table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_GWP_NORM_MONTH_RESULT_TA_CR0001_2024_PARALLEL
(
    REC_ID                VARCHAR(64),
    SUBCLASS_TAB_NAME     VARCHAR(64),
    COMPANY_CODE          VARCHAR(8),
    MAIN_CAT_BATCH_NUMBER VARCHAR(64),
    FACTOR_VERSION        VARCHAR(100),
    UPDATE_DATE           VARCHAR(6),
    INDEX_CODE            VARCHAR(1000),
    MAT_NO                VARCHAR(64),
    MAT_TRACK_NO          VARCHAR(64),
    MAT_SEQ_NO            BIGINT,
    FAMILY_CODE           VARCHAR(1000),
    UNIT_CODE             VARCHAR(100),
    UNIT_NAME             VARCHAR(256),
    PRODUCT_CODE          VARCHAR(100),
    PRODUCT_NAME          VARCHAR(256),
    PRODUCT_VALUE         DECIMAL(27, 6),
    MAT_WT                DECIMAL(18, 3),
    MAT_STATUS            VARCHAR(10),
    TYPE_CODE             VARCHAR(20),
    TYPE_NAME             VARCHAR(64),
    ITEM_CODE             VARCHAR(100),
    ITEM_NAME             VARCHAR(256),
    VALUE                 DECIMAL(27, 6),
    UNITM_AC              VARCHAR(64),
    UNIT_COST             DOUBLE,
    DEPT_NAME             VARCHAR(256),
    DEPT_CODE             VARCHAR(10),
    DEPT_MID_NAME         VARCHAR(20),
    LCI_ELEMENT_CODE      VARCHAR(256),
    LCI_ELEMENT_CNAME     VARCHAR(256),
    C_CYCLE               DOUBLE,
    C1                    DOUBLE,
    C2                    DOUBLE,
    C3                    DOUBLE,
    C4                    DOUBLE,
    C5                    DOUBLE,
    C_INSITE              DOUBLE,
    C_OUTSITE             DOUBLE,
    FLAG                  VARCHAR(32),
    REC_CREATOR           VARCHAR(32),
    REC_CREATE_TIME       VARCHAR(32),
    REC_REVISOR           VARCHAR(32),
    REC_REVISE_TIME       VARCHAR(32)
)
    distribute by hash (REC_ID);



INSERT INTO BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
(REC_ID, BATCH_NUMBER, START_YM, END_YM, COMPANY_CODE, SOURCE_PROC_KEY, SOURCE_PROC_CODE, SOURCE_PROC_NAME,
 TARGET_PROC_KEY, TARGET_PROC_CODE, TARGET_PROC_NAME, PRODUCT_CODE, PRODUCT_NAME, ITEM_CODE, ITEM_NAME,
 LCI_ELEMENT_CODE, TYPE, LOAD)
SELECT ''                       AS REC_ID,
       '20240120241220250819YS' AS BATCH_NUMBER,
       '202401'                 AS START_YM,
       '202412'                 AS END_YM,
       'TA'                     AS COMPANY_CODE,
       A.SOURCE_PROC_KEY,
       B.PROC_CODE              AS SOURCE_PROC_CODE,
       B.PROC_NAME              AS SOURCE_PROC_NAME,
       A.TARGET_PROC_KEY,
       C.PROC_CODE              AS TARGET_PROC_CODE,
       C.PROC_NAME              AS TARGET_PROC_NAME,
       C.PRODUCT_CODE           AS PRODUCT_CODE,
       C.PRODUCT_NAME           AS PRODUCT_NAME,
       ITEM_CODE,
       ITEM_NAME,
       LCI_ELEMENT_CODE,
       TYPE,
       LOAD
FROM (SELECT *
      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_C1_CYCLE
      UNION
      SELECT *
      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_C2_CYCLE
      UNION
      SELECT *
      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_C3_CYCLE
      UNION
      SELECT *
      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_C4_CYCLE
      UNION
      SELECT *
      FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_C5_CYCLE) A
         JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_PROC_PRODUCT_LIST B ON A.SOURCE_PROC_KEY = B.PROC_KEY
         JOIN T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_PROC_PRODUCT_LIST C ON A.TARGET_PROC_KEY = C.PROC_KEY;



select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
where BATCH_NUMBER = '20240120241220250819YS'
  and COMPANY_CODE = 'TA'
  and TARGET_PROC_KEY = 'C111_N0210'
;

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where BATCH_NUMBER = '20240120241220250819YS'
  and COMPANY_CODE = 'TA';


update T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
set LCI_ELEMENT_VALUE = LCI_ELEMENT_VALUE * 0.735
where VERSION = 'CML_Ecoinvent3.11'
  and NAME in ('液化天然气');

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = 'CML_Ecoinvent3.11'
  and NAME in ('液化天然气');

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
where BATCH_NUMBER = '20240120241220250819YS'
  and COMPANY_CODE = 'TA'
;

select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where BATCH_NUMBER = '20240120241220250928YS'
  and COMPANY_CODE = 'TA';

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where BATCH_NUMBER = '20240120241220250928YS_CONS'
  and COMPANY_CODE = 'TA'
order by LCI_ELEMENT_CODE, PROC_KEY;

DELETE
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = 'CML_Ecoinvent3.11'
  AND UUID in ('8d2d0806be9f49a187b91b378de5aaf7',
               'acb11c74dfa344648c537d7fbb63d446',
               '08182819bdf14d5c8ba9214fa5555eea',
               '3dfcf36e408f43a288616ace5166f0a3',
               'f6392975fb1546a79a66e91653fc607c',
               'dda59f1e6a264b4ca5012a8e4adf158d',
               '2809099c0c644eb3a9cfc4c03b0580fb',
               '3b6b1f76586e4d15a2a0e3038d87e830',
               '88512fea68bb48f4a8ed60f50c523474',
               '5d062bb35717443cacdd511d81c749b9',
               'ec68c61946d7426a8b9d00b2cbc94188',
               '799b8713ad584fd19acffd4cccee409a',
               '6a996eab107a4bb3a88f591392bced7b',
               'a19356e7648f49698c4412a8335ee728',
               'efade80591194b67b3236bf920eda17a',
               '6b2552e95a2b47549b7f60f026a811c5',
               'b8593f3ed1f94656b7699633140ab383',
               'f2826864080e42d4ac6fcd52eddd965e',
               '10619a5edee340fd88ad7b1b8edc1e79',
               'b991b591425340dd9442c00671470ff2',
               'b11b829822b54fadb1d81c36e2e1a2d3',
               'bff9969de0344cadb86656348da3b4cd',
               '7dd730b01baf46dcbfe2adb5301f75d9',
               'd15cdaf161f14d2bae7c74896e5cb747',
               '7b5d90803ed04862a1a93f27bb4ce846');

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where UUID in ('7dd730b01baf46dcbfe2adb5301f75d9', 'd15cdaf161f14d2bae7c74896e5cb747');


select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'ZG';

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = 'CML_Ecoinvent3.11'
  and flag = 'STREAM';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where COMPANY_CODE = 'ZG';


call BG00MAC102.P_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_CALC('TA', '202401', '202412',
                                                      'T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT',
                                                      'T_ADS_FACT_LCA_MAIN_CAT_MATRIX', '20240120241220250909YS',
                                                      '2025', '易碳_Ecoinvent3.8');



call sysproc.admin_cmd('reorg table T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT');



WITH DIFF AS (select B.batch_number,
                     B.factor_version,
                     B.company_code,
                     B.PROC_key,
                     B.proc_code,
                     B.proc_name,
                     B.product_code,
                     B.product_name,
                     B.lci_element_code,
                     B.c_cycle,
                     A.G_CYCLE,
                     B.C_CYCLE - A.G_CYCLE as DIFF
              from (select *
                    from T_CALC_MAIN_RESULT
                    where COMPANY_CODE = 'TA'
                      and BATCH_NUMBER = '20240120241220250909YS'
                      and LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计') A
                       join (select *
                             from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
                             where COMPANY_CODE = 'TA'
                               and FACTOR_VERSION = '易碳_Ecoinvent3.8'
                               and BATCH_NUMBER = '20240120241220250909YS'
                               and LCI_ELEMENT_CODE = 'GWP-total') B
                            on A.PROC_CODE = B.PROC_CODE
                                and A.PRODUCT_NAME = B.PRODUCT_NAME)
SELECT *, CAST(DIFF AS DOUBLE) / CAST(G_CYCLE AS DOUBLE) AS PERCENTAGE
FROM DIFF
ORDER BY PROC_KEY
;

SELECT *
FROM T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
WHERE BATCH_NUMBER = '20240120241220250909YS'
  AND COMPANY_CODE = 'TA'
  AND LCI_ELEMENT_CODE = 'GWP-total'
  AND TARGET_PROC_KEY = 'D002_930';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
where TARGET_PROC_KEY like 'CO%'
  and LCI_ELEMENT_CODE = 'GWP-total'
  and BATCH_NUMBER = '20240120241220250909YS';

truncate table T_ADS_FACT_CALC_MAIN_MID_RESULT_NEW immediate;
;


with data as (select *
              from T_ADS_FACT_LCA_PROC_DATA
              where COMPANY_CODE = 'TA'
                and BATCH_NUMBER = '20240120241220250909YS'),
     norm_dist as (select *
                   from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
                   where BATCH_NUMBER = '20240120241220250909YS'
                     and COMPANY_CODE = 'TA'
                     and TYPE != 'C1'),
     exception1 as (select distinct LCA_DATA_ITEM_CODE
                    from T_ADS_FACT_CALC_MAIN_MID_RESULT_NEW
                    where TYPE != 1
                    except
                    select distinct ITEM_CODE
                    from norm_dist),
     exception2 as (select distinct ITEM_CODE
                    from norm_dist
                    except
                    select distinct LCA_DATA_ITEM_CODE
                    from T_ADS_FACT_CALC_MAIN_MID_RESULT_NEW
                    where TYPE != 1),
     data1 as (select B.*
               from exception1 A
                        JOIN data B
                             on A.LCA_DATA_ITEM_CODE = B.LCA_DATA_ITEM_CODE),
     data2 as (select B.*
               from exception2 A
                        JOIN data B
                             on A.ITEM_CODE = B.LCA_DATA_ITEM_CODE),
     energy_procs as (select distinct LCA_PROC_CODE, PRODUCT_CODE
                      from data1
                      where LCA_DATA_ITEM_CAT_CODE = '04'),
     extra_byprod as (select distinct ITEM_CODE, ITEM_NAME
                      from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
                      where BATCH_NUMBER = '20240120241220250909YS'
                        and COMPANY_CODE = 'TA'
                        and TYPE = 'C2'
                        and TARGET_PROC_CODE not in (select LCA_PROC_CODE from energy_procs)
                        and SOURCE_PROC_CODE not in (select LCA_PROC_CODE from energy_procs)
                      except
                      select distinct LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
                      from T_ADS_FACT_CALC_MAIN_MID_RESULT_NEW
                      where TYPE = 2
                         or TYPE = 7)
select distinct LCA_DATA_ITEM_NAME
from T_ADS_FACT_CALC_MAIN_MID_RESULT_NEW
where type = 2;



select distinct BIGCLASS_NAME, LCA_DATA_ITEM_CODE, TRANS_MTHD_CODE, TRANS_DISTANCE, UNIT
from T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA
where ORG_CODE = 'TA';


select *
from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
WHERE company_code = 'TA'
  AND BATCH_NUMBER LIKE '20240120241220251118YS%';


SELECT *
FROM T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
WHERE UUID = '4db6408f89ec4612a103f96761272966';



select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220251118YS_GREEN';

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220251118YS_GREEN_CONS';

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION like 'CML%'
  and LCI_ELEMENT_CODE = 'GWP-total'
  and UUID = 'f6392975fb1546a79a66e91653fc607c';

select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where DATA_CODE = '18172';


--   18071	副产品-水渣


select lca_proc_code,
       lca_proc_name,
       product_code,
       product_name,
       lca_data_item_cat_code,
       lca_data_item_cat_name,
       lca_data_item_code,
       lca_data_item_name,
       value,
       unit
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120241220251021YS'
except
select lca_proc_code,
       lca_proc_name,
       product_code,
       product_name,
       lca_data_item_cat_code,
       lca_data_item_cat_name,
       lca_data_item_code,
       lca_data_item_name,
       value,
       unit
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120241220251021YS_OLD';

select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where COMPANY_CODE = 'ZG'
  and BATCH_NUMBER = '20240120241220251021YS_CONS';



drop table BG00MAC102.T_ADS_WH_LCA_EPD_SUBCLASS_DATA_RESULT_TAB_INDEX;

create table BG00MAC102.T_ADS_WH_LCA_EPD_SUBCLASS_DATA_RESULT_TAB_INDEX
(
    REC_ID                        VARCHAR(64),
    PCR                           VARCHAR(64),
    PROCEDURE                     VARCHAR(256),
    COMPANY_CODE                  VARCHAR(8),
    CATEGORY                      VARCHAR(64),
    MAIN_CAT_BATCH_NUMBER         VARCHAR(64),
    FACTOR_YEAR                   VARCHAR(4),
    FACTOR_VERSION                VARCHAR(100),
    SUBCLASS_TAB_NAME             VARCHAR(256),
    SUBCLASS_TAB_DESC             VARCHAR(500),
    SUBCLASS_RESULT_TAB_NAME      VARCHAR(256),
    SUBCLASS_RESULT_DIST_TAB_NAME VARCHAR(256),
    REC_CREATOR                   VARCHAR(32),
    REC_CREATE_TIME               VARCHAR(32),
    REC_REVISOR                   VARCHAR(32),
    REC_REVISE_TIME               VARCHAR(32)
)
    distribute by hash (REC_ID);

comment on table BG00MAC102.T_ADS_WH_LCA_EPD_SUBCLASS_DATA_RESULT_TAB_INDEX is '明细产品计算数据结果维护表';


SELECT DISTINCT FROM T_ADS_FACT_LCA_PROC_DATA
    WHERE SUBSTR(BATCH_NUMBER, 1, 12) IN
          (SELECT CONCAT(UPDATE_DATE, UPDATE_DATE)
           FROM (SELECT DISTINCT UPDATE_DATE
                 FROM T_ADS_FACT_LCA_HP0001_2024_TEST));



WITH UPDATE_DATE AS (SELECT DISTINCT UPDATE_DATE FROM T_ADS_FACT_LCA_HP0001_2025_NEW_1105),
     BATCH_MATCH AS (SELECT DISTINCT A.UPDATE_DATE,
                                     B.BATCH_NUMBER
                     FROM UPDATE_DATE A,
                          T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT B
                     WHERE B.BATCH_NUMBER LIKE A.UPDATE_DATE || A.UPDATE_DATE || '%YS_CONS'),
     BATCH_RANK AS (SELECT *, RANK() OVER (PARTITION BY UPDATE_DATE ORDER BY BATCH_NUMBER DESC ) AS BATCH_RANK
                    FROM BATCH_MATCH),
     BATCH AS (SELECT * FROM BATCH_RANK WHERE BATCH_RANK = 1)
SELECT A.UPDATE_DATE, COALESCE(B.BATCH_NUMBER, 'DEFAULT') AS BATCH_NUMBER
FROM UPDATE_DATE A
         LEFT JOIN BATCH B ON A.UPDATE_DATE = B.UPDATE_DATE
ORDER BY A.UPDATE_DATE
;


insert into T_ADS_FACT_LCA_PROC_DATA_0002_DT
select HEX(RAND())                    AS REC_ID,
       CONCAT(BATCH_NUMBER, '_GREEN') as BATCH_NUMBER,
       START_YM,
       END_YM,
       LCA_PROC_CODE,
       LCA_PROC_NAME,
       PRODUCT_CODE,
       PRODUCT_NAME,
       LCA_DATA_ITEM_CAT_CODE,
       LCA_DATA_ITEM_CAT_NAME,
       LCA_DATA_ITEM_CODE,
       LCA_DATA_ITEM_NAME,
       VALUE,
       UNIT,
       INDEX_CODE,
       COMPANY_CODE,
       REC_CREATE_TIME,
       REC_CREATOR,
       MAT_STATUS,
       WG_PRODUCT_CODE
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER in ('20250620250620251127YS',
                       '20250720250720251127YS',
                       '20250820250820251127YS',
                       '20250820250920251127YS',
                       '20250920250920251127YS',
                       '20251020251020251127YS',
                       '20240620251020251127YS');

update T_ADS_FACT_LCA_PROC_DATA_0002_DT
set LCA_DATA_ITEM_CODE = '50110',
    LCA_DATA_ITEM_NAME = '电-外购-光伏电'
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER in ('20250620250620251127YS_GREEN',
                       '20250720250720251127YS_GREEN',
                       '20250820250820251127YS_GREEN',
                       '20250820250920251127YS_GREEN',
                       '20250920250920251127YS_GREEN',
                       '20251020251020251127YS_GREEN',
                       '20240620251020251127YS_GREEN')
  and LCA_DATA_ITEM_CODE = 'DL010';

delete
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER in ('20250620250620251127YS_GREEN',
                       '20250720250720251127YS_GREEN',
                       '20250820250820251127YS_GREEN',
                       '20250820250920251127YS_GREEN',
                       '20250920250920251127YS_GREEN',
                       '20251020251020251127YS_GREEN',
                       '20240620251020251127YS_GREEN')
  and LCA_PROC_CODE = 'DL01';


select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER like '20240620251020251126YS_GREEN%';


select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where BATCH_NUMBER in ('20250820250920251127YS_GREEN', '20240620251020251127YS_GREEN');



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
;

select *
from T_ADS_WH_LCA_MAT_DATA
where ORG_CODE = 'ZG'
  and START_TIME = '2025'
  and ITEM_NAME like '%天然气%';


select LCA_PROC_CODE,
       LCA_PROC_NAME,
       PRODUCT_CODE,
       PRODUCT_NAME,
       LCA_DATA_ITEM_CAT_CODE,
       LCA_DATA_ITEM_CAT_NAME,
       LCA_DATA_ITEM_CODE,
       LCA_DATA_ITEM_NAME,
       SUM(VALUE) AS VALUE,
       UNIT,
       COMPANY_CODE
from (select REC_ID,
             BATCH_NUMBER,
             START_YM,
             END_YM,
             LCA_PROC_CODE,
             LCA_PROC_NAME,
             PRODUCT_CODE,
             PRODUCT_NAME,
             LCA_DATA_ITEM_CAT_CODE,
             LCA_DATA_ITEM_CAT_NAME,
             LCA_DATA_ITEM_CODE,
             LCA_DATA_ITEM_NAME,
             CASE
                 WHEN UNIT = '万度' THEN VALUE * 10000
                 WHEN UNIT = '吨' THEN VALUE * 1000
                 WHEN UNIT = '千立方米' THEN VALUE * 1000
                 ELSE VALUE END AS VALUE,
             CASE
                 WHEN UNIT = '万度' THEN '度'
                 WHEN UNIT = '吨' THEN '千克'
                 WHEN UNIT = '公斤' THEN '千克'
                 WHEN UNIT = '千立方米' THEN '立方米'
                 ELSE UNIT END  AS UNIT,
             INDEX_CODE,
             COMPANY_CODE,
             REC_CREATE_TIME,
             REC_CREATOR,
             MAT_STATUS,
             WG_PRODUCT_CODE
      from BG00MAC102.T_ADS_FACT_LCA_PROC_DATA_0002_DT
      where COMPANY_CODE = 'ZG'
        and BATCH_NUMBER in ('20230720230720251202YS',
                             '20230820230820251202YS',
                             '20230920230920251202YS',
                             '20231020231020251202YS',
                             '20231120231120251202YS',
                             '20231220231220251202YS',
                             '20240120240120251202YS',
                             '20240220240220251202YS',
                             '20240320240320251202YS',
                             '20240420240420251202YS',
                             '20240520240520251202YS',
                             '20240620240620251202YS',
                             '20240720240720251202YS',
                             '20240820240820251202YS',
                             '20240920240920251202YS',
                             '20241020241020251202YS',
                             '20241120241120251202YS',
                             '20241220241220251202YS'))
group by LCA_PROC_CODE, LCA_PROC_NAME, PRODUCT_CODE, PRODUCT_NAME, LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CAT_NAME,
         LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME, UNIT, COMPANY_CODE;


with ITEM as (select A.*
              from (select distinct COMPANY_CODE,
                                    substr(START_YM, 1, 4) as YEAR,
                                    LCA_DATA_ITEM_CAT_NAME,
                                    LCA_DATA_ITEM_CODE,
                                    LCA_DATA_ITEM_NAME
                    from T_ADS_FACT_LCA_PROC_DATA
                    where substr(START_YM, 1, 4) > '2023'
                    order by COMPANY_CODE, YEAR, LCA_DATA_ITEM_CAT_NAME, LCA_DATA_ITEM_CODE) A
                       left join (select distinct data_code,
                                                  uuid,
                                                  base_code
                                  from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                                  where START_TIME = '2025'
                                    and FLAG = 'LCI') B on A.COMPANY_CODE = B.BASE_CODE
                  and A.LCA_DATA_ITEM_CODE = B.DATA_CODE
              where B.DATA_CODE is null),
     FACTOR as (select BASE_CODE,
                       DATA_CODE,
                       A.UUID,
                       A.FLAG,
                       B.NAME              as NAME_YT,
                       B.LCI_ELEMENT_VALUE as GWP_YT,
                       B.BACKGROUND_DATA   as BACKGROUND_YT,
                       C.NAME              as NAME_CML,
                       C.LCI_ELEMENT_VALUE as GWP_CML,
                       C.BACKGROUND_DATA   as BACKGROUND_CML,
                       D.NAME              as NAME_EN15804,
                       D.LCI_ELEMENT_VALUE as GWP_EN15804,
                       D.BACKGROUND_DATA   as BACKGROUND_EN15804
                from (select distinct start_time,
                                      data_code,
                                      uuid,
                                      base_code,
                                      remark,
                                      flag,
                                      item_name,
                                      disch_coeff_name
                      from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                      where START_TIME = '2025') A
                         left join (select uuid, name, lci_element_value, background_data
                                    from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                                    where VERSION like '易碳%'
                                      and LCI_ELEMENT_CODE = 'GWP-total') B
                                   on A.UUID = B.UUID
                         left join (select uuid, name, lci_element_value, background_data
                                    from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                                    where VERSION like 'CML%'
                                      and LCI_ELEMENT_CODE = 'GWP-total') C
                                   on A.UUID = C.UUID
                         left join (select uuid,
                                           name,
                                           lci_element_value,
                                           background_data
                                    from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                                    where VERSION like 'EN15804%'
                                      and LCI_ELEMENT_CODE = 'GWP-total') D
                                   on A.UUID = D.UUID),
     FULL as (select *
              from ITEM A
                       left join FACTOR B
                                 on A.COMPANY_CODE = B.BASE_CODE AND A.LCA_DATA_ITEM_CODE = B.DATA_CODE),
     AGG as (select distinct LISTAGG(distinct COMPANY_CODE, ', ')           as COMPANY_CODE,
                             LISTAGG(distinct YEAR, ', ')                   as YEAR,
                             LISTAGG(distinct LCA_DATA_ITEM_CAT_NAME, ', ') as ITEM_CAT_NAME,
                             LCA_DATA_ITEM_CODE                             as ITEM_CODE,
                             LISTAGG(distinct LCA_DATA_ITEM_NAME, ', ')     as ITEM_NAME,
                             UUID,
                             LISTAGG(distinct FLAG, ', ')                   as FLAG,
                             NAME_YT,
                             GWP_YT,
                             BACKGROUND_YT,
                             NAME_CML,
                             GWP_CML,
                             BACKGROUND_CML,
                             NAME_EN15804,
                             GWP_EN15804,
                             BACKGROUND_EN15804
             from FULL
             group by LCA_DATA_ITEM_CODE,
                      UUID,
                      NAME_YT,
                      GWP_YT,
                      BACKGROUND_YT,
                      NAME_CML,
                      GWP_CML,
                      BACKGROUND_CML,
                      NAME_EN15804,
                      GWP_EN15804,
                      BACKGROUND_EN15804
             order by ITEM_CODE, COMPANY_CODE, YEAR)
select *
from AGG;


select * from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION like 'CML%'
and FLAG = 'STREAM';




