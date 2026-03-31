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

CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_EPD_NORM_CALC_BUFF_MONTH(
        V_COMPANY_CODE => 'TA',
        V_START_MONTH => NULL,
        V_END_MONTH => NULL,
        V_FACTOR_YEAR => '2025',
        V_FACTOR_VERSION => 'CML_Ecoinvent3.11',
        V_MAIN_CAT_RESULT_TAB_NAME => 'T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT',
        V_MAIN_CAT_BATCH_NUMBER => '20240120241220250819YS',
        V_SUBCLASS_TAB_NAME => 'T_ADS_FACT_LCA_HP0001_2024_TEST',
        V_SUBCLASS_RESULT_TAB_NAME => ''
     );



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


select distinct NAME
from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
where FLAG = 'STREAM'
order by NAME;

select distinct LCA_DATA_ITEM_CAT_CODE, LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and LCA_DATA_ITEM_CAT_CODE > '05'
  and BATCH_NUMBER like '202501202512%'
order by LCA_DATA_ITEM_CAT_CODE;


select UNIT_PROCESS_NAME, BACKGROUND_INFO
from T_ADS_DIM_LCA_UNIT_PROCESS
where UNIT_PROCESS_NAME in ('海运',
                            '汽运',
                            '河运',
                            '铁运')



with BR as (select *
            from T_ADS_FACT_LCA_PROC_DATA
            where BATCH_NUMBER = '20250120251220260227YS'
              and COMPANY_CODE = 'BR'),
     TA as (select *
            from T_ADS_FACT_LCA_PROC_DATA
            where COMPANY_CODE = 'TA'
              and BATCH_NUMBER = '20250120251220260114YS')
select HEX(RAND()),
       '20250120251220260227YS',
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
       'BR',
       REC_CREATE_TIME,
       REC_CREATOR,
       MAT_STATUS,
       WG_PRODUCT_CODE
from TA;



insert into T_ADS_WH_LCA_ITEM_CONTRAST
select HEX(RAND()),
       PCR,
       VERSION,
       'BR',
       ITEM_CODE,
       ITEM_NAME,
       UUID,
       FLAG,
       'TA',
       REC_CREATE_TIME,
       REC_REVISOR,
       REC_REVISE_TIME,
       REMARK
from T_ADS_WH_LCA_ITEM_CONTRAST
where COMPANY_CODE = 'TA'
  and (VERSION = 'NORM_Ecoinvent3.11'
    or VERSION is null);


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where COMPANY_CODE = 'BR';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'BR'
  and BATCH_NUMBER = '20250120251220260227YS'
  and LCI_ELEMENT_CODE = 'GWP-total'
  and FACTOR_VERSION like 'NORM%'
  and PROC_CODE in (select distinct LCA_PROC_CODE
                    from T_ADS_FACT_LCA_PROC_DATA
                    where COMPANY_CODE = 'BR'
                      and BATCH_NUMBER = '20250120251220260227YS'
                      and WG_PRODUCT_CODE = '')
order by PROC_KEY;


with data_all as (select distinct *
                  from T_ADS_FACT_LCA_PROC_DATA
                  where COMPANY_CODE = 'BR'
                    and BATCH_NUMBER = '20250120251220260227YS'),
     data_br as (select distinct *
                 from T_ADS_FACT_LCA_PROC_DATA
                 where COMPANY_CODE = 'BR'
                   and BATCH_NUMBER = '20250120251220260227YS'
                   and WG_PRODUCT_CODE = ''),
     data_ta as (select distinct *
                 from T_ADS_FACT_LCA_PROC_DATA
                 where COMPANY_CODE = 'BR'
                   and BATCH_NUMBER = '20250120251220260227YS'
                   and WG_PRODUCT_CODE = 'TA')
select distinct LCA_DATA_ITEM_CAT_NAME, LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
from data_br
where LCA_DATA_ITEM_CODE not in (select distinct ITEM_CODE
                                 from T_ADS_WH_LCA_ITEM_CONTRAST
                                 where COMPANY_CODE = 'BR'
                                   and (VERSION = 'NORM_Ecoinvent3.11'
                                     or VERSION is null)
                                 union
                                 select distinct PRODUCT_CODE
                                 from data_all)
order by LCA_DATA_ITEM_CAT_NAME, LCA_DATA_ITEM_CODE;


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where ITEM_CODE = '65900';

select *
from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
where NAME = '防锈油';


select *
from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
where VERSION = 'NORM_Ecoinvent3.11'
  and LCI_ELEMENT_CODE = 'GWP-total';


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where COMPANY_CODE = 'BR'
  and VERSION = 'NORM_Ecoinvent3.11';


select *
from T_ADS_WH_LCA_ITEM_CONTRAST
where VERSION is null;

select *
from T_ADS_WH_ZP_FACTOR_LIBRARY
where ITEM_CODE = 'A-20250124-2-CG';


select *
from T_ADS_WH_LCA_MAT_DATA;


select count(*)
from T_ADS_WH_LCA_ITEM_CONTRAST;
