select distinct *
from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_DATA
where item_code in (select distinct item_code
                    from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_DATA
                    where ITEM_CODE not in (select distinct item_code
                                            from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_FACTOR_MAT
                                            union
                                            select distinct ITEM_CODE
                                            from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_FACTOR_EP
                                            union
                                            select distinct product_code
                                            from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_PROC_PRODUCT_LIST));

select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'RG'
  and BATCH_NUMBER = '20250120250920251014YS';

select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220250819YS';


select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where UUID = '77867204852b4a66ae72024ce1d7d096';

select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where BASE_CODE = 'RG';

select *
from T_ADS_WH_LCA_MAT_DATA
where ORG_CODE = 'RG'
  and REC_CREATE_TIME = '20251017';


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_RESULT
where COMPANY_CODE = 'TA';
-- 48083	回收焦炉煤气
-- 50621	压缩空气-外购-盈达气体
-- 48080	回收高炉煤气
-- 48082	回收转炉煤气

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM;

select REC_ID,
       START_TIME,
       END_TIME,
       DATA_CODE,
       UUID,
       BASE_CODE,
       REC_CREATOR,
       REC_CREATE_TIME,
       REC_REVISOR,
       REC_REVISE_TIME,
       REMARK,
       FLAG
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST;

select distinct UUID, NAME
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = '易碳_Ecoinvent3.8'
  and UUID not in (select UUID
                   from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                   where VERSION = 'CML_Ecoinvent3.11')
  and UUID in (select UUID
               from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
               where BASE_CODE = 'RG'
                 and START_TIME = '2025');

select *
from (select * from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST) A
         join (select distinct COMPANY_CODE, LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
               from BG00MAC102.T_ADS_FACT_LCA_PROC_DATA) B on A.DATA_CODE = B.LCA_DATA_ITEM_CODE
    and A.BASE_CODE = B.COMPANY_CODE;


-- 导入背景因子

delete
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION like 'CML%'
  and UUID = 'f1a6fe148e27438bac77e080c211e1a2'
  and REC_ID is not null;

insert into T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
select HEX(RAND()),
       A.VERSION,
       UUID,
       NAME,
       A.LCI_ELEMENT_CODE,
       A.LCI_ELEMENT_NAME,
       A.LCI_ELEMENT_CNAME,
       LCI_ELEMENT_VALUE,
       FLAG,
       REC_CREATOR,
       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI') AS REC_CREATE_TIME,
       REC_REVISOR,
       REC_REVISE_TIME,
       REMARK,
       BACKGROUND_DATA,
       LCI_UNIT
from (select *
      from T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION
      where VERSION = 'CML_Ecoinvent3.11'
        and LCI_ELEMENT_NAME is not null) A
         join (select *
               from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
               where VERSION = 'CML_Ecoinvent3.11'
                 and REC_ID is null) B on A.LCI_ELEMENT_NAME = B.LCI_ELEMENT_NAME;

delete
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = 'CML_Ecoinvent3.11'
  and REC_ID is null;

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = 'CML_Ecoinvent3.11'
  and REC_CREATE_TIME = '202510271621';


-- 建筑用钢
insert into T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
select HEX(RAND()),
       A.VERSION,
       UUID,
       NAME,
       A.LCI_ELEMENT_CODE,
       A.LCI_ELEMENT_NAME,
       A.LCI_ELEMENT_CNAME,
       LCI_ELEMENT_VALUE,
       FLAG,
       REC_CREATOR,
       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI') AS REC_CREATE_TIME,
       REC_REVISOR,
       REC_REVISE_TIME,
       REMARK,
       BACKGROUND_DATA,
       LCI_UNIT
from (select *
      from T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION
      where VERSION = 'EN15804_Ecoinvent3.11') A
         join (select *
               from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
               where VERSION = 'EN15804_Ecoinvent3.11'
                 and REC_ID is null) B on A.LCI_ELEMENT_NAME = B.LCI_ELEMENT_NAME;


delete
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where VERSION = 'EN15804_Ecoinvent3.11'
  and REC_ID is null;


insert into T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
select HEX(RAND()),
       'EN15804_Ecoinvent3.11',
       UUID,
       NAME,
       A.LCI_ELEMENT_CODE,
       A.LCI_ELEMENT_NAME,
       A.LCI_ELEMENT_CNAME,
       LCI_ELEMENT_VALUE,
       FLAG,
       REC_CREATOR,
       TO_CHAR(CURRENT_TIMESTAMP, 'yyyyMMddHH24MI') AS REC_CREATE_TIME,
       REC_REVISOR,
       REC_REVISE_TIME,
       REMARK,
       BACKGROUND_DATA,
       LCI_UNIT
from (select *
      from T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION
      where VERSION = 'EN15804_Ecoinvent3.11') A
         join (select *
               from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
               where VERSION = 'EF3.1_Ecoinvent3.11'
                 and FLAG = 'STREAM') B on A.LCI_ELEMENT_CODE = B.LCI_ELEMENT_CODE;

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where VERSION = 'EN15804_Ecoinvent3.11';

select *
from T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION
where VERSION = 'EN15804_Ecoinvent3.11';

--

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where UUID = '171bb9d90bc44717b821ead8532b10be';

--171bb9d90bc44717b821ead8532b10be 0.2590028471210000

select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220250819YS';

select distinct *
from T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC_DATA
where ITEM_CODE not in (select ITEM_CODE
                        from T_ADS_WH_LCA_MAT_DATA
                        where ORG_CODE = 'RG');

select *
from T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'RG'
  and BATCH_NUMBER = '20250120250920251011YS'
  and LCA_PROC_CODE = 'XABA';



select *
from T_ADS_WH_LCA_MAT_DATA
where ORG_CODE = 'RG';

select rec_id,
       version,
       'bff9ad2e387941108df19951980959b2' as uuid,
       '解析气'                           as name,
       lci_element_code,
       lci_element_name,
       lci_element_cname,
       lci_element_value,
       flag,
       rec_creator,
       rec_create_time,
       rec_revisor,
       rec_revise_time,
       remark
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
where VERSION = 'SimaPro_Ecoinvent3.11'
  and NAME = '天然气';


INSERT INTO BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST (REC_ID, START_TIME, END_TIME, DATA_CODE, UUID, BASE_CODE,
                                                               REC_CREATOR, REC_CREATE_TIME, REC_REVISOR,
                                                               REC_REVISE_TIME, REMARK, FLAG)
VALUES (HEX(RAND()),
        '2025',
        '2025',
        '12570',
        'aa8fa35274284382b5df9195f6c61cad',
        'BSZG',
        null,
        null,
        null,
        null,
        null,
        'SY');

INSERT INTO BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST (REC_ID, START_TIME, END_TIME, DATA_CODE, UUID, BASE_CODE,
                                                               REC_CREATOR, REC_CREATE_TIME, REC_REVISOR,
                                                               REC_REVISE_TIME, REMARK, FLAG)
VALUES (HEX(RAND()),
        '2025',
        '2025',
        '12570',
        'aa8fa35274284382b5df9195f6c61cad',
        'ZG',
        null,
        null,
        null,
        null,
        null,
        'SY');


select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  AND LCA_DATA_ITEM_CODE = '58181'
;

select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where BASE_CODE = 'ZG'
  and START_TIME = '2025'
  and DATA_CODE = '18053';


select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where UUID = '08182819bdf14d5c8ba9214fa5555eea';

select *
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
where VERSION like 'EN%'
  and NAME like '%粉%';


SELECT *
FROM T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_MAT
WHERE MAT_NAME IN ('废铁处理',
                   '富锌材料处理',
                   'BOF废渣处理',
                   '蒸汽（低压）',
                   '蒸汽（中压）');


select *
from T_ADS_FACT_LCA_MAIN_CAT_EPD_CONS_RESULT
where BATCH_NUMBER in ('20240120241220250928YS_CONS', '20240120241220251021YS_CONS');


select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA';


select distinct ORG_CODE,
                BIGCLASS_CODE,
                BIGCLASS_NAME,
                MAT_CODE,
                MAT_NAME,
                TRANS_MTHD_CODE,
                TRANS_DISTANCE,
                UNIT
from T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA;

select *
from T_ADS_WH_LCA_TRANS_DATA;

select *
from T_ADS_FACT_LCA_PROC_DATA_0002_DT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20240120241220251111YS';


select *
from T_ADS_WH_LCA_MAT_DATA
where ORG_CODE = 'TA'
  and START_TIME = '2025';

select * from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where BASE_CODE = 'TA'
and DATA_CODE = 'HG280';



