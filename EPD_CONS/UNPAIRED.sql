select *
from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA
where item_code in (select distinct item_code
                    from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_DATA
                    where ITEM_CODE not in (select distinct item_code
                                            from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_MAT
                                            union
                                            select distinct ITEM_CODE
                                            from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_FACTOR_EP
                                            union
                                            select distinct product_code
                                            from T_ADS_TEMP_LCA_MAIN_CAT_EPD_CONS_CALC_PROC_PRODUCT_LIST));


select *
from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
where DATA_CODE = '04'
  and BASE_CODE = 'TA';

select * from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
    where VERSION like 'EF3.1%';


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
from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
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


select * from