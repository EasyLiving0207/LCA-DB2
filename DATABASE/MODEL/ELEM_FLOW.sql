select *
from (select *
      from T_ADS_DIM_LCA_PCR_INDICATOR
      where REFERENCE_IMPACT_INDICATOR_ID not in
            (select distinct IMPACT_INDICATOR_ID from T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT)) A
         join V_ADS_LCA_IMPACT_INDICATOR B on A.REFERENCE_IMPACT_INDICATOR_ID = B.IMPACT_INDICATOR_ID;



SELECT DISTINCT IMPACT_METHOD_NAME, IMPACT_CATEGORY_NAME, IMPACT_INDICATOR_NAME, IMPACT_INDICATOR_ID
FROM V_ADS_LCA_IMPACT_INDICATOR
WHERE IMPACT_METHOD_NAME like 'EN%'
  and RESOURCES_EMISSIONS_TOTAL = 'inventory'
order by IMPACT_METHOD_NAME, IMPACT_CATEGORY_NAME, IMPACT_INDICATOR_NAME;


select *
from V_ADS_LCA_PCR_ELEMENTARY_FLOW_IMPACT
where PCR = 'CONS';
;



insert into T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW
select COMPANY_CODE,
       ITEM_CODE,
       ITEM_NAME,
       ELEMENTARY_FLOW_ID,
       1 AS CONVERSION_FACTOR
from (select *
      from T_ADS_WH_LCA_ITEM_CONTRAST
      where FLAG = 'STREAM'
        and VERSION = 'NORM_Ecoinvent3.11') A
         join
     (select distinct UUID, NAME
      from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
      where FLAG = 'STREAM'
        and VERSION = 'NORM_Ecoinvent3.11') B on A.UUID = B.UUID
         join (select * from T_ADS_TEMP_LCA_ELEM_FLOW_MAP) C on B.UUID = C.UUID;



insert into T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW (COMPANY_CODE, ITEM_CODE, ITEM_NAME, ELEMENTARY_FLOW_ID,
                                               CONVERSION_FACTOR)
with ALL as (select B.COMPANY_CODE, B.ITEM_CODE, B.NAME
             from (select COMPANY_CODE, ITEM_CODE, NAME
                   from (select COMPANY_CODE, ITEM_CODE, ELEMENTARY_FLOW_ID from T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW) A
                            join (select * from T_ADS_TEMP_LCA_ELEM_FLOW_MAP) B
                                 on A.ELEMENTARY_FLOW_ID = B.ELEMENTARY_FLOW_ID) A
                      full join
                  (select A.BASE_CODE as COMPANY_CODE, DATA_CODE AS ITEM_CODE, B.NAME
                   from (select distinct BASE_CODE, DATA_CODE, UUID
                         from T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                         where FLAG = 'LCI'
                           and START_TIME = '2025'
                           and BASE_CODE != 'BSZG') A
                            join

                        (select distinct UUID, NAME
                         from T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                         where FLAG = 'STREAM'
                           and VERSION = 'EN15804_Ecoinvent3.11') B on A.UUID = B.UUID) B
                  on A.COMPANY_CODE = B.COMPANY_CODE and A.ITEM_CODE = B.ITEM_CODE
             where A.ITEM_CODE is null)
select COMPANY_CODE, ITEM_CODE, NULL AS ITEM_NAME, ELEMENTARY_FLOW_ID, 1
from ALL
         left join T_ADS_TEMP_LCA_ELEM_FLOW_MAP B on ALL.NAME = B.NAME
where ELEMENTARY_FLOW_ID is not null;













