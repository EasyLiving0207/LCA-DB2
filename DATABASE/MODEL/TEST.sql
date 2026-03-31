with NORM_OLD as (select distinct VERSION, UUID, NAME, BACKGROUND, UNIT
                  from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
                  where VERSION like 'NORM%'
                    and FLAG = 'MAT')
select *
from (select distinct *
      from T_ADS_DIM_LCA_UNIT_PROCESS
      where PCR = 'BASIC') A
         FULL OUTER JOIN (select * from NORM_OLD) B on a.UNIT_PROCESS_NAME = B.NAME;


select *
from (select *
      from T_ADS_DIM_LCA_UNIT_PROCESS) UP
         join (select * from T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT) UPI on UP.UNIT_PROCESS_ID = UPI.UNIT_PROCESS_ID
         join (select *
               from T_ADS_DIM_LCA_PCR_INDICATOR
               where INDICATOR_CODE = 'GWP-total') PI
              on UPI.IMPACT_INDICATOR_ID = PI.REFERENCE_IMPACT_INDICATOR_ID
;


select *
from T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
where VERSION like 'NORM%'
  and FLAG = 'MAT'
  and NAME = '镍铬铁'
  and LCI_ELEMENT_CODE = 'GWP-total';

SELECT *
FROM BG00MAC102.V_ADS_LCA_UNIT_PROCESS_IMPACT
ORDER BY PCR, SOURCE, CATEGORY, UNIT_PROCESS_NAME, INDICATOR_CODE;

select * from BG00MAC102.V_ADS_LCA_UNIT_PROCESS_IMPACT






