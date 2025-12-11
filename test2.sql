select *
from BG00MAC102.T_ADS_FACT_LCA_CR0001_2023
where BATCH_NUMBER like 'MX2023%';

SELECT *
FROM BG00MAC102.T_ADS_FACT_LCA_ROUTE_CR0001_2023
WHERE MAT_NO IN (SELECT MAT_NO
                 from BG00MAC102.T_ADS_FACT_LCA_CR0001_2023
                 where BATCH_NUMBER like 'MX2023%');

WITH T1 AS (SELECT DISTINCT MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME
            FROM BG00MAC102.T_ADS_FACT_LCA_CR0001_2023
            where BATCH_NUMBER like 'MX2023%')

SELECT BATCH_NUMBER,
       MAT_NO,
       MAT_TRACK_NO,
       MAT_SEQ_NO,
       FAMILY_CODE,
       UNIT_CODE,
       UNIT_NAME,
       TYPE_CODE,
       TYPE_NAME,
       ITEM_CODE,
       ITEM_NAME,
       CASE
           WHEN UNITM_AC = '万度' THEN VALUE * 10000
           WHEN UNITM_AC = '吨' THEN VALUE * 1000
           WHEN UNITM_AC = '千立方米' THEN VALUE * 1000
           ELSE VALUE
           END
           AS VALUE
FROM BG00MAC102.T_ADS_FACT_LCA_CR0001_2023
WHERE BATCH_NUMBER LIKE 'MX2023%';


call BG00MAC102.P_ADS_FACT_LCA_MAIN_CAT_SEQ_CALC(
        V_COMPANY_CODE => 'TA',
        V_START_DATE => '202301',
        V_END_DATE => '202312'
     );

select *
from BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20230120231220240903YS'
  AND LCA_DATA_ITEM_CODE = 'A4441';

select *
from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_RESULT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20230120231220240903YS';

select *
from BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_ENERGY_RESULT
where COMPANY_CODE = 'TA'
  and BATCH_NUMBER = '20230120231220240903YS';

--                 INSERT INTO T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_RESULT (FAMILY_CODE, UNIT_CODE, PRODUCT_CODE, C1, C2, C3, C4, C5, FLAG)
--                 SELECT A.FAMILY_CODE,
--                        A.UNIT_CODE,
--                        A.PRODUCT_CODE,
--                        A.UNIT_COST * B.C1 AS C1,
--                        A.UNIT_COST * B.C2 AS C2,
--                        A.UNIT_COST * B.C3 AS C3,
--                        A.UNIT_COST * B.C4 AS C4,
--                        A.UNIT_COST * B.C5 AS C5,
--                        'PREV'             AS FLAG
--                 FROM (SELECT *
--                       FROM T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_RESOURCE_MAIN
--                       WHERE FAMILY_CODE = V_FAMILY_CODE
--                         AND UNIT_CODE = V_UNIT_CODE) A
--                          JOIN (SELECT FAMILY_CODE,
--                                       UNIT_CODE,
--                                       PRODUCT_CODE,
--                                       SUM(C1) AS C1,
--                                       SUM(C2) AS C2,
--                                       SUM(C3) AS C3,
--                                       SUM(C4) AS C4,
--                                       SUM(C5) AS C5
--                                FROM T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_RESULT
--                                GROUP BY FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B
--                               ON A.ITEM_CODE = B.PRODUCT_CODE;

call BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC(
        V_COMPANY_CODE => 'TA',
        V_START_DATE => '202301',
        V_END_DATE => '202312',
        V_FACTOR_YEAR => '2024',
        V_SUBCLASS_TAB_NAME => 'T_ADS_FACT_LCA_CR0001_2023',
        V_SUB_BATCH_NUMBER => '20230120231220240315YS'
     );

select * from BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT
where SUBCLASS_TAB_NAME = 'T_ADS_FACT_LCA_SI0001_2023'
  and BATCH_NUMBER = 'MX202306'
  and MAT_TRACK_NO = '20230601054118434687'
order by FAMILY_CODE, MAT_SEQ_NO;

SELECT MAT_NO,
       MAT_TRACK_NO,
       FAMILY_CODE,
       MAT_SEQ_NO,
       UNIT_CODE,
       UNIT_NAME,
       PRODUCT_CODE,
       PRODUCT_NAME,
       SUM(C1) + SUM(C2) + SUM(C3) + SUM(C4) + SUM(C5) AS C_CYCLE,
       SUM(C1)                                         AS C1,
       SUM(C2)                                         AS C2,
       SUM(C3)                                         AS C3,
       SUM(C4)                                         AS C4,
       SUM(C5)                                         AS C5
FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_RESULT_DIST
GROUP BY MAT_NO, MAT_TRACK_NO, FAMILY_CODE, MAT_SEQ_NO, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME
ORDER BY MAT_TRACK_NO, FAMILY_CODE, MAT_SEQ_NO;


SELECT *
FROM T_ADS_FACT_LCA_CR0001_2023;

SELECT *
FROM (select DISTINCT MAT_TRACK_NO from T_ADS_FACT_LCA_CR0001_2023) A
         INNER JOIN (SELECT DISTINCT MAT_TRACK_NO FROM T_CALC_SUBCLASS_RESULT_CR0001) B
                    ON A.MAT_TRACK_NO = B.MAT_TRACK_NO;

SELECT *
FROM BG00MAC102.T_ADS_FACT_LCA_CR0001_2023
where BATCH_NUMBER = '20230120231220240315YS'
  AND MAT_TRACK_NO = '20231226151304914031';

SELECT *
FROM BG00MAC102.T_CALC_MAIN_RESULT
where BATCH_NUMBER = '20230120231220240903YS'
  and LCI_ELEMENT_NAME = '全球变暖潜力(GWP100):合计';

SELECT ITEM_CODE AS "itemCode",
       ITEM_NAME AS "itemName",
       PARENT    AS "parent"
FROM BG00MAC102.T_ADS_WH_LCA_CODE
WHERE CODE_TYPE = 'code.ca.lca.type'
  and PARENT = '1001'
  AND FLAG = '1'
ORDER BY SORT;

SELECT * FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT
WHERE SUBCLASS_TAB_NAME = 'T_ADS_FACT_LCA_SI0001_2023'
AND BATCH_NUMBER = 'MX202306'
AND MAT_TRACK_NO = '20230601054118434687';


