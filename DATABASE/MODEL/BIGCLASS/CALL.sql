CALL BG00MAC102.P_ADS_LCA_BIGCLASS_CALC_ALLOCATION('bd7265c4-2f27-4484-8939-c59eebb1fd6b',
                                                   'CONS',
                                                   'ecoinvent-v3.11',
                                                   'TA',
                                                   '202602',
                                                   '202602',
                                                   NULL,
                                                   FALSE,
                                                   NULL,
                                                   NULL);

SELECT *
FROM BG00MAC102.V_ADS_LCA_BIGCLASS_IMPACT_RESULT
WHERE PROC_CODE LIKE 'BF01';


SELECT *
FROM BG00MAC102.V_ADS_LCA_BIGCLASS_IMPACT_CONTRIBUTION
WHERE TARGET_PROC_CODE LIKE 'BF01'
  AND BIGCLASS_REC_ID = '1abc2c09-d438-4560-b552-a5786e19eb5a'
ORDER BY BIGCLASS_REC_ID, TARGET_PROC_KEY, PCR_INDICATOR_ID, IMPACT_CATEGORY,
         ABS(IMPACT_PER_UNIT_PRODUCT) DESC;


SELECT *
FROM BG00MAC102.V_ADS_LCA_BIGCLASS_IMPACT_RESULT
WHERE BIGCLASS_REC_ID = '1abc2c09-d438-4560-b552-a5786e19eb5a'
  AND IS_GWP_TOTAL;



CALL BG00MAC102.P_ADS_LCA_BIGCLASS_DELETE_RECORD('bd7265c4-2f27-4484-8939-c59eebb1fd6b');


SELECT COUNT(1) AS "total"
FROM BG00MAC102.V_ADS_LCA_BIGCLASS_IMPACT_RESULT
WHERE BIGCLASS_REC_ID = '54e7c6c8-1eba-4511-9c2d-8f8837e4d802';

SELECT BIGCLASS_REC_ID                           AS "bigclassRecId",
       PCR                                       AS "pcr",
       PCR_ENAME                                 AS "pcrEname",
       PCR_CNAME                                 AS "pcrCname",
       PRODUCT_CATEGORY_CODE                     AS "productCategoryCode",
       PCR_VERSION                               AS "pcrVersion",
       DATABASE_VERSION                          AS "databaseVersion",
       DATABASE_NAME                             AS "databaseName",
       DATABASE_FULL_VERSION                     AS "databaseFullVersion",
       COMPANY_CODE                              AS "companyCode",
       START_YM                                  AS "startYm",
       END_YM                                    AS "endYm",
       CERTIFICATION_NUMBER                      AS "certificationNumber",
       CAST(USING_GREEN_ELECTRICITY AS SMALLINT) as "usingGreenElectricity",
       GREEN_ELECTRICITY_PROPORTION              AS "greenElectricityProportion",
       RECORD_TIME                               AS "recordTime",
       CAST(IS_CURRENT AS SMALLINT)              as "isCurrent",
       REMARK                                    AS "remark"
FROM BG00MAC102.V_ADS_LCA_BIGCLASS_CALC_RECORD
WHERE BIGCLASS_REC_ID = '4295283e-50f1-4792-a49f-e28e9da65257' FETCH FIRST 20000 ROWS ONLY;
;

SELECT DISTINCT PROC_NAME, PRODUCT_NAME
FROM V_ADS_LCA_BIGCLASS_IMPACT_RESULT
WHERE GROUP_DESCRIPTION IS NULL;











