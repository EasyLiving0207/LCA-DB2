SELECT *
FROM T_ADS_FACT_LCA_LOW_CARB_EMIS_PROC_DATA;

SELECT *
FROM (select DISTINCT LCA_DATA_ITEM_CODE, LCA_DATA_ITEM_NAME
      from T_ADS_FACT_LCA_LOW_CARB_EMIS_PROC_DATA
      WHERE BATCH_NUMBER = '20240120241220250107YS'
        AND COMPANY_CODE = 'TA') A
         LEFT JOIN (SELECT DISTINCT A.DATA_CODE, B.DATA_ITEM_NAME, B.GWP, B.BACKGROUND_DATA, B.SOURCE AS GWP_SOURCE
                    FROM (SELECT DISTINCT *
                          FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                          WHERE BASE_CODE = 'TA'
                            AND START_TIME = '2025') A
                             JOIN
                         (SELECT *
                          FROM T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_NEW
                          WHERE START_TIME = '2025'
                            AND COMPANY_CODE = 'TA') B ON A.UUID = B.UUID) B
                   ON A.LCA_DATA_ITEM_CODE = B.DATA_CODE
         LEFT JOIN (SELECT DISTINCT ITEM_CODE,
                                    ITEM_NAME,
                                    DISCH_COEFF,
                                    HOTVALUE,
                                    SOURCE
                    FROM T_ADS_WH_LCA_MAT_DATA
                    WHERE START_TIME = '2025'
                      AND ORG_CODE = 'TA') C
                   ON A.LCA_DATA_ITEM_CODE = C.ITEM_CODE;