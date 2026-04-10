CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_LCA_BIGCLASS_CALC_PROPAGATION(IN V_BIGCLASS_REC_ID VARCHAR(36))
    SPECIFIC P_ADS_LCA_BIGCLASS_CALC_PROPAGATION
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    INHERIT SPECIAL REGISTERS
    OLD SAVEPOINT LEVEL
    DYNAMIC RESULT SETS 1
BEGIN

    ------------------------------------日志变量定义------------------------------------

    DECLARE RES CURSOR WITH RETURN FOR
        SELECT V_BIGCLASS_REC_ID FROM SYSIBM.SYSDUMMY1;

    DELETE FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID;
    DELETE FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT (BIGCLASS_REC_ID, PROC_KEY, PCR_INDICATOR_ID,
                                                                  IMPACT_TOTAL, IMPACT_PRODUCTION, IMPACT_UPSTREAM,
                                                                  IMPACT_C1, IMPACT_C2, IMPACT_C3, IMPACT_C4, IMPACT_C5)
    WITH MATRIX_INVERSED AS (SELECT *
                             FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX
                             WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID
                               AND IS_INVERSED),
         IMPACT_ACTI AS (SELECT *
                         FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT
                         WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID),
         PROC_INFO AS (SELECT DISTINCT PROC_KEY,
                                       PROC_CODE,
                                       PROC_NAME,
                                       PRODUCT_CODE,
                                       PRODUCT_NAME,
                                       PRODUCT_VALUE,
                                       PRODUCT_UNIT
                       FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION
                       WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID),
         INDICATOR AS (SELECT PCR_INDICATOR_ID,
                              PCR,
                              INDICATOR_CODE,
                              INDICATOR_NAME_IN_PCR,
                              INDICATOR_CNAME_IN_PCR,
                              INDICATOR_UNIT_IN_PCR,
                              REFERENCE_IMPACT_INDICATOR_ID,
                              IS_GWP_TOTAL
                       FROM BG00MAC102.T_ADS_DIM_LCA_PCR_INDICATOR
                       WHERE PCR = (SELECT PCR
                                    FROM BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD
                                    WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID)),
         IMPACT_AGG AS (SELECT PROC_KEY,
                               IMPACT_CATEGORY,
                               IMPACT_INDICATOR_ID,
                               PCR_INDICATOR_ID,
                               SUM(IMPACT_PER_UNIT_PRODUCT) AS IMPACT
                        FROM IMPACT_ACTI
                        GROUP BY PROC_KEY,
                                 IMPACT_CATEGORY,
                                 IMPACT_INDICATOR_ID,
                                 PCR_INDICATOR_ID),
         IMPACT_EMBEDDED AS (SELECT A.PROC_KEY             AS SOURCE_PROC_KEY,
                                    B.TARGET_PROC_KEY      AS TARGET_PROC_KEY,
                                    IMPACT_CATEGORY,
                                    IMPACT_INDICATOR_ID,
                                    PCR_INDICATOR_ID,
                                    A.IMPACT * B.UNIT_COST AS IMPACT
                             FROM IMPACT_AGG A
                                      CROSS JOIN MATRIX_INVERSED B
                             WHERE A.PROC_KEY = B.SOURCE_PROC_KEY),
         IMPACT_CYCLE AS (SELECT TARGET_PROC_KEY AS PROC_KEY,
                                 IMPACT_CATEGORY,
                                 IMPACT_INDICATOR_ID,
                                 PCR_INDICATOR_ID,
                                 SUM(IMPACT)     AS IMPACT
                          FROM IMPACT_EMBEDDED
                          GROUP BY TARGET_PROC_KEY,
                                   IMPACT_CATEGORY,
                                   IMPACT_INDICATOR_ID,
                                   PCR_INDICATOR_ID)
    SELECT V_BIGCLASS_REC_ID                                                        AS BIGCLASS_REC_ID,
           A.PROC_KEY,
           B.PCR_INDICATOR_ID,
           COALESCE(C1.IMPACT, 0) + COALESCE(C2.IMPACT, 0) + COALESCE(C3.IMPACT, 0) +
           COALESCE(C4.IMPACT, 0) + COALESCE(C5.IMPACT, 0)                          AS IMPACT_TOTAL,
           COALESCE(C1.IMPACT, 0) + COALESCE(C2.IMPACT, 0)                          AS IMPACT_PRODUCTION,
           COALESCE(C3.IMPACT, 0) + COALESCE(C4.IMPACT, 0) + COALESCE(C5.IMPACT, 0) AS IMPACT_UPSTREAM,
           COALESCE(C1.IMPACT, 0)                                                   AS IMPACT_C1,
           COALESCE(C2.IMPACT, 0)                                                   AS IMPACT_C2,
           COALESCE(C3.IMPACT, 0)                                                   AS IMPACT_C3,
           COALESCE(C4.IMPACT, 0)                                                   AS IMPACT_C4,
           COALESCE(C5.IMPACT, 0)                                                   AS IMPACT_C5
    FROM PROC_INFO A
             LEFT JOIN (SELECT * FROM INDICATOR) B ON TRUE
             LEFT JOIN (SELECT * FROM IMPACT_CYCLE WHERE IMPACT_CATEGORY = 'C1') C1
                       ON A.PROC_KEY = C1.PROC_KEY AND B.PCR_INDICATOR_ID = C1.PCR_INDICATOR_ID
             LEFT JOIN (SELECT * FROM IMPACT_CYCLE WHERE IMPACT_CATEGORY = 'C2') C2
                       ON A.PROC_KEY = C2.PROC_KEY AND B.PCR_INDICATOR_ID = C2.PCR_INDICATOR_ID
             LEFT JOIN (SELECT * FROM IMPACT_CYCLE WHERE IMPACT_CATEGORY = 'C3') C3
                       ON A.PROC_KEY = C3.PROC_KEY AND B.PCR_INDICATOR_ID = C3.PCR_INDICATOR_ID
             LEFT JOIN (SELECT * FROM IMPACT_CYCLE WHERE IMPACT_CATEGORY = 'C4') C4
                       ON A.PROC_KEY = C4.PROC_KEY AND B.PCR_INDICATOR_ID = C4.PCR_INDICATOR_ID
             LEFT JOIN (SELECT * FROM IMPACT_CYCLE WHERE IMPACT_CATEGORY = 'C5') C5
                       ON A.PROC_KEY = C5.PROC_KEY AND B.PCR_INDICATOR_ID = C5.PCR_INDICATOR_ID;


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION (BIGCLASS_REC_ID, SOURCE_PROC_KEY,
                                                                        TARGET_PROC_KEY, ITEM_CAT_CODE, ITEM_CAT_NAME,
                                                                        ITEM_CODE, ITEM_NAME, ITEM_VALUE, ITEM_UNIT,
                                                                        IMPACT_CATEGORY, UNIT_PROCESS_ID,
                                                                        ELEMENTARY_FLOW_ID, PCR_INDICATOR_ID,
                                                                        IMPACT_AMOUNT, IMPACT_PER_UNIT_PRODUCT)
    WITH MATRIX_INVERSED AS (SELECT *
                             FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX
                             WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID
                               AND IS_INVERSED),
         IMPACT_ACTI AS (SELECT PROC_KEY,
                                ITEM_CAT_CODE,
                                ITEM_CAT_NAME,
                                ITEM_CODE,
                                ITEM_NAME,
                                ITEM_VALUE,
                                ITEM_UNIT,
                                IMPACT_CATEGORY,
                                UNIT_PROCESS_ID,
                                ELEMENTARY_FLOW_ID,
                                PCR_INDICATOR_ID,
                                IMPACT_AMOUNT,
                                IMPACT_PER_UNIT_PRODUCT
                         FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT
                         WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID),
         IMPACT_GWP AS (SELECT A.*
                        FROM IMPACT_ACTI A
                                 JOIN (SELECT *
                                       FROM BG00MAC102.T_ADS_DIM_LCA_PCR_INDICATOR
                                       WHERE IS_GWP_TOTAL) B
                                      ON A.PCR_INDICATOR_ID = B.PCR_INDICATOR_ID),
         IMPACT_CONTRIBUTION AS (SELECT V_BIGCLASS_REC_ID                       AS BIGCLASS_REC_ID,
                                        A.PROC_KEY                              AS SOURCE_PROC_KEY,
                                        B.TARGET_PROC_KEY                       AS TARGET_PROC_KEY,
                                        ITEM_CAT_CODE,
                                        ITEM_CAT_NAME,
                                        ITEM_CODE,
                                        ITEM_NAME,
                                        ITEM_VALUE * B.UNIT_COST                AS ITEM_VALUE,
                                        ITEM_UNIT,
                                        IMPACT_CATEGORY,
                                        UNIT_PROCESS_ID,
                                        ELEMENTARY_FLOW_ID,
                                        PCR_INDICATOR_ID,
                                        A.IMPACT_AMOUNT * B.UNIT_COST           AS IMPACT_AMOUNT,
                                        A.IMPACT_PER_UNIT_PRODUCT * B.UNIT_COST AS IMPACT_PER_UNIT_PRODUCT
                                 FROM IMPACT_GWP A
                                          CROSS JOIN MATRIX_INVERSED B
                                 WHERE A.PROC_KEY = B.SOURCE_PROC_KEY)
    SELECT *
    FROM IMPACT_CONTRIBUTION;

    OPEN RES;
END;