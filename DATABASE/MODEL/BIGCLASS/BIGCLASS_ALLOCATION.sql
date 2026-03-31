CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_LCA_BIGCLASS_CALC_ALLOCATION(IN V_BIGCLASS_REC_ID VARCHAR(36),
                                                                          IN V_PCR VARCHAR(100),
                                                                          IN V_DATABASE_VERSION VARCHAR(100),
                                                                          IN V_COMPANY_CODE VARCHAR(20),
                                                                          IN V_START_YM VARCHAR(8),
                                                                          IN V_END_YM VARCHAR(8),
                                                                          IN V_CERTIFICATION_NUMBER VARCHAR(100),
                                                                          IN V_USING_GREEN_ELECTRICITY BOOLEAN,
                                                                          IN V_GREEN_ELECTRICITY_PROPORTION DOUBLE,
                                                                          IN V_REMARK VARCHAR(255))
    SPECIFIC P_ADS_LCA_BIGCLASS_CALC_ALLOCATION
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    INHERIT SPECIAL REGISTERS
    OLD SAVEPOINT LEVEL
    DYNAMIC RESULT SETS 1
BEGIN

    ------------------------------------日志变量定义------------------------------------
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102';
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_BIGCLASS_CALC_ALLOCATION'; --临时表名
    DECLARE V_PNAME VARCHAR(50) DEFAULT 'P_ADS_LCA_BIGCLASS_CALC_ALLOCATION'; --存储过程名
    DECLARE V_QUERY_STR CLOB(1 M);
    DECLARE V_BIGCLASS_PROC_DATA_TAB_NAME VARCHAR(255);
    DECLARE V_RECORD_TIME TIMESTAMP(6);
    DECLARE RES CURSOR WITH RETURN FOR
        SELECT V_BIGCLASS_REC_ID FROM SYSIBM.SYSDUMMY1;

    DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN
    END;

    EXECUTE IMMEDIATE 'DROP TABLE SESSION.PROC_DATA_ORIGINAL';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.ALLOCATION';

    DECLARE GLOBAL TEMPORARY TABLE SESSION.PROC_DATA_ORIGINAL (
        PROC_KEY VARCHAR(255),
        PROC_CODE VARCHAR(255),
        PROC_NAME VARCHAR(255),
        PRODUCT_CODE VARCHAR(255),
        PRODUCT_NAME VARCHAR(255),
        ITEM_CAT_CODE VARCHAR(10),
        ITEM_CAT_NAME VARCHAR(50),
        ITEM_CODE VARCHAR(255),
        ITEM_NAME VARCHAR(255),
        ITEM_VALUE DECIMAL(30, 6),
        ITEM_UNIT VARCHAR(255)
        ) ON COMMIT PRESERVE ROWS;

    DECLARE GLOBAL TEMPORARY TABLE SESSION.ALLOCATION (
        PROC_KEY VARCHAR(255),
        PROC_CODE VARCHAR(255),
        PROC_NAME VARCHAR(255),
        PRODUCT_CODE VARCHAR(255),
        PRODUCT_NAME VARCHAR(255),
        ITEM_CAT_CODE VARCHAR(10),
        ITEM_CAT_NAME VARCHAR(50),
        ITEM_CODE VARCHAR(255),
        ITEM_NAME VARCHAR(255),
        ITEM_VALUE DECIMAL(30, 6),
        ITEM_UNIT VARCHAR(255),
        PROC_CATEGORY VARCHAR(255),
        PRODUCT_CATEGORY VARCHAR(255),
        PROC_KEY_AFTER VARCHAR(255),
        PROC_CODE_AFTER VARCHAR(255),
        PROC_NAME_AFTER VARCHAR(255),
        ALLOCATION_FACTOR DOUBLE,
        PRODUCT_CODE_AFTER VARCHAR(255),
        PRODUCT_NAME_AFTER VARCHAR(255),
        PRODUCT_VALUE_AFTER DECIMAL(30, 6),
        PRODUCT_UNIT_AFTER VARCHAR(255)
        ) ON COMMIT PRESERVE ROWS;

    ------------------------------------存储过程变量定义---------------------------------

    CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);

    SET V_RECORD_TIME = CURRENT_TIMESTAMP;

    SELECT BIGCLASS_PROC_DATA_TAB_NAME
    INTO V_BIGCLASS_PROC_DATA_TAB_NAME
    FROM BG00MAC102.T_ADS_DIM_LCA_PCR
    WHERE PCR = V_PCR;

    ------------------------------------处理逻辑(开始)------------------------------------

    DELETE FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID;
    DELETE FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID;

    DELETE
    FROM BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD
    WHERE BIGCLASS_REC_ID = V_BIGCLASS_REC_ID;

    UPDATE BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD
    SET IS_CURRENT = FALSE
    WHERE PCR = V_PCR
      AND DATABASE_VERSION = V_DATABASE_VERSION
      AND COMPANY_CODE = V_COMPANY_CODE
      AND START_YM = V_START_YM
      AND END_YM = V_END_YM
      AND CERTIFICATION_NUMBER = V_CERTIFICATION_NUMBER
      AND USING_GREEN_ELECTRICITY = V_USING_GREEN_ELECTRICITY
      AND GREEN_ELECTRICITY_PROPORTION = V_GREEN_ELECTRICITY_PROPORTION;

    INSERT INTO BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD (BIGCLASS_REC_ID, PCR, DATABASE_VERSION, COMPANY_CODE,
                                                               START_YM, END_YM, CERTIFICATION_NUMBER,
                                                               USING_GREEN_ELECTRICITY, GREEN_ELECTRICITY_PROPORTION,
                                                               RECORD_TIME, IS_CURRENT, REMARK)
    VALUES (V_BIGCLASS_REC_ID, V_PCR, V_DATABASE_VERSION,
            V_COMPANY_CODE, V_START_YM, V_END_YM,
            V_CERTIFICATION_NUMBER, V_USING_GREEN_ELECTRICITY,
            V_GREEN_ELECTRICITY_PROPORTION, CURRENT_TIMESTAMP,
            TRUE, V_REMARK);


    SET V_QUERY_STR = '
            INSERT INTO SESSION.PROC_DATA_ORIGINAL (PROC_KEY, PROC_CODE, PROC_NAME, PRODUCT_CODE,
                                                    PRODUCT_NAME, ITEM_CAT_CODE,
                                                    ITEM_CAT_NAME, ITEM_CODE, ITEM_NAME, ITEM_VALUE,
                                                    ITEM_UNIT)
            WITH BATCH_NO AS (SELECT MAX(BATCH_NUMBER) AS MAX_BATCH_NUMBER
                              FROM ' || V_TMP_SCHEMA || '.' || V_BIGCLASS_PROC_DATA_TAB_NAME || '
                              WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                                AND START_YM = ''' || V_START_YM || '''
                                AND END_YM = ''' || V_END_YM || '''),
                 DATA AS (SELECT CONCAT(CONCAT(LCA_PROC_CODE, ''_''), PRODUCT_CODE) AS PROC_KEY,
                                 LCA_PROC_CODE                                    AS PROC_CODE,
                                 LCA_PROC_NAME                                    AS PROC_NAME,
                                 PRODUCT_CODE,
                                 PRODUCT_NAME,
                                 LCA_DATA_ITEM_CAT_CODE                           AS ITEM_CAT_CODE,
                                 LCA_DATA_ITEM_CAT_NAME                           AS ITEM_CAT_NAME,
                                 LCA_DATA_ITEM_CODE                               AS ITEM_CODE,
                                 LCA_DATA_ITEM_NAME                               AS ITEM_NAME,
                                 VALUE                                            AS ITEM_VALUE,
                                 UNIT
                          FROM ' || V_TMP_SCHEMA || '.' || V_BIGCLASS_PROC_DATA_TAB_NAME || '
                          WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                            AND START_YM = ''' || V_START_YM || '''
                            AND END_YM = ''' || V_END_YM || '''
                            AND BATCH_NUMBER = (SELECT MAX_BATCH_NUMBER FROM BATCH_NO)),
                 UNIT_CF AS (SELECT UNIT_NAME_BEFORE, UNIT_NAME_AFTER, CONVERSION_FACTOR
                             FROM BG00MAC102.T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR
                             WHERE IS_CANONICAL)
            SELECT DATA.PROC_KEY,
                   DATA.PROC_CODE,
                   DATA.PROC_NAME,
                   DATA.PRODUCT_CODE,
                   DATA.PRODUCT_NAME,
                   DATA.ITEM_CAT_CODE,
                   DATA.ITEM_CAT_NAME,
                   DATA.ITEM_CODE,
                   DATA.ITEM_NAME,
                   COALESCE(ABS(DATA.ITEM_VALUE) * UNIT_CF.CONVERSION_FACTOR, ABS(DATA.ITEM_VALUE)) AS ITEM_VALUE,
                   COALESCE(UNIT_CF.UNIT_NAME_AFTER, DATA.UNIT)                                     AS ITEM_UNIT
            FROM DATA
                     LEFT JOIN UNIT_CF ON DATA.UNIT = UNIT_CF.UNIT_NAME_BEFORE';
    PREPARE STMT FROM V_QUERY_STR;
    EXECUTE STMT;


    INSERT INTO SESSION.ALLOCATION (PROC_KEY,
                                    PROC_CODE,
                                    PROC_NAME,
                                    PRODUCT_CODE,
                                    PRODUCT_NAME,
                                    ITEM_CAT_CODE,
                                    ITEM_CAT_NAME,
                                    ITEM_CODE,
                                    ITEM_NAME,
                                    ITEM_VALUE,
                                    ITEM_UNIT,
                                    PROC_CATEGORY,
                                    PRODUCT_CATEGORY,
                                    PROC_KEY_AFTER,
                                    PROC_CODE_AFTER,
                                    PROC_NAME_AFTER,
                                    ALLOCATION_FACTOR,
                                    PRODUCT_CODE_AFTER,
                                    PRODUCT_NAME_AFTER,
                                    PRODUCT_VALUE_AFTER,
                                    PRODUCT_UNIT_AFTER)
    WITH DATA_ORIGINAL AS (SELECT *
                           FROM SESSION.PROC_DATA_ORIGINAL),
         ALLOCATION_PROCESS AS (SELECT *
                                FROM BG00MAC102.T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS
                                WHERE PCR = V_PCR
                                  AND COMPANY_CODE = V_COMPANY_CODE),
         ALLOCATION_FACTOR AS (SELECT *
                               FROM BG00MAC102.T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR
                               WHERE PCR = V_PCR),
         ALLOCATION1 AS (SELECT A.PROC_CATEGORY,
                                A.PROC_KEY_BEFORE,
                                A.PROC_CODE_BEFORE,
                                A.PROC_NAME_BEFORE,
                                A.PROC_KEY_AFTER,
                                A.PROC_CODE_AFTER,
                                A.PROC_NAME_AFTER,
                                A.PRODUCT_CATEGORY,
                                A.ITEM_CAT_CODE,
                                A.ITEM_CODE,
                                A.ITEM_NAME,
                                B.ALLOCATION_FACTOR
                         FROM ALLOCATION_PROCESS A
                                  JOIN ALLOCATION_FACTOR B ON A.PROC_CATEGORY = B.PROC_CATEGORY
                             AND A.PRODUCT_CATEGORY = B.PRODUCT_CATEGORY),
         ALLOCATION AS (SELECT A.PROC_KEY,
                               A.PROC_CODE,
                               A.PROC_NAME,
                               A.PRODUCT_CODE,
                               A.PRODUCT_NAME,
                               A.ITEM_CAT_CODE,
                               A.ITEM_CAT_NAME,
                               A.ITEM_CODE,
                               A.ITEM_NAME,
                               A.ITEM_VALUE,
                               A.ITEM_UNIT,
                               B.PROC_CATEGORY,
                               B.PRODUCT_CATEGORY,
                               B.PROC_KEY_AFTER,
                               B.PROC_CODE_AFTER,
                               B.PROC_NAME_AFTER,
                               B.ALLOCATION_FACTOR * CAST(A.ITEM_VALUE AS DECIMAL(20, 10)) /
                               SUM(A.ITEM_VALUE) OVER (PARTITION BY A.PROC_KEY, B.PRODUCT_CATEGORY) AS ALLOCATION_FACTOR
                        FROM SESSION.PROC_DATA_ORIGINAL A
                                 JOIN ALLOCATION1 B ON A.PROC_KEY = B.PROC_KEY_BEFORE
                            AND A.ITEM_CAT_CODE = B.ITEM_CAT_CODE
                            AND A.ITEM_CODE = B.ITEM_CODE
                        ORDER BY A.PROC_KEY, B.PRODUCT_CATEGORY),
         ALLOCATION_PRODUCT AS (SELECT PROC_KEY_AFTER,
                                       PROC_CODE_AFTER,
                                       PROC_NAME_AFTER,
                                       ITEM_CODE       AS PRODUCT_CODE_AFTER,
                                       ITEM_NAME       AS PRODUCT_NAME_AFTER,
                                       SUM(ITEM_VALUE) AS PRODUCT_VALUE_AFTER,
                                       ITEM_UNIT       AS PRODUCT_UNIT_AFTER
                                FROM ALLOCATION
                                GROUP BY PROC_KEY_AFTER,
                                         PROC_CODE_AFTER,
                                         PROC_NAME_AFTER,
                                         ITEM_CODE,
                                         ITEM_NAME,
                                         ITEM_UNIT)
    SELECT A.*,
           B.PRODUCT_CODE_AFTER,
           B.PRODUCT_NAME_AFTER,
           B.PRODUCT_VALUE_AFTER,
           B.PRODUCT_UNIT_AFTER
    FROM ALLOCATION A
             JOIN ALLOCATION_PRODUCT B ON A.PROC_KEY_AFTER = B.PROC_KEY_AFTER;


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT
    WITH CO_PRODUCT AS (SELECT DISTINCT ITEM_CODE
                        FROM SESSION.PROC_DATA_ORIGINAL O
                        WHERE NOT EXISTS (SELECT 1
                                          FROM BG00MAC102.T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS P
                                          WHERE P.PCR = V_PCR
                                            AND P.COMPANY_CODE = V_COMPANY_CODE
                                            AND P.PROC_KEY_BEFORE = O.PROC_KEY)
                          AND ITEM_CAT_CODE = '05'),
         PCR_INDICATOR AS (SELECT PCR_INDICATOR_ID,
                                  INDICATOR_CODE,
                                  INDICATOR_NAME_IN_PCR,
                                  INDICATOR_CNAME_IN_PCR,
                                  INDICATOR_UNIT_IN_PCR,
                                  REFERENCE_IMPACT_INDICATOR_ID,
                                  IS_GWP_TOTAL
                           FROM BG00MAC102.T_ADS_DIM_LCA_PCR_INDICATOR
                           WHERE PCR = V_PCR),
         ITEM_UNIT_PROCESS AS (SELECT ITEM_CODE, UNIT_PROCESS_ID, CONVERSION_FACTOR
                               FROM BG00MAC102.T_ADS_BR_LCA_ITEM_UNIT_PROCESS
                               WHERE PCR = V_PCR
                                 AND DATABASE_VERSION = V_DATABASE_VERSION
                                 AND COMPANY_CODE = V_COMPANY_CODE),
         UNIT_PROCESS AS (SELECT UNIT_PROCESS_ID,
                                 PCR,
                                 DATABASE_VERSION,
                                 SOURCE,
                                 CATEGORY,
                                 UNIT_PROCESS_NAME,
                                 BACKGROUND_INFO,
                                 UNIT_NAME,
                                 REFERENCE_DATASET_ID,
                                 CONVERSION_FACTOR
                          FROM BG00MAC102.T_ADS_DIM_LCA_UNIT_PROCESS
                          WHERE PCR = V_PCR
                            AND DATABASE_VERSION = V_DATABASE_VERSION),
         UNIT_PROCESS_IMPACT AS (SELECT UNIT_PROCESS_ID, IMPACT_INDICATOR_ID, AMOUNT
                                 FROM BG00MAC102.T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT),
         ITEM_ELEMENTARY_FLOW AS (SELECT ITEM_CODE,
                                         ELEMENTARY_FLOW_ID,
                                         CONVERSION_FACTOR,
                                         IS_CO2
                                  FROM BG00MAC102.T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW
                                  WHERE COMPANY_CODE = V_COMPANY_CODE),
         ELEMENTARY_FLOW_IMPACT AS (SELECT ELEMENTARY_FLOW_ID,
                                           IMPACT_INDICATOR_ID,
                                           CHARACTERIZATION_FACTOR
                                    FROM BG00MAC102.T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT
                                    WHERE DATABASE_VERSION = V_DATABASE_VERSION),
         TRANSPORT_DISTANCE AS (SELECT LCA_DATA_ITEM_CODE           AS ITEM_CODE,
                                       CASE
                                           WHEN TRANS_MTHD_CODE = 'carTrans' THEN 'TRANS_TRUCK'
                                           WHEN TRANS_MTHD_CODE = 'railwayTrans' THEN 'TRANS_RAIL'
                                           WHEN TRANS_MTHD_CODE = 'seaTrans' THEN 'TRANS_SEA'
                                           WHEN TRANS_MTHD_CODE = 'riversTrans' THEN 'TRANS_RIVER'
                                           WHEN TRANS_MTHD_CODE = '柴油' THEN 'TRANS_DIESEL'
                                           ELSE TRANS_MTHD_CODE END AS TRANS_METHOD,
                                       CASE
                                           WHEN UNIT = 'nm' THEN TRANS_DISTANCE * 1.852
                                           ELSE TRANS_DISTANCE END  AS TRANS_DISTANCE,
                                       CASE
                                           WHEN UNIT = 'nm' THEN 'km'
                                           ELSE UNIT END            AS UNIT_NAME
                                FROM BG00MAC102.T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE
                                WHERE YEAR = '2025'
                                  AND ORG_CODE = V_COMPANY_CODE),
         TRANSPORT_PROCESS AS (SELECT *
                               FROM UNIT_PROCESS
                               WHERE CATEGORY IN
                                     ('TRANS_SEA', 'TRANS_RIVER', 'TRANS_TRUCK', 'TRANS_RAIL', 'TRANS_DIESEL')),
         ELEM_IMPACT AS (SELECT A.*,
                                'C1'                                      AS IMPACT_CATEGORY,
                                NULL                                      AS UNIT_PROCESS_ID,
                                B.ELEMENTARY_FLOW_ID,
                                C.IMPACT_INDICATOR_ID,
                                D.PCR_INDICATOR_ID,
                                CASE
                                    WHEN A.ITEM_CAT_CODE IN ('04', '05')
                                        THEN CAST(- B.CONVERSION_FACTOR AS DOUBLE) *
                                             CAST(C.CHARACTERIZATION_FACTOR AS DOUBLE) *
                                             CAST(A.ITEM_VALUE AS DOUBLE)
                                    ELSE CAST(B.CONVERSION_FACTOR AS DOUBLE) *
                                         CAST(C.CHARACTERIZATION_FACTOR AS DOUBLE) *
                                         CAST(A.ITEM_VALUE AS DOUBLE) END AS IMPACT_AMOUNT
                         FROM SESSION.PROC_DATA_ORIGINAL A
                                  JOIN ITEM_ELEMENTARY_FLOW B ON A.ITEM_CODE = B.ITEM_CODE
                                  JOIN ELEMENTARY_FLOW_IMPACT C
                                       ON B.ELEMENTARY_FLOW_ID = C.ELEMENTARY_FLOW_ID
                                  JOIN PCR_INDICATOR D
                                       ON C.IMPACT_INDICATOR_ID = D.REFERENCE_IMPACT_INDICATOR_ID),
         CO_PRODUCT_INPUT AS (SELECT A.*
                              FROM (SELECT DISTINCT *
                                    FROM SESSION.PROC_DATA_ORIGINAL
                                    WHERE ITEM_CAT_CODE < '04') A
                                       JOIN (SELECT ITEM_CODE FROM CO_PRODUCT) B
                                            ON A.ITEM_CODE = B.ITEM_CODE),
         CO_PRODUCT_INPUT_IMPACT AS (SELECT A.*,
                                            'C2'                         AS IMPACT_CATEGORY,
                                            B.UNIT_PROCESS_ID,
                                            NULL                         AS ELEMENTARY_FLOW_ID,
                                            C.IMPACT_INDICATOR_ID,
                                            D.PCR_INDICATOR_ID,
                                            CAST(B.CONVERSION_FACTOR AS DOUBLE) * CAST(C.AMOUNT AS DOUBLE) *
                                            CAST(A.ITEM_VALUE AS DOUBLE) AS IMPACT_AMOUNT
                                     FROM CO_PRODUCT_INPUT A
                                              JOIN ITEM_UNIT_PROCESS B
                                                   ON A.ITEM_CODE = B.ITEM_CODE
                                              JOIN UNIT_PROCESS_IMPACT C ON B.UNIT_PROCESS_ID = C.UNIT_PROCESS_ID
                                              JOIN PCR_INDICATOR D
                                                   ON C.IMPACT_INDICATOR_ID = D.REFERENCE_IMPACT_INDICATOR_ID),
         UPSTREAM_INPUT AS (SELECT DISTINCT *
                            FROM SESSION.PROC_DATA_ORIGINAL
                            WHERE ITEM_CAT_CODE < '04'
                            EXCEPT
                            SELECT *
                            FROM CO_PRODUCT_INPUT),
         UPSTREAM_IMPACT AS (SELECT A.*,
                                    'C3'                         AS IMPACT_CATEGORY,
                                    B.UNIT_PROCESS_ID,
                                    NULL                         AS ELEMENTARY_FLOW_ID,
                                    C.IMPACT_INDICATOR_ID,
                                    D.PCR_INDICATOR_ID,
                                    CAST(B.CONVERSION_FACTOR AS DOUBLE) * CAST(C.AMOUNT AS DOUBLE) *
                                    CAST(A.ITEM_VALUE AS DOUBLE) AS IMPACT_AMOUNT
                             FROM UPSTREAM_INPUT A
                                      JOIN ITEM_UNIT_PROCESS B
                                           ON A.ITEM_CODE = B.ITEM_CODE
                                      JOIN UNIT_PROCESS_IMPACT C ON B.UNIT_PROCESS_ID = C.UNIT_PROCESS_ID
                                      JOIN PCR_INDICATOR D
                                           ON C.IMPACT_INDICATOR_ID = D.REFERENCE_IMPACT_INDICATOR_ID),
         CO_PRODUCT_OUTPUT AS (SELECT A.*
                               FROM (SELECT DISTINCT *
                                     FROM SESSION.PROC_DATA_ORIGINAL
                                     WHERE ITEM_CAT_CODE = '05') A
                                        JOIN (SELECT ITEM_CODE FROM CO_PRODUCT) B
                                             ON A.ITEM_CODE = B.ITEM_CODE),
         CO_PRODUCT_OUTPUT_IMPACT AS (SELECT A.*,
                                             'C4'                         AS IMPACT_CATEGORY,
                                             B.UNIT_PROCESS_ID,
                                             NULL                         AS ELEMENTARY_FLOW_ID,
                                             C.IMPACT_INDICATOR_ID,
                                             D.PCR_INDICATOR_ID,
                                             CAST(B.CONVERSION_FACTOR AS DOUBLE) * CAST(C.AMOUNT AS DOUBLE) *
                                             CAST(A.ITEM_VALUE AS DOUBLE) AS IMPACT_AMOUNT
                                      FROM CO_PRODUCT_OUTPUT A
                                               JOIN ITEM_UNIT_PROCESS B
                                                    ON A.ITEM_CODE = B.ITEM_CODE
                                               JOIN UNIT_PROCESS_IMPACT C ON B.UNIT_PROCESS_ID = C.UNIT_PROCESS_ID
                                               JOIN PCR_INDICATOR D
                                                    ON C.IMPACT_INDICATOR_ID = D.REFERENCE_IMPACT_INDICATOR_ID),
         TRANSPORT_IMPACT AS (SELECT A.*,
                                     'C5'                                AS IMPACT_CATEGORY,
                                     C.UNIT_PROCESS_ID,
                                     NULL                                AS ELEMENTARY_FLOW_ID,
                                     D.IMPACT_INDICATOR_ID,
                                     E.PCR_INDICATOR_ID,
                                     CAST(B.TRANS_DISTANCE AS DOUBLE) * CAST(D.AMOUNT AS DOUBLE) *
                                     CAST(A.ITEM_VALUE AS DOUBLE) / 1000 AS IMPACT_AMOUNT
                              FROM UPSTREAM_INPUT A
                                       JOIN TRANSPORT_DISTANCE B ON A.ITEM_CODE = B.ITEM_CODE
                                       JOIN TRANSPORT_PROCESS C ON B.TRANS_METHOD = C.CATEGORY
                                       JOIN UNIT_PROCESS_IMPACT D ON C.UNIT_PROCESS_ID = D.UNIT_PROCESS_ID
                                       JOIN PCR_INDICATOR E
                                            ON D.IMPACT_INDICATOR_ID = E.REFERENCE_IMPACT_INDICATOR_ID),
         IMPACT_ALL AS (SELECT *
                        FROM ELEM_IMPACT
                        UNION ALL
                        SELECT *
                        FROM CO_PRODUCT_INPUT_IMPACT
                        UNION ALL
                        SELECT *
                        FROM UPSTREAM_IMPACT
                        UNION ALL
                        SELECT *
                        FROM CO_PRODUCT_OUTPUT_IMPACT
                        UNION ALL
                        SELECT *
                        FROM TRANSPORT_IMPACT),
         IMPACT_ALLOCATION AS (SELECT PROC_KEY,
                                      PROC_CODE,
                                      PROC_NAME,
                                      PRODUCT_CODE,
                                      PRODUCT_NAME,
                                      PRODUCT_VALUE,
                                      PRODUCT_UNIT,
                                      ITEM_CAT_CODE,
                                      ITEM_CAT_NAME,
                                      ITEM_CODE,
                                      ITEM_NAME,
                                      SUM(ITEM_VALUE)    AS ITEM_VALUE,
                                      ITEM_UNIT,
                                      IMPACT_CATEGORY,
                                      UNIT_PROCESS_ID,
                                      ELEMENTARY_FLOW_ID,
                                      IMPACT_INDICATOR_ID,
                                      PCR_INDICATOR_ID,
                                      SUM(IMPACT_AMOUNT) AS IMPACT_AMOUNT
                               FROM (SELECT B.PROC_KEY_AFTER                      AS PROC_KEY,
                                            B.PROC_CODE_AFTER                     AS PROC_CODE,
                                            B.PROC_NAME_AFTER                     AS PROC_NAME,
                                            B.PRODUCT_CODE_AFTER                  AS PRODUCT_CODE,
                                            B.PRODUCT_NAME_AFTER                  AS PRODUCT_NAME,
                                            B.PRODUCT_VALUE_AFTER                 AS PRODUCT_VALUE,
                                            B.PRODUCT_UNIT_AFTER                  AS PRODUCT_UNIT,
                                            A.ITEM_CAT_CODE,
                                            A.ITEM_CAT_NAME,
                                            A.ITEM_CODE,
                                            A.ITEM_NAME,
                                            A.ITEM_VALUE * B.ALLOCATION_FACTOR    AS ITEM_VALUE,
                                            A.ITEM_UNIT,
                                            A.IMPACT_CATEGORY,
                                            A.UNIT_PROCESS_ID,
                                            A.ELEMENTARY_FLOW_ID,
                                            A.IMPACT_INDICATOR_ID,
                                            A.PCR_INDICATOR_ID,
                                            A.IMPACT_AMOUNT * B.ALLOCATION_FACTOR AS IMPACT_AMOUNT
                                     FROM IMPACT_ALL A
                                              JOIN SESSION.ALLOCATION B ON A.PROC_KEY = B.PROC_KEY)
                               GROUP BY PROC_KEY,
                                        PROC_CODE,
                                        PROC_NAME,
                                        PRODUCT_CODE,
                                        PRODUCT_NAME,
                                        PRODUCT_VALUE,
                                        PRODUCT_UNIT,
                                        ITEM_CAT_CODE,
                                        ITEM_CAT_NAME,
                                        ITEM_CODE,
                                        ITEM_NAME,
                                        ITEM_UNIT,
                                        IMPACT_CATEGORY,
                                        UNIT_PROCESS_ID,
                                        ELEMENTARY_FLOW_ID,
                                        IMPACT_INDICATOR_ID,
                                        PCR_INDICATOR_ID),
         IMPACT_REST AS (SELECT A.PROC_KEY,
                                A.PROC_CODE,
                                A.PROC_NAME,
                                A.PRODUCT_CODE,
                                A.PRODUCT_NAME,
                                B.ITEM_VALUE AS PRODUCT_VALUE,
                                B.ITEM_UNIT  AS PRODUCT_UNIT,
                                A.ITEM_CAT_CODE,
                                A.ITEM_CAT_NAME,
                                A.ITEM_CODE,
                                A.ITEM_NAME,
                                A.ITEM_VALUE,
                                A.ITEM_UNIT,
                                A.IMPACT_CATEGORY,
                                A.UNIT_PROCESS_ID,
                                A.ELEMENTARY_FLOW_ID,
                                A.IMPACT_INDICATOR_ID,
                                A.PCR_INDICATOR_ID,
                                A.IMPACT_AMOUNT
                         FROM (SELECT *
                               FROM IMPACT_ALL I
                               WHERE NOT EXISTS (SELECT 1
                                                 FROM SESSION.ALLOCATION A
                                                 WHERE A.PROC_KEY = I.PROC_KEY)) A
                                  JOIN (SELECT DISTINCT *
                                        FROM SESSION.PROC_DATA_ORIGINAL
                                        WHERE ITEM_CAT_CODE = '04') B ON A.PROC_KEY = B.PROC_KEY)
    SELECT V_BIGCLASS_REC_ID             AS BIGCLASS_REC_ID,
           *,
           IMPACT_AMOUNT / PRODUCT_VALUE AS IMPACT_PER_UNIT_PRODUCT
    FROM (SELECT *
          FROM IMPACT_ALLOCATION
          UNION ALL
          SELECT *
          FROM IMPACT_REST)
    WHERE IMPACT_AMOUNT <> 0;


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX (BIGCLASS_REC_ID, SOURCE_PROC_KEY,
                                                                        TARGET_PROC_KEY, UNIT_COST, IS_INVERSED)
    WITH PROCESS_ALLOCATION AS (SELECT PROC_KEY,
                                       PROC_CODE,
                                       PROC_NAME,
                                       PRODUCT_CODE,
                                       PRODUCT_NAME,
                                       PRODUCT_VALUE,
                                       PRODUCT_UNIT,
                                       ITEM_CAT_CODE,
                                       ITEM_CAT_NAME,
                                       ITEM_CODE,
                                       ITEM_NAME,
                                       SUM(ITEM_VALUE) AS ITEM_VALUE,
                                       ITEM_UNIT
                                FROM (SELECT B.PROC_KEY_AFTER                   AS PROC_KEY,
                                             B.PROC_CODE_AFTER                  AS PROC_CODE,
                                             B.PROC_NAME_AFTER                  AS PROC_NAME,
                                             B.PRODUCT_CODE_AFTER               AS PRODUCT_CODE,
                                             B.PRODUCT_NAME_AFTER               AS PRODUCT_NAME,
                                             B.PRODUCT_VALUE_AFTER              AS PRODUCT_VALUE,
                                             B.PRODUCT_UNIT_AFTER               AS PRODUCT_UNIT,
                                             A.ITEM_CAT_CODE,
                                             A.ITEM_CAT_NAME,
                                             A.ITEM_CODE,
                                             A.ITEM_NAME,
                                             A.ITEM_VALUE * B.ALLOCATION_FACTOR AS ITEM_VALUE,
                                             A.ITEM_UNIT
                                      FROM (SELECT *
                                            FROM SESSION.PROC_DATA_ORIGINAL
                                            WHERE ITEM_CAT_CODE NOT IN ('04', '05')) A
                                               JOIN SESSION.ALLOCATION B ON A.PROC_KEY = B.PROC_KEY
                                      UNION
                                      SELECT DISTINCT PROC_KEY_AFTER      AS PROC_KEY,
                                                      PROC_CODE_AFTER     AS PROC_CODE,
                                                      PROC_NAME_AFTER     AS PROC_NAME,
                                                      PRODUCT_CODE_AFTER  AS PRODUCT_CODE,
                                                      PRODUCT_NAME_AFTER  AS PRODUCT_NAME,
                                                      PRODUCT_VALUE_AFTER AS PRODUCT_VALUE,
                                                      PRODUCT_UNIT_AFTER  AS PRODUCT_UNIT,
                                                      '04'                AS ITEM_CAT_CODE,
                                                      '产品'              AS ITEM_CAT_NAME,
                                                      PRODUCT_CODE_AFTER  AS ITEM_CODE,
                                                      PRODUCT_NAME_AFTER  AS ITEM_NAME,
                                                      PRODUCT_VALUE_AFTER AS ITEM_VALUE,
                                                      PRODUCT_UNIT_AFTER  AS ITEM_UNIT
                                      FROM SESSION.ALLOCATION)
                                GROUP BY PROC_KEY,
                                         PROC_CODE,
                                         PROC_NAME,
                                         PRODUCT_CODE,
                                         PRODUCT_NAME,
                                         PRODUCT_VALUE,
                                         PRODUCT_UNIT,
                                         ITEM_CAT_CODE,
                                         ITEM_CAT_NAME,
                                         ITEM_CODE,
                                         ITEM_NAME,
                                         ITEM_UNIT),
         PROCESS_REST AS (SELECT A.PROC_KEY,
                                 A.PROC_CODE,
                                 A.PROC_NAME,
                                 B.ITEM_CODE  AS PRODUCT_CODE,
                                 B.ITEM_NAME  AS PRODUCT_NAME,
                                 B.ITEM_VALUE AS PRODUCT_VALUE,
                                 B.ITEM_UNIT  AS PRODUCT_UNIT,
                                 A.ITEM_CAT_CODE,
                                 A.ITEM_CAT_NAME,
                                 A.ITEM_CODE,
                                 A.ITEM_NAME,
                                 A.ITEM_VALUE,
                                 A.ITEM_UNIT
                          FROM (SELECT *
                                FROM SESSION.PROC_DATA_ORIGINAL O
                                WHERE NOT EXISTS (SELECT 1
                                                  FROM SESSION.ALLOCATION A
                                                  WHERE A.PROC_KEY = O.PROC_KEY)) A
                                   JOIN (SELECT *
                                         FROM SESSION.PROC_DATA_ORIGINAL
                                         WHERE ITEM_CAT_CODE = '04') B ON A.PROC_KEY = B.PROC_KEY),
         PROCESS_ALL AS (SELECT *,
                                CAST(ITEM_VALUE AS DOUBLE) / CAST(PRODUCT_VALUE AS DOUBLE) AS UNIT_COST
                         FROM (SELECT *
                               FROM PROCESS_REST
                               UNION ALL
                               SELECT *
                               FROM PROCESS_ALLOCATION)),
         PRODUCT AS (SELECT DISTINCT PROC_KEY, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, PRODUCT_UNIT
                     FROM PROCESS_ALL)
    SELECT V_BIGCLASS_REC_ID,
           B.PROC_KEY AS SOURCE_PROC_KEY,
           A.PROC_KEY AS TARGET_PROC_KEY,
           A.UNIT_COST,
           FALSE
    FROM (SELECT * FROM PROCESS_ALL WHERE ITEM_CAT_CODE < '04') A
             JOIN PRODUCT B ON A.ITEM_CODE = B.PRODUCT_CODE;

    OPEN RES;
END;



