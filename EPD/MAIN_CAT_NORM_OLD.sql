CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_CALC(IN V_COMPANY_CODE VARCHAR(4),
                                                                             IN V_START_YM VARCHAR(100),
                                                                             IN V_END_YM VARCHAR(100),
                                                                             IN V_MAIN_CAT_TAB_NAME VARCHAR(100),
                                                                             IN V_MATRIX_TAB_NAME VARCHAR(100),
                                                                             IN V_MAIN_CAT_BATCH_NUMBER VARCHAR(100),
                                                                             IN V_FACTOR_YEAR VARCHAR(4),
                                                                             IN V_FACTOR_VERSION VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_CALC
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    CALLED ON NULL INPUT
    INHERIT SPECIAL REGISTERS
    OLD
        SAVEPOINT LEVEL
BEGIN
    DECLARE V_START_TIMESTAMP TIMESTAMP;
    DECLARE V_LAST_TIMESTAMP TIMESTAMP;

    DECLARE V_LOG_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102 '; --日志表所在SCHEMA
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_CALC'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);
    DECLARE MESSAGE_TEXT VARCHAR(2048);

    ------------------------------------日志变量定义------------------------------------
    DECLARE TAR_SCHEMA1 VARCHAR(32) DEFAULT 'BG00MAC102'; --目标表SCHEMA
    DECLARE SRC_TAB_NAME1 VARCHAR(32) DEFAULT ' '; --源表SCHEMA.表名
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_MAIN_CAT_EPD_NORM_CALC'; --临时表名
    DECLARE V_PNAME VARCHAR(50) DEFAULT 'P_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_CALC'; --存储过程名
    DECLARE V_QUERY_STR CLOB(1 M); --查询SQL
    DECLARE V_TMP_NAME VARCHAR(128);
    --完整的临时表名
    ------------------------------------存储过程变量定义---------------------------------

    --执行失败，记录日志
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            GET DIAGNOSTICS EXCEPTION 1 MESSAGE_TEXT = MESSAGE_TEXT;

            SET V_LAST_TIMESTAMP = CURRENT_TIMESTAMP;
            CALL BG00MAC102.P_WRITE_LOG(V_LOG_SCHEMA,
                                        V_ROUTINE_NAME,
                                        V_START_TIMESTAMP,
                                        V_LAST_TIMESTAMP,
                                        'F',
                                        V_QUERY_STR,
                                        'SQLCODE:' || TO_CHAR(SQLCODE) || '. SQLSTATE:' || SQLSTATE ||
                                        '. MESSAGE_TEXT:' || MESSAGE_TEXT);
        END;


    --开始时间
    SET V_START_TIMESTAMP = CURRENT_TIMESTAMP;

    --删除此存储过程创建的所有临时表（如果上次执行出错的话，有可能有些临时表没删）
    CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
    COMMIT;

    ------------------------------------处理逻辑(开始)------------------------------------

    --取活动数据
    SET V_QUERY_STR = 'SELECT REC_ID,
                      BATCH_NUMBER,
                      START_YM,
                      END_YM,
                      COMPANY_CODE,
                      CONCAT(CONCAT(LCA_PROC_CODE, ''_''), PRODUCT_CODE) AS PROC_KEY,
                      LCA_PROC_CODE                                    AS PROC_CODE,
                      LCA_PROC_NAME                                    AS PROC_NAME,
                      PRODUCT_CODE,
                      PRODUCT_NAME,
                      LCA_DATA_ITEM_CAT_CODE                           AS ITEM_CAT_CODE,
                      LCA_DATA_ITEM_CAT_NAME                           AS ITEM_CAT_NAME,
                      LCA_DATA_ITEM_CODE                               AS ITEM_CODE,
                      LCA_DATA_ITEM_NAME                               AS ITEM_NAME,
                      CASE
                          WHEN UNIT = ''万度'' THEN VALUE * 10000
                          WHEN UNIT = ''吨'' THEN VALUE * 1000
                          WHEN UNIT = ''千立方米'' THEN VALUE * 1000
                          ELSE VALUE
                          END
                                                                       AS VALUE,
                      CASE
                          WHEN UNIT = ''万度'' THEN ''度''
                          WHEN UNIT = ''吨'' THEN ''千克''
                          WHEN UNIT = ''千立方米'' THEN ''立方米''
                          ELSE UNIT
                          END
                                                                       AS UNIT
               FROM BG00MAC102.T_ADS_FACT_LCA_PROC_DATA
               WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                 AND BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT PROC_KEY,
                                       PROC_CODE,
                                       PROC_NAME,
                                       ITEM_CODE AS PRODUCT_CODE,
                                       ITEM_NAME AS PRODUCT_NAME,
                                       VALUE     AS PRODUCT_VALUE
                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                           WHERE ITEM_CAT_NAME = ''产品''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_PRODUCT_LIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.*,
                              B.PRODUCT_VALUE,
                              CAST(A.VALUE AS DOUBLE) / CAST(B.PRODUCT_VALUE AS DOUBLE) AS UNIT_COST
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA  A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
                                    ON A.PROC_KEY = B.PROC_KEY';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DATA', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT ITEM_CODE, ITEM_NAME
         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ITEM_BASE', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT PROC_KEY,
                              SOURCE_PROC_KEY,
                              UNIT_COST
                       FROM ' || V_TMP_SCHEMA || '.' || V_MATRIX_TAB_NAME || '
                       WHERE BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''
                         AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                         AND INV = ''Y''
                         AND UNIT_COST != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'MATRIX_INV', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT *
                       FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                       WHERE FLAG = ''FCP''
                         AND BASE_CODE = ''' || V_COMPANY_CODE || '''
                         AND START_TIME = ''' || V_FACTOR_YEAR || '''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FCP', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT *
                       FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                       WHERE FLAG = ''SY''
                         AND BASE_CODE = ''' || V_COMPANY_CODE || '''
                         AND START_TIME = ''' || V_FACTOR_YEAR || '''
                         AND DATA_CODE NOT IN (SELECT DATA_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FCP)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'SY', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT *
                       FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                       WHERE FLAG = ''LCI''
                         AND BASE_CODE = ''' || V_COMPANY_CODE || '''
                         AND START_TIME = ''' || V_FACTOR_YEAR || '''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT *
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FCP
                       UNION
                       SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_SY';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'UUID', V_QUERY_STR);


    SET V_QUERY_STR = '
        WITH STREAM AS (SELECT VERSION, UUID AS STREAM_ID,
                       NAME AS STREAM_NAME,
                       LCI_ELEMENT_CODE,
                       LCI_ELEMENT_NAME,
                       LCI_ELEMENT_VALUE
                FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                WHERE VERSION = ''' || V_FACTOR_VERSION || '''
                  AND FLAG = ''STREAM'')
        SELECT DISTINCT A.DATA_CODE,
                        A.UUID AS STREAM_ID,
                        B.VERSION,
                        B.STREAM_NAME,
                        B.LCI_ELEMENT_CODE,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI A
                 JOIN STREAM B ON A.UUID = B.STREAM_ID';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_STREAM', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT *
                       FROM BG00MAC102.T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION
                       WHERE VERSION = ''' || V_FACTOR_VERSION || '''
                       AND LCI_ELEMENT_CODE IS NOT NULL';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_LIST', V_QUERY_STR);


    SET V_QUERY_STR = '
            SELECT DATA_CODE AS ITEM_CODE,
                   LCI_ELEMENT_CODE,
                   LCI_ELEMENT_NAME,
                   LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM
            WHERE VERSION = ''' || V_FACTOR_VERSION || '''
            UNION
            SELECT A.ITEM_CODE,
                   A.LCI_ELEMENT_CODE,
                   A.LCI_ELEMENT_NAME,
                   A.LCI_ELEMENT_VALUE * B.HOTVALUE AS LCI_ELEMENT_VALUE
            FROM
                (SELECT DATA_CODE AS ITEM_CODE,
                        LCI_ELEMENT_CODE,
                        LCI_ELEMENT_NAME,
                        LCI_ELEMENT_VALUE
                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM
                 WHERE LCI_ELEMENT_CODE IN (''RSF'', ''NRSF'')) A
                    LEFT JOIN (SELECT *
                               FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                               WHERE START_TIME = ''' || V_FACTOR_YEAR || '''
                                 AND ORG_CODE = ''' || V_COMPANY_CODE || ''') B ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_EP', V_QUERY_STR);


    SET V_QUERY_STR = '
            SELECT A.ITEM_CODE AS ITEM_CODE,
                   ''GWP-total'' AS LCI_ELEMENT_CODE,
                   (SELECT LCI_ELEMENT_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST
                      WHERE LCI_ELEMENT_CODE = ''GWP-total'') AS LCI_ELEMENT_NAME,
                   B.DISCH_COEFF AS LCI_ELEMENT_VALUE
            FROM (SELECT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE) A
                  JOIN
                 (SELECT DISTINCT *
                  FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                  WHERE ORG_CODE = ''' || V_COMPANY_CODE || '''
                    AND START_TIME = ''' || V_FACTOR_YEAR || ''') B
                 ON A.ITEM_CODE = B.ITEM_CODE
            WHERE B.DISCH_COEFF IS NOT NULL
            UNION
            SELECT A.ITEM_CODE AS ITEM_CODE,
                   ''GWP-fossil'' AS LCI_ELEMENT_CODE,
                (SELECT LCI_ELEMENT_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST
                 WHERE LCI_ELEMENT_CODE = ''GWP-fossil'') AS LCI_ELEMENT_NAME,
                   B.DISCH_COEFF AS LCI_ELEMENT_VALUE
            FROM (SELECT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE) A
                     JOIN
                 (SELECT DISTINCT *
                  FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                  WHERE ORG_CODE = ''' || V_COMPANY_CODE || '''
                    AND START_TIME = ''' || V_FACTOR_YEAR || ''') B
                 ON A.ITEM_CODE = B.ITEM_CODE
            WHERE B.DISCH_COEFF IS NOT NULL
            UNION
            SELECT A.ITEM_CODE AS ITEM_CODE,
                   ''PENRT'' AS LCI_ELEMENT_CODE,
                   (SELECT LCI_ELEMENT_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST
                      WHERE LCI_ELEMENT_CODE = ''PENRT'') AS LCI_ELEMENT_NAME,
                   B.HOTVALUE AS LCI_ELEMENT_VALUE
            FROM (SELECT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE) A
                JOIN
                 (SELECT DISTINCT *
                  FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                  WHERE ORG_CODE = ''' || V_COMPANY_CODE || '''
                    AND START_TIME = ''' || V_FACTOR_YEAR || ''') B
                  ON A.ITEM_CODE = B.ITEM_CODE
            WHERE B.HOTVALUE IS NOT NULL
            UNION
            SELECT A.ITEM_CODE AS ITEM_CODE,
                   ''PENRE'' AS LCI_ELEMENT_CODE,
                   (SELECT LCI_ELEMENT_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST
                      WHERE LCI_ELEMENT_CODE = ''PENRE'') AS LCI_ELEMENT_NAME,
                   B.HOTVALUE AS LCI_ELEMENT_VALUE
            FROM (SELECT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE) A
                 JOIN
                 (SELECT DISTINCT *
                  FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                  WHERE ORG_CODE = ''' || V_COMPANY_CODE || '''
                    AND START_TIME = ''' || V_FACTOR_YEAR || ''') B
                 ON A.ITEM_CODE = B.ITEM_CODE
            WHERE B.HOTVALUE IS NOT NULL';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_FUEL', V_QUERY_STR);


    SET V_QUERY_STR = '
            SELECT *
            FROM
            ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_EP
            UNION
            SELECT *
            FROM
            ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FUEL';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_DIRECT', V_QUERY_STR);


    SET V_QUERY_STR = '
     WITH TRANS_DATA AS (SELECT LCA_DATA_ITEM_CODE           AS ITEM_CODE,
                           RIVER_CAR_TRANS_VALUE / 1000 AS RIVER_CAR,
                           TRUCK_CAR_TRANS_VALUE / 1000 AS TRUCK_CAR,
                           TRAIN_TRANS_VALUE / 1000     AS TRAIN,
                           CUSTOMS_TRANS_VALUE / 1000   AS CUSTOMS
                    FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                    WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                      AND START_TIME = ''' || V_FACTOR_YEAR || '''),
     FACTOR_DISTANCE AS (SELECT A.ITEM_CODE,
                                A.ITEM_NAME,
                                COALESCE(B.RIVER_CAR, 0) AS RIVER_CAR,
                                COALESCE(B.TRUCK_CAR, 0) AS TRUCK_CAR,
                                COALESCE(B.TRAIN, 0)     AS TRAIN,
                                COALESCE(B.CUSTOMS, 0)   AS CUSTOMS
                         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE A
                                  LEFT JOIN TRANS_DATA B ON A.ITEM_CODE = B.ITEM_CODE),
     FACTOR_TRANSPORT1 AS (SELECT *
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                           WHERE VERSION = ''' || V_FACTOR_VERSION || '''
                             AND LCI_ELEMENT_CODE IN (SELECT LCI_ELEMENT_CODE
                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST)
                             AND NAME IN (''海运'', ''河运'', ''铁运'', ''汽运'')),
     ALL_COMBINED AS (SELECT F.ITEM_CODE,
                             F.ITEM_NAME,
                             T.LCI_ELEMENT_CODE,
                             T.LCI_ELEMENT_NAME,
                             CASE
                                 WHEN T.NAME = ''河运'' THEN F.RIVER_CAR * T.LCI_ELEMENT_VALUE
                                 WHEN T.NAME = ''汽运'' THEN F.TRUCK_CAR * T.LCI_ELEMENT_VALUE
                                 WHEN T.NAME = ''铁运'' THEN F.TRAIN * T.LCI_ELEMENT_VALUE
                                 WHEN T.NAME = ''海运'' THEN F.CUSTOMS * T.LCI_ELEMENT_VALUE
                                 ELSE 0
                                 END AS LCI_ELEMENT_VALUE
                      FROM FACTOR_DISTANCE F
                               JOIN FACTOR_TRANSPORT1 T ON 1 = 1)
      SELECT *
      FROM (SELECT ITEM_CODE,
                   ITEM_NAME,
                   LCI_ELEMENT_CODE,
                   LCI_ELEMENT_NAME,
                   SUM(LCI_ELEMENT_VALUE) AS LCI_ELEMENT_VALUE
            FROM ALL_COMBINED
            GROUP BY ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME)
      WHERE LCI_ELEMENT_VALUE != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_TRANSPORT', V_QUERY_STR);


    SET V_QUERY_STR = '
    WITH MAT AS (SELECT A.DATA_CODE,
                        A.FLAG,
                        B.NAME,
                        B.LCI_ELEMENT_CODE,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE
                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_UUID A
                          JOIN (SELECT *
                                FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM
                                WHERE VERSION = ''' || V_FACTOR_VERSION || '''
                                  AND FLAG = ''MAT''
                                  AND LCI_ELEMENT_CODE IN
                                      (SELECT LCI_ELEMENT_CODE
                                     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST)) B
                               ON A.UUID = B.UUID)
        SELECT DISTINCT A.ITEM_CODE,
                        A.ITEM_NAME,
                        B.FLAG,
                        B.NAME AS MAT_NAME,
                        B.LCI_ELEMENT_CODE,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE
        FROM (SELECT ITEM_CODE, ITEM_NAME
              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE) A
             JOIN
             (SELECT * FROM MAT) B
             ON A.ITEM_CODE = B.DATA_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_MAT', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT ITEM_CODE, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT
    WHERE FLAG = ''FCP''
      AND LCI_ELEMENT_VALUE != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_FCP', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT ITEM_CODE, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT
    WHERE FLAG = ''SY''
      AND LCI_ELEMENT_VALUE != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_SY', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT *
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
    WHERE ITEM_CAT_CODE < ''04''
      AND ITEM_CODE NOT IN (SELECT PRODUCT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'INPUT', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT *
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
    WHERE ITEM_CAT_CODE IN (''04'', ''05'', ''08'')';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'OUTPUT', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT *
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
    WHERE ITEM_CAT_CODE IN (''06'', ''07'')';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'EMISSION', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT *
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
    WHERE ITEM_CAT_CODE = ''05''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'BY_PRODUCT', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT REC_ID,
               BATCH_NUMBER,
               START_YM,
               END_YM,
               COMPANY_CODE,
               PROC_KEY,
               PROC_CODE,
               PROC_NAME,
               PRODUCT_CODE,
               PRODUCT_NAME,
               ITEM_CAT_CODE,
               ITEM_CAT_NAME,
               ITEM_CODE,
               ITEM_NAME,
               ABS(VALUE) AS VALUE,
               UNIT,
               PRODUCT_VALUE,
               ABS(UNIT_COST) AS UNIT_COST
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE ITEM_CAT_CODE = ''08''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'WASTE', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              CASE
                                  WHEN ITEM_CAT_CODE < ''04'' THEN A.UNIT_COST * B.LCI_ELEMENT_VALUE
                                  ELSE -ABS(A.UNIT_COST * B.LCI_ELEMENT_VALUE) END AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                                INNER JOIN (SELECT *
                                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FUEL) B
                                           ON A.ITEM_CODE = B.ITEM_CODE
                       UNION
                       SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                                INNER JOIN (SELECT *
                                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_EP) B
                                    ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C1_DIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              ''C1''               AS TYPE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C1_DIST A
                                CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                       AND A.LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C1_CYCLE', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH C1_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                                                B.PROC_KEY           AS TARGET_PROC_KEY,
                                                A.LCI_ELEMENT_CODE,
                                                A.LCI_ELEMENT_NAME,
                                                A.LOAD * B.UNIT_COST AS COST
                                         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C1_DIST A
                                                  CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                                         WHERE A.PROC_KEY = B.SOURCE_PROC_KEY)
                        SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, SUM(COST) AS C1
                        FROM C1_CYCLE
                        GROUP BY TARGET_PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C1_AGG', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT A
                                INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                      '_FACTOR_FCP B ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C2_DIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              ''C2''               AS TYPE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C2_DIST A
                                CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                       AND A.LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C2_CYCLE', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH C2_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                                                B.PROC_KEY           AS TARGET_PROC_KEY,
                                                A.LCI_ELEMENT_CODE,
                                                A.LCI_ELEMENT_NAME,
                                                A.LOAD * B.UNIT_COST AS COST
                                         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C2_DIST A
                                                  CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                                         WHERE A.PROC_KEY = B.SOURCE_PROC_KEY)
                        SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, SUM(COST) AS C2
                        FROM C2_CYCLE
                        GROUP BY TARGET_PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C2_AGG', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT A
                                INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                      '_FACTOR_SY B ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C3_DIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              ''C3''               AS TYPE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C3_DIST A
                                CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                       AND A.LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C3_CYCLE', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH C3_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                                                B.PROC_KEY           AS TARGET_PROC_KEY,
                                                A.LCI_ELEMENT_CODE,
                                                A.LCI_ELEMENT_NAME,
                                                A.LOAD * B.UNIT_COST AS COST
                                         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C3_DIST A
                                                  CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                                         WHERE A.PROC_KEY = B.SOURCE_PROC_KEY)
                        SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, SUM(COST) AS C3
                        FROM C3_CYCLE
                        GROUP BY TARGET_PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C3_AGG', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              -ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_BY_PRODUCT A
                                INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT B
                                           ON A.ITEM_CODE = B.ITEM_CODE
                       UNION
                       SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_WASTE A
                                INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                      '_FACTOR_MAT B ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C4_DIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              ''C4''               AS TYPE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C4_DIST A
                                CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                       AND A.LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C4_CYCLE', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH C4_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                                                B.PROC_KEY           AS TARGET_PROC_KEY,
                                                A.LCI_ELEMENT_CODE,
                                                A.LCI_ELEMENT_NAME,
                                                A.LOAD * B.UNIT_COST AS COST
                                         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C4_DIST A
                                                  CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                                         WHERE A.PROC_KEY = B.SOURCE_PROC_KEY)
                        SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, SUM(COST) AS C4
                        FROM C4_CYCLE
                        GROUP BY TARGET_PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C4_AGG', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT PROC_KEY,
                              PROC_NAME,
                              PRODUCT_NAME,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.UNIT_COST,
                              B.LCI_ELEMENT_CODE,
                              B.LCI_ELEMENT_NAME,
                              B.LCI_ELEMENT_VALUE,
                              A.UNIT_COST * B.LCI_ELEMENT_VALUE AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT A
                                INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                      '_FACTOR_TRANSPORT B ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C5_DIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                              B.PROC_KEY           AS TARGET_PROC_KEY,
                              A.ITEM_CAT_CODE,
                              A.ITEM_CAT_NAME,
                              A.ITEM_CODE,
                              A.ITEM_NAME,
                              A.LCI_ELEMENT_CODE,
                              ''C5''               AS TYPE,
                              A.LOAD * B.UNIT_COST AS LOAD
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C5_DIST A
                                CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                       WHERE A.PROC_KEY = B.SOURCE_PROC_KEY
                       AND A.LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C5_CYCLE', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH C5_CYCLE AS (SELECT A.PROC_KEY           AS SOURCE_PROC_KEY,
                                                B.PROC_KEY           AS TARGET_PROC_KEY,
                                                A.LCI_ELEMENT_CODE,
                                                A.LCI_ELEMENT_NAME,
                                                A.LOAD * B.UNIT_COST AS COST
                                         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C5_DIST A
                                                  CROSS JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MATRIX_INV B
                                         WHERE A.PROC_KEY = B.SOURCE_PROC_KEY)
                        SELECT TARGET_PROC_KEY AS PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, SUM(COST) AS C5
                        FROM C5_CYCLE
                        GROUP BY TARGET_PROC_KEY, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'C5_AGG', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH RESULT1 AS (SELECT HEX(RAND())                      AS REC_ID,
                                             ''' || V_MAIN_CAT_BATCH_NUMBER || '''  AS BATCH_NUMBER,
                                             ''' || V_COMPANY_CODE || '''       AS COMPANY_CODE,
                                             A.PROC_KEY,
                                             A.PROC_CODE,
                                             A.PROC_NAME,
                                             A.PRODUCT_CODE,
                                             A.PRODUCT_NAME,
                                             A.LCI_ELEMENT_CODE,
                                             COALESCE(C1, 0)          AS C1_DIRECT,
                                             COALESCE(C2, 0)          AS C2_BP,
                                             COALESCE(C3, 0)          AS C3_OUT,
                                             COALESCE(C4, 0)          AS C4_BP_NEG,
                                             COALESCE(C5, 0)          AS C5_TRANS
                                      FROM (SELECT *
                                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST
                                            LEFT JOIN (SELECT DISTINCT LCI_ELEMENT_CODE, LCI_ELEMENT_NAME FROM ' ||
                      V_TMP_SCHEMA || '.' ||
                      V_TMP_TAB || '_LCI_LIST) ON 1 = 1) A
                                               LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C1_AGG C1_AGG ON A.PROC_KEY = C1_AGG.PROC_KEY AND A.LCI_ELEMENT_CODE = C1_AGG.LCI_ELEMENT_CODE
                                               LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C2_AGG C2_AGG ON A.PROC_KEY = C2_AGG.PROC_KEY AND A.LCI_ELEMENT_CODE = C2_AGG.LCI_ELEMENT_CODE
                                               LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C3_AGG C3_AGG ON A.PROC_KEY = C3_AGG.PROC_KEY AND A.LCI_ELEMENT_CODE = C3_AGG.LCI_ELEMENT_CODE
                                               LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C4_AGG C4_AGG ON A.PROC_KEY = C4_AGG.PROC_KEY AND A.LCI_ELEMENT_CODE = C4_AGG.LCI_ELEMENT_CODE
                                               LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C5_AGG C5_AGG ON A.PROC_KEY = C5_AGG.PROC_KEY AND A.LCI_ELEMENT_CODE = C5_AGG.LCI_ELEMENT_CODE
                                      ORDER BY A.PROC_KEY, A.LCI_ELEMENT_CODE)
                      SELECT *,
                             C1_DIRECT + C2_BP                                   AS C_INSITE,
                             C3_OUT + C4_BP_NEG + C5_TRANS                       AS C_OUTSITE,
                             C1_DIRECT + C2_BP + C3_OUT + C4_BP_NEG + C5_TRANS   AS C_CYCLE,
                             TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')      AS REC_CREATE_TIME
                      FROM RESULT1';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT', V_QUERY_STR);


    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_TAB_NAME || '
                       WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''
                       AND BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_TAB_NAME || ' (REC_ID, BATCH_NUMBER, FACTOR_VERSION, START_YM, END_YM, COMPANY_CODE,
                                                              PROC_KEY, PROC_CODE, PROC_NAME, PRODUCT_CODE,
                                                              PRODUCT_NAME, LCI_ELEMENT_CODE,
                                                              C1_DIRECT, C2_BP, C3_OUT, C4_BP_NEG, C5_TRANS, C_INSITE,
                                                              C_OUTSITE, C_CYCLE, REC_CREATE_TIME)
                       SELECT REC_ID,
                              BATCH_NUMBER,
                              ''' || V_FACTOR_VERSION || ''',
                              ''' || V_START_YM || ''',
                              ''' || V_END_YM || ''',
                              COMPANY_CODE,
                              PROC_KEY,
                              PROC_CODE,
                              PROC_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              LCI_ELEMENT_CODE,
                              C1_DIRECT,
                              C2_BP,
                              C3_OUT,
                              C4_BP_NEG,
                              C5_TRANS,
                              C_INSITE,
                              C_OUTSITE,
                              C_CYCLE,
                              REC_CREATE_TIME
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    -- DIST
    SET V_QUERY_STR = 'DELETE FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
                       WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'INSERT INTO BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_EPD_NORM_DIST
                        (REC_ID, BATCH_NUMBER, START_YM, END_YM, COMPANY_CODE, SOURCE_PROC_KEY, SOURCE_PROC_CODE, SOURCE_PROC_NAME,
                         TARGET_PROC_KEY, TARGET_PROC_CODE, TARGET_PROC_NAME, PRODUCT_CODE, PRODUCT_NAME, ITEM_CAT_CODE, ITEM_CAT_NAME,
                         ITEM_CODE, ITEM_NAME, LCI_ELEMENT_CODE, TYPE, LOAD, REC_CREATE_TIME)
                        SELECT HEX(RAND())                              AS REC_ID,
                               ''' || V_MAIN_CAT_BATCH_NUMBER || '''    AS BATCH_NUMBER,
                               ''' || V_START_YM || '''                 AS START_YM,
                               ''' || V_END_YM || '''                   AS END_YM,
                               ''' || V_COMPANY_CODE || '''             AS COMPANY_CODE,
                               A.SOURCE_PROC_KEY,
                               B.PROC_CODE              AS SOURCE_PROC_CODE,
                               B.PROC_NAME              AS SOURCE_PROC_NAME,
                               A.TARGET_PROC_KEY,
                               C.PROC_CODE              AS TARGET_PROC_CODE,
                               C.PROC_NAME              AS TARGET_PROC_NAME,
                               C.PRODUCT_CODE           AS PRODUCT_CODE,
                               C.PRODUCT_NAME           AS PRODUCT_NAME,
                               ITEM_CAT_CODE,
                               ITEM_CAT_NAME,
                               ITEM_CODE,
                               ITEM_NAME,
                               LCI_ELEMENT_CODE,
                               TYPE,
                               LOAD,
                               TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')
                        FROM (SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C1_CYCLE
                              UNION
                              SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C2_CYCLE
                              UNION
                              SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C3_CYCLE
                              UNION
                              SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C4_CYCLE
                              UNION
                              SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_C5_CYCLE) A
                                 JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B ON A.SOURCE_PROC_KEY = B.PROC_KEY
                                 JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                      '_PROC_PRODUCT_LIST C ON A.TARGET_PROC_KEY = C.PROC_KEY';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    ------------------------------------处理逻辑(结束)------------------------------------

--     删除生成的临时表
--     CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
--     COMMIT;


    --过程结束时间
    SET V_LAST_TIMESTAMP = CURRENT_TIMESTAMP;

    --执行成功，记录日志
    CALL BG00MAC102.P_WRITE_LOG(V_LOG_SCHEMA,
                                V_ROUTINE_NAME,
                                V_START_TIMESTAMP,
                                V_LAST_TIMESTAMP,
                                'T',
                                V_PARM_INFO,
                                'OK');
END;

