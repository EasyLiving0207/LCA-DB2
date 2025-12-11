CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_GWP_NORM_CALC(IN V_COMPANY_CODE VARCHAR(4),
                                                                             IN V_START_MONTH VARCHAR(6),
                                                                             IN V_END_MONTH VARCHAR(6),
                                                                             IN V_FACTOR_YEAR VARCHAR(4),
                                                                             IN V_FACTOR_VERSION VARCHAR(100),
                                                                             IN V_MAIN_CAT_RESULT_TAB_NAME VARCHAR(100),
                                                                             IN V_MAIN_CAT_BATCH_NUMBER VARCHAR(100),
                                                                             IN V_SUBCLASS_TAB_NAME VARCHAR(100),
                                                                             IN V_SUBCLASS_RESULT_TAB_NAME VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_GWP_NORM_CALC
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

    DECLARE V_LOG_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --日志表所在SCHEMA
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_GWP_NORM_CALC'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);
    DECLARE MESSAGE_TEXT VARCHAR(2048);

    ------------------------------------日志变量定义------------------------------------
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_GWP_NORM_CALC'; --临时表名
    DECLARE V_QUERY_STR CLOB(1 M);
    --查询SQL
    --完整的临时表名
    ------------------------------------存储过程变量定义---------------------------------

    DECLARE V_ROW_COUNT INT;
    DECLARE V_RANK INT DEFAULT 1;
    DECLARE V_MAX_RANK INT;

    DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN
    END;
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.RANK_PROC';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.DIST_BUFFER';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.RESULT_BUFFER';

    DECLARE GLOBAL TEMPORARY TABLE SESSION.RANK_PROC (
        INDEX_CODE VARCHAR(1200),
        PREV_INDEX_CODE VARCHAR(1200),
        SUM_FLAG BIGINT
        ) ON COMMIT PRESERVE ROWS;

    DECLARE GLOBAL TEMPORARY TABLE SESSION.DIST_BUFFER (
        UPDATE_DATE VARCHAR(10),
        INDEX_CODE VARCHAR(1200),
        MAT_NO VARCHAR(64),
        MAT_TRACK_NO VARCHAR(64),
        MAT_SEQ_NO BIGINT,
        FAMILY_CODE VARCHAR(1000),
        UNIT_CODE VARCHAR(100),
        UNIT_NAME VARCHAR(256),
        PRODUCT_CODE VARCHAR(100),
        PRODUCT_NAME VARCHAR(256),
        PRODUCT_VALUE DECIMAL(27, 6),
        MAT_WT DECIMAL(18, 3),
        MAT_STATUS VARCHAR(10),
        TYPE_CODE VARCHAR(20),
        TYPE_NAME VARCHAR(64),
        ITEM_CODE VARCHAR(100),
        ITEM_NAME VARCHAR(256),
        VALUE DECIMAL(27, 6),
        UNITM_AC VARCHAR(64),
        UNIT_COST DOUBLE,
        LCI_ELEMENT_CODE VARCHAR(256),
        C1 DOUBLE,
        C2 DOUBLE,
        C3 DOUBLE,
        C4 DOUBLE,
        C5 DOUBLE,
        FLAG VARCHAR(20)
        ) ON COMMIT PRESERVE ROWS;

    DECLARE GLOBAL TEMPORARY TABLE SESSION.RESULT_BUFFER (
        UPDATE_DATE VARCHAR(10),
        INDEX_CODE VARCHAR(1200),
        MAT_NO VARCHAR(64),
        MAT_TRACK_NO VARCHAR(64),
        MAT_SEQ_NO BIGINT,
        FAMILY_CODE VARCHAR(1000),
        UNIT_CODE VARCHAR(100),
        UNIT_NAME VARCHAR(256),
        PRODUCT_CODE VARCHAR(100),
        PRODUCT_NAME VARCHAR(256),
        PRODUCT_VALUE DECIMAL(27, 6),
        MAT_WT DECIMAL(18, 3),
        MAT_STATUS VARCHAR(10),
        TYPE_CODE VARCHAR(20),
        TYPE_NAME VARCHAR(64),
        ITEM_CODE VARCHAR(100),
        ITEM_NAME VARCHAR(256),
        VALUE DECIMAL(27, 6),
        UNITM_AC VARCHAR(64),
        UNIT_COST DOUBLE,
        LCI_ELEMENT_CODE VARCHAR(256),
        C1 DOUBLE,
        C2 DOUBLE,
        C3 DOUBLE,
        C4 DOUBLE,
        C5 DOUBLE,
        FLAG VARCHAR(32)
        ) ON COMMIT PRESERVE ROWS;


    --开始时间
    SET V_START_TIMESTAMP = CURRENT_TIMESTAMP;

    --删除此存储过程创建的所有临时表（如果上次执行出错的话，有可能有些临时表没删）
    CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
    COMMIT;

    ------------------------------------处理逻辑(开始)------------------------------------

    TRUNCATE TABLE BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG IMMEDIATE;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'begin', null, CURRENT_TIMESTAMP);

    --取大类结果
    SET V_QUERY_STR = 'SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_RESULT_TAB_NAME || '
                       WHERE BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''
                       AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''
                       AND LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ENERGY_RESULT', V_QUERY_STR);

    --取活动数据
    SET V_QUERY_STR = '
               SELECT UPDATE_DATE,
                      MAT_TRACK_NO,
                      MAT_NO,
                      MAT_SEQ_NO,
                      CASE WHEN FAMILY_CODE IS NULL THEN ''00''
                      WHEN FAMILY_CODE = '''' THEN ''00''
                      ELSE FAMILY_CODE END AS FAMILY_CODE,
                      UNIT_CODE,
                      UNIT_NAME,
                      MAT_WT,
                      MAT_STATUS,
                      TYPE_CODE,
                      TYPE_NAME,
                      ITEM_CODE,
                      ITEM_NAME,
                      CASE WHEN UNITM_AC = ''万度'' THEN VALUE * 10000
                      WHEN UNITM_AC = ''吨'' THEN VALUE * 1000
                      WHEN UNITM_AC = ''千立方米'' THEN VALUE * 1000
                      ELSE VALUE END AS VALUE,
                      CASE WHEN UNITM_AC = ''万度'' THEN ''度''
                      WHEN UNITM_AC = ''吨'' THEN ''千克''
                      WHEN UNITM_AC = ''公斤'' THEN ''千克''
                      WHEN UNITM_AC = ''千立方米'' THEN ''立方米''
                      ELSE UNITM_AC END AS UNITM_AC
                      FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                      WHERE VALUE IS NOT NULL AND VALUE != 0
                      AND MAT_TRACK_NO NOT IN (SELECT DISTINCT MAT_TRACK_NO
                           FROM (SELECT MAT_TRACK_NO, MAX(FAMILY_CODE) AS MAX_NO
                                 FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                                 GROUP BY MAT_TRACK_NO)
                           WHERE MAX_NO IS NULL)';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT MAT_TRACK_NO FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'MAT_TRACK_NO', V_QUERY_STR);


    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                                  INDEX_CODE,
                                  MAT_NO,
                                  A.MAT_TRACK_NO,
                                  MAT_SEQ_NO,
                                  FAMILY_CODE,
                                  UNIT_CODE,
                                  UNIT_NAME,
                                  PRODUCT_CODE,
                                  PRODUCT_NAME,
                                  PRODUCT_VALUE,
                                  MAT_WT,
                                  MAT_STATUS,
                                  TYPE_CODE,
                                  TYPE_NAME,
                                  ITEM_CODE,
                                  ITEM_NAME,
                                  VALUE,
                                  UNITM_AC,
                                  UNIT_COST,
                                  LCI_ELEMENT_CODE,
                                  C1,
                                  C2,
                                  C3,
                                  C4,
                                  C5,
                                  FLAG
                           FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_PARALLEL A
                                    JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO B
                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                           WHERE UPDATE_DATE < ''' || V_START_MONTH || '''
                           AND SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || '''
                           AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                           AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''
                           AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''';
        CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PREV_RESULT', V_QUERY_STR);

        SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                                  A.MAT_TRACK_NO,
                                  MAT_NO,
                                  MAT_SEQ_NO,
                                  CASE WHEN FAMILY_CODE IS NULL THEN ''00''
                                  WHEN FAMILY_CODE = '''' THEN ''00''
                                  ELSE FAMILY_CODE END AS FAMILY_CODE,
                                  UNIT_CODE,
                                  UNIT_NAME,
                                  MAT_WT,
                                  MAT_STATUS,
                                  TYPE_CODE,
                                  TYPE_NAME,
                                  ITEM_CODE,
                                  ITEM_NAME,
                                  CASE WHEN UNITM_AC = ''万度'' THEN VALUE * 10000
                                  WHEN UNITM_AC = ''吨'' THEN VALUE * 1000
                                  WHEN UNITM_AC = ''千立方米'' THEN VALUE * 1000
                                  ELSE VALUE END AS VALUE,
                                  CASE WHEN UNITM_AC = ''万度'' THEN ''度''
                                  WHEN UNITM_AC = ''吨'' THEN ''千克''
                                  WHEN UNITM_AC = ''公斤'' THEN ''千克''
                                  WHEN UNITM_AC = ''千立方米'' THEN ''立方米''
                                  ELSE UNITM_AC END AS UNITM_AC
                                  FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || ' A
                                  JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO B
                                  ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                  WHERE VALUE IS NOT NULL AND VALUE != 0
                                  AND A.MAT_TRACK_NO NOT IN (SELECT DISTINCT MAT_TRACK_NO
                                       FROM (SELECT MAT_TRACK_NO, MAX(FAMILY_CODE) AS MAX_NO
                                             FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                                             GROUP BY MAT_TRACK_NO)
                                       WHERE MAX_NO IS NULL)
                                  AND UPDATE_DATE < ''' || V_START_MONTH || '''';
        CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PREV_TEMP_DATA', V_QUERY_STR);
    END IF;


    --PROC信息
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = 'SELECT DISTINCT UPDATE_DATE,
                                  CASE
                                     WHEN FAMILY_CODE = ''00'' THEN CONCAT(CONCAT(CONCAT(CONCAT(MAT_TRACK_NO, ''_''), FAMILY_CODE), ''_''), MAT_SEQ_NO)
                                     ELSE CONCAT(CONCAT(MAT_TRACK_NO, ''_''), FAMILY_CODE) END      AS INDEX_CODE,
                                  MAT_TRACK_NO,
                                  MAT_SEQ_NO,
                                  FAMILY_CODE,
                                  UNIT_CODE,
                                  UNIT_NAME,
                                  ITEM_CODE AS PRODUCT_CODE,
                                  ITEM_NAME AS PRODUCT_NAME,
                                  VALUE AS PRODUCT_VALUE
                             FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                                   UNION
                                   SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PREV_TEMP_DATA)
                             WHERE TYPE_CODE = ''04''';
    ELSE
        SET V_QUERY_STR = 'SELECT DISTINCT UPDATE_DATE,
                                  CASE
                                     WHEN FAMILY_CODE = ''00'' THEN CONCAT(CONCAT(CONCAT(CONCAT(MAT_TRACK_NO, ''_''), FAMILY_CODE), ''_''), MAT_SEQ_NO)
                                     ELSE CONCAT(CONCAT(MAT_TRACK_NO, ''_''), FAMILY_CODE) END      AS INDEX_CODE,
                                  MAT_TRACK_NO,
                                  MAT_SEQ_NO,
                                  FAMILY_CODE,
                                  UNIT_CODE,
                                  UNIT_NAME,
                                  ITEM_CODE AS PRODUCT_CODE,
                                  ITEM_NAME AS PRODUCT_NAME,
                                  VALUE AS PRODUCT_VALUE
                             FROM
                                 ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                             WHERE TYPE_CODE = ''04''';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_PRODUCT_LIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.UPDATE_DATE,
                              CASE
                              WHEN A.FAMILY_CODE = ''00'' THEN CONCAT(CONCAT(CONCAT(CONCAT(A.MAT_TRACK_NO, ''_''), A.FAMILY_CODE), ''_''), A.MAT_SEQ_NO)
                              ELSE CONCAT(CONCAT(A.MAT_TRACK_NO, ''_''), A.FAMILY_CODE) END      AS INDEX_CODE,
                              A.MAT_NO,
                              A.MAT_TRACK_NO,
                              A.MAT_SEQ_NO,
                              A.FAMILY_CODE,
                              A.UNIT_CODE,
                              A.UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              ABS(VALUE) AS VALUE,
                              UNITM_AC,
                              PRODUCT_VALUE,
                              CAST(ABS(VALUE) AS DOUBLE) / CAST(PRODUCT_VALUE AS DOUBLE) AS UNIT_COST
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA A
                              JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                               AND A.FAMILY_CODE = B.FAMILY_CODE
                               AND A.UNIT_CODE = B.UNIT_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DATA', V_QUERY_STR);

    --工艺路径
    SET V_QUERY_STR = 'WITH TEMP AS (SELECT A.*, B.PREV_RANK
                                     FROM (SELECT DISTINCT UPDATE_DATE, INDEX_CODE, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME
                                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA) A
                                              JOIN (SELECT DISTINCT A.MAT_TRACK_NO, MAX(A.MAT_SEQ_NO) AS PREV_RANK
                                                    FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || ' A
                                                    JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO B
                                                      ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                    WHERE A.FAMILY_CODE IS NULL
                                                    GROUP BY A.MAT_TRACK_NO) B
                                                   ON A.MAT_TRACK_NO = B.MAT_TRACK_NO),
                       RANK AS (SELECT *,
                                       CASE
                                           WHEN FAMILY_CODE = ''00'' AND MAT_SEQ_NO = 1 THEN NULL
                                           WHEN FAMILY_CODE = ''00'' AND MAT_SEQ_NO > 1 THEN CONCAT(
                                                   CONCAT(CONCAT(CONCAT(MAT_TRACK_NO, ''_''), FAMILY_CODE), ''_''), MAT_SEQ_NO - 1)
                                           WHEN FAMILY_CODE = ''01'' THEN CONCAT(CONCAT(CONCAT(CONCAT(MAT_TRACK_NO, ''_''), ''00''), ''_''), PREV_RANK)
                                           ELSE CONCAT(CONCAT(MAT_TRACK_NO, ''_''), LEFT(FAMILY_CODE, LENGTH(FAMILY_CODE) - 2))
                                           END                                                          AS PREV_INDEX_CODE,
                                       CASE
                                           WHEN FAMILY_CODE = ''00'' THEN MAT_SEQ_NO
                                           ELSE CAST(LENGTH(FAMILY_CODE) / 2 AS BIGINT) + PREV_RANK END AS RANK
                                FROM TEMP),
                       DEPT AS (SELECT A.*,
                                       B.DEPT_CODE,
                                       B.DEPT_NAME,
                                       B.DEPT_MID_NAME
                                FROM RANK A
                                         LEFT JOIN (SELECT *
                                                      FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022
                                                      WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || ''') B
                                              ON A.UNIT_CODE = B.UNIT_CODE),
                       DEPT_PREV AS (SELECT A.*,
                                            B.DEPT_CODE     AS PREV_DEPT_CODE,
                                            B.DEPT_NAME     AS PREV_DEPT_NAME,
                                            B.DEPT_MID_NAME AS PREV_DEPT_MID_NAME
                                     FROM DEPT A
                                              LEFT JOIN DEPT B ON A.PREV_INDEX_CODE = B.INDEX_CODE)
                       SELECT *,
                              CASE
                                  WHEN PREV_DEPT_CODE IS NULL THEN 0
                                  WHEN PREV_DEPT_CODE = DEPT_CODE THEN 0
                                  ELSE 1 END AS SUM_FLAG
                       FROM DEPT_PREV';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_SEQ', V_QUERY_STR);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'begin', null, CURRENT_TIMESTAMP);

    --数据项和系数
    SET V_QUERY_STR = 'SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ITEM_BASE', V_QUERY_STR);


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


    SET V_QUERY_STR = 'SELECT DISTINCT *
                       FROM BG00MAC102.T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION
                       WHERE VERSION = ''' || V_FACTOR_VERSION || '''
                       AND LCI_ELEMENT_CODE = ''GWP-total''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_LIST', V_QUERY_STR);


    SET V_QUERY_STR = 'WITH STREAM AS (SELECT VERSION, UUID AS STREAM_ID,
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


    SET V_QUERY_STR = 'SELECT DATA_CODE AS ITEM_CODE,
                              LCI_ELEMENT_CODE,
                              LCI_ELEMENT_NAME,
                              LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM
                       WHERE VERSION = ''' || V_FACTOR_VERSION || '''
                            AND LCI_ELEMENT_CODE IN (SELECT LCI_ELEMENT_CODE
                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_EP', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.ITEM_CODE AS ITEM_CODE,
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
                       WHERE B.DISCH_COEFF IS NOT NULL';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_FUEL', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT *
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


    --主工序产品列表
    SET V_QUERY_STR = '
        SELECT DISTINCT MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PRODUCT_CODE
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'MAIN_PRODUCT', V_QUERY_STR);

    --能辅产品列表
    SET V_QUERY_STR = '
    SELECT DISTINCT PROC_KEY,
                    PRODUCT_CODE
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ENERGY_PRODUCT', V_QUERY_STR);

    --原材料数据
    SET V_QUERY_STR = 'SELECT A.*
                        FROM (SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                              WHERE TYPE_CODE IN (''01'', ''02'', ''03'')) A
                                 JOIN (SELECT *
                                             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ) B
                                            ON A.INDEX_CODE = B.INDEX_CODE
                                 JOIN (SELECT *
                                             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST) C
                                            ON B.PREV_INDEX_CODE = C.INDEX_CODE AND A.ITEM_CODE = C.PRODUCT_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'INPUT_MAIN', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''01'', ''02'', ''03'')
        AND ITEM_CODE IN (SELECT DISTINCT PRODUCT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_PRODUCT)
        EXCEPT
        (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN)
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'INPUT_ENERGY', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''01'', ''02'', ''03'')
        EXCEPT
        (
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_ENERGY
        UNION
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN)
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'INPUT_RESOURCE', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''01'', ''02'', ''03'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'INPUT_ALL', V_QUERY_STR);


    --OUTPUT数据
    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE = ''04''
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'OUTPUT_PRODUCT', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE = ''05''
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'OUTPUT_BY_PRODUCT', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE = ''08''
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'OUTPUT_WASTE', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''06'', ''07'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'OUTPUT_EMISSION', V_QUERY_STR);


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('current', 'begin', null, CURRENT_TIMESTAMP);

    --本工序结果

    SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              VALUE,
                              UNITM_AC,
                              UNIT_COST,
                              LCI_ELEMENT_CODE,
                              C1,
                              C2,
                              C3,
                              C4,
                              C5,
                              FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             0                                              AS C1,
                             COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C2,
                             0                                              AS C3,
                             0                                              AS C4,
                             0                                              AS C5,
                             ''RESOURCE''                                   AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_RESOURCE A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FCP B
                                    ON A.ITEM_CODE = B.ITEM_CODE
                      UNION
                      SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             0                                              AS C1,
                             0                                              AS C2,
                             COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C3,
                             0                                              AS C4,
                             0                                              AS C5,
                             ''RESOURCE''                                     AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_RESOURCE A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_SY B
                                    ON A.ITEM_CODE = B.ITEM_CODE
                      UNION
                      SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             0                                              AS C1,
                             0                                              AS C2,
                             0                                              AS C3,
                             0                                              AS C4,
                             COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C5,
                             ''RESOURCE''                                     AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_RESOURCE A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT B
                                    ON A.ITEM_CODE = B.ITEM_CODE)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_RESOURCE', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              VALUE,
                              UNITM_AC,
                              UNIT_COST,
                              LCI_ELEMENT_CODE,
                              C1,
                              C2,
                              C3,
                              C4,
                              C5,
                              FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             COALESCE(A.UNIT_COST * B.C1_DIRECT, 0) AS C1,
                             COALESCE(A.UNIT_COST * B.C2_BP, 0)     AS C2,
                             COALESCE(A.UNIT_COST * B.C3_OUT, 0)    AS C3,
                             COALESCE(A.UNIT_COST * B.C4_BP_NEG, 0) AS C4,
                             COALESCE(A.UNIT_COST * B.C5_TRANS, 0)  AS C5,
                             ''ENERGY''                               AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_ENERGY A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT B
                                    ON A.ITEM_CODE = B.PRODUCT_CODE)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_ENERGY', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              VALUE,
                              UNITM_AC,
                              UNIT_COST,
                              LCI_ELEMENT_CODE,
                              C1,
                              C2,
                              C3,
                              C4,
                              C5,
                              FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             0                                                    AS C1,
                             0                                                    AS C2,
                             0                                                    AS C3,
                             COALESCE(-ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C4,
                             0                                                    AS C5,
                             ''DISCOUNT''                                           AS FLAG
                      FROM (SELECT *
                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_BY_PRODUCT) A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT B
                                    ON A.ITEM_CODE = B.ITEM_CODE
                      UNION
                      SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             0                                                    AS C1,
                             0                                                    AS C2,
                             0                                                    AS C3,
                             COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0)  AS C4,
                             0                                                    AS C5,
                             ''DISCOUNT''                                         AS FLAG
                      FROM (SELECT *
                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_WASTE) A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT B
                                    ON A.ITEM_CODE = B.ITEM_CODE)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_DISCOUNT', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              VALUE,
                              UNITM_AC,
                              UNIT_COST,
                              LCI_ELEMENT_CODE,
                              C1,
                              C2,
                              C3,
                              C4,
                              C5,
                              FLAG
                       FROM (SELECT A.*,
                                    B.LCI_ELEMENT_CODE,
                                    COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C1,
                                    0                                              AS C2,
                                    0                                              AS C3,
                                    0                                              AS C4,
                                    0                                              AS C5,
                                    ''DISCH''                                      AS FLAG
                             FROM (SELECT *
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_ALL) A
                                      JOIN (SELECT *
                                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FUEL) B
                                           ON A.ITEM_CODE = B.ITEM_CODE
                             UNION
                             SELECT A.*,
                                    B.LCI_ELEMENT_CODE,
                                    COALESCE(-ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C1,
                                    0                                                    AS C2,
                                    0                                                    AS C3,
                                    0                                                    AS C4,
                                    0                                                    AS C5,
                                    ''DISCH''                                            AS FLAG
                             FROM (SELECT *
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_PRODUCT
                                   UNION
                                   SELECT *
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_BY_PRODUCT
                                   UNION
                                   SELECT *
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_WASTE) A
                                      JOIN (SELECT *
                                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FUEL) B
                                           ON A.ITEM_CODE = B.ITEM_CODE)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_FUEL', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              VALUE,
                              UNITM_AC,
                              UNIT_COST,
                              LCI_ELEMENT_CODE,
                              C1,
                              C2,
                              C3,
                              C4,
                              C5,
                              FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_CODE,
                             COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C1,
                             0                                                   AS C2,
                             0                                                   AS C3,
                             0                                                   AS C4,
                             0                                                   AS C5,
                             ''EP''                                              AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                               JOIN (SELECT *
                                     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_EP) B
                                    ON A.ITEM_CODE = B.ITEM_CODE)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_EP', V_QUERY_STR);


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('current', 'end', null, CURRENT_TIMESTAMP);

    --遍历结转工序

    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || ' WHERE
                       SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || '''
                       AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''
                       AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_PARALLEL WHERE
                       SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || '''
                       AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''
                       AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'SELECT DISTINCT MAX(RANK) AS MAX_RANK
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    SET V_RANK = 1;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('seq', cast(V_MAX_RANK as VARCHAR(10)), '0', CURRENT_TIMESTAMP);

    WHILE V_RANK <= V_MAX_RANK
        DO
            DELETE FROM SESSION.RANK_PROC;
            SET V_QUERY_STR = 'INSERT INTO SESSION.RANK_PROC
            SELECT INDEX_CODE,
                   PREV_INDEX_CODE,
                   SUM_FLAG
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ
            WHERE RANK = ''' || V_RANK || '''';
            PREPARE STMT FROM V_QUERY_STR;
            EXECUTE STMT;

            DELETE FROM SESSION.DIST_BUFFER;
            SET V_QUERY_STR = 'INSERT INTO SESSION.DIST_BUFFER
            SELECT UPDATE_DATE,
                   INDEX_CODE,
                   MAT_NO,
                   MAT_TRACK_NO,
                   MAT_SEQ_NO,
                   FAMILY_CODE,
                   UNIT_CODE,
                   UNIT_NAME,
                   PRODUCT_CODE,
                   PRODUCT_NAME,
                   PRODUCT_VALUE,
                   MAT_WT,
                   MAT_STATUS,
                   TYPE_CODE,
                   TYPE_NAME,
                   ITEM_CODE,
                   ITEM_NAME,
                   VALUE,
                   UNITM_AC,
                   UNIT_COST,
                   LCI_ELEMENT_CODE,
                   SUM(C1)   AS C1,
                   SUM(C2)   AS C2,
                   SUM(C3)   AS C3,
                   SUM(C4)   AS C4,
                   SUM(C5)   AS C5,
                   NULL      AS FLAG
            FROM ((SELECT A.*
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_RESOURCE A
                            JOIN SESSION.RANK_PROC B ON A.INDEX_CODE = B.INDEX_CODE)
                  UNION
                  (SELECT A.*
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_ENERGY A
                            JOIN SESSION.RANK_PROC B ON A.INDEX_CODE = B.INDEX_CODE)
                  UNION
                  (SELECT A.*
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_DISCOUNT A
                            JOIN SESSION.RANK_PROC B ON A.INDEX_CODE = B.INDEX_CODE)
                  UNION
                  (SELECT A.*
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_FUEL A
                            JOIN SESSION.RANK_PROC B ON A.INDEX_CODE = B.INDEX_CODE)
                  UNION
                  (SELECT A.*
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_EP A
                            JOIN SESSION.RANK_PROC B ON A.INDEX_CODE = B.INDEX_CODE))
            GROUP BY UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                     PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, MAT_WT, MAT_STATUS, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME,
                     VALUE, UNITM_AC, UNIT_COST, LCI_ELEMENT_CODE';
            PREPARE STMT FROM V_QUERY_STR;
            EXECUTE STMT;

            IF V_START_MONTH IS NOT NULL THEN
                SET V_QUERY_STR = 'INSERT INTO SESSION.RESULT_BUFFER
                                   SELECT A.*
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PREV_RESULT A
                                            JOIN SESSION.RANK_PROC B
                                                 ON A.INDEX_CODE = B.PREV_INDEX_CODE';
                PREPARE STMT FROM V_QUERY_STR;
                EXECUTE STMT;
            END IF;

            SET V_QUERY_STR = 'INSERT INTO SESSION.DIST_BUFFER
                                   SELECT im.UPDATE_DATE,
                                          im.INDEX_CODE,
                                          im.MAT_NO,
                                          im.MAT_TRACK_NO,
                                          im.MAT_SEQ_NO,
                                          im.FAMILY_CODE,
                                          im.UNIT_CODE,
                                          im.UNIT_NAME,
                                          im.PRODUCT_CODE,
                                          im.PRODUCT_NAME,
                                          im.PRODUCT_VALUE,
                                          im.MAT_WT,
                                          im.MAT_STATUS,
                                          dist.TYPE_CODE,
                                          dist.TYPE_NAME,
                                          dist.ITEM_CODE,
                                          dist.ITEM_NAME,
                                          im.VALUE * dist.UNIT_COST AS VALUE,
                                          dist.UNITM_AC,
                                          im.UNIT_COST * dist.UNIT_COST AS UNIT_COST,
                                          dist.LCI_ELEMENT_CODE,
                                          im.UNIT_COST * dist.C1 AS C1,
                                          im.UNIT_COST * dist.C2 AS C2,
                                          im.UNIT_COST * dist.C3 AS C3,
                                          im.UNIT_COST * dist.C4 AS C4,
                                          im.UNIT_COST * dist.C5 AS C5,
                                          dist.FLAG
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN im
                                            JOIN (SELECT * FROM SESSION.RANK_PROC WHERE SUM_FLAG = 0) rp
                                                 ON im.INDEX_CODE = rp.INDEX_CODE
                                            JOIN SESSION.RESULT_BUFFER dist
                                                 ON rp.PREV_INDEX_CODE = dist.INDEX_CODE';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            SET V_QUERY_STR = 'INSERT INTO SESSION.DIST_BUFFER
                                   SELECT im.UPDATE_DATE,
                                          im.INDEX_CODE,
                                          im.MAT_NO,
                                          im.MAT_TRACK_NO,
                                          im.MAT_SEQ_NO,
                                          im.FAMILY_CODE,
                                          im.UNIT_CODE,
                                          im.UNIT_NAME,
                                          im.PRODUCT_CODE,
                                          im.PRODUCT_NAME,
                                          im.PRODUCT_VALUE,
                                          im.MAT_WT,
                                          im.MAT_STATUS,
                                          im.TYPE_CODE,
                                          im.TYPE_NAME,
                                          im.ITEM_CODE,
                                          im.ITEM_NAME,
                                          im.VALUE,
                                          im.UNITM_AC,
                                          im.UNIT_COST,
                                          dist.LCI_ELEMENT_CODE,
                                          SUM(im.UNIT_COST * dist.C1) AS C1,
                                          SUM(im.UNIT_COST * dist.C2) AS C2,
                                          SUM(im.UNIT_COST * dist.C3) AS C3,
                                          SUM(im.UNIT_COST * dist.C4) AS C4,
                                          SUM(im.UNIT_COST * dist.C5) AS C5,
                                          ''SUM''                    AS FLAG
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN im
                                            JOIN (SELECT * FROM SESSION.RANK_PROC WHERE SUM_FLAG = 1) rp
                                                 ON im.INDEX_CODE = rp.INDEX_CODE
                                            JOIN SESSION.RESULT_BUFFER dist
                                                 ON rp.PREV_INDEX_CODE = dist.INDEX_CODE
                                   GROUP BY im.UPDATE_DATE,
                                            im.INDEX_CODE,
                                            im.MAT_NO,
                                            im.MAT_TRACK_NO,
                                            im.MAT_SEQ_NO,
                                            im.FAMILY_CODE,
                                            im.UNIT_CODE,
                                            im.UNIT_NAME,
                                            im.PRODUCT_CODE,
                                            im.PRODUCT_NAME,
                                            im.PRODUCT_VALUE,
                                            im.MAT_WT,
                                            im.MAT_STATUS,
                                            im.TYPE_CODE,
                                            im.TYPE_NAME,
                                            im.ITEM_CODE,
                                            im.ITEM_NAME,
                                            im.VALUE,
                                            im.UNITM_AC,
                                            im.UNIT_COST,
                                            dist.LCI_ELEMENT_CODE';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            DELETE FROM SESSION.RESULT_BUFFER;

            INSERT INTO SESSION.RESULT_BUFFER (UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE,
                                               UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, MAT_WT,
                                               MAT_STATUS, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC,
                                               UNIT_COST, LCI_ELEMENT_CODE, C1, C2, C3, C4, C5, FLAG)
            SELECT UPDATE_DATE,
                   INDEX_CODE,
                   MAT_NO,
                   MAT_TRACK_NO,
                   MAT_SEQ_NO,
                   FAMILY_CODE,
                   UNIT_CODE,
                   UNIT_NAME,
                   PRODUCT_CODE,
                   PRODUCT_NAME,
                   PRODUCT_VALUE,
                   MAT_WT,
                   MAT_STATUS,
                   TYPE_CODE,
                   TYPE_NAME,
                   ITEM_CODE,
                   ITEM_NAME,
                   SUM(VALUE),
                   UNITM_AC,
                   SUM(UNIT_COST),
                   LCI_ELEMENT_CODE,
                   SUM(C1),
                   SUM(C2),
                   SUM(C3),
                   SUM(C4),
                   SUM(C5),
                   FLAG
            FROM SESSION.DIST_BUFFER
            GROUP BY UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                     PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, MAT_WT, MAT_STATUS, TYPE_CODE, TYPE_NAME, ITEM_CODE,
                     ITEM_NAME, UNITM_AC,
                     LCI_ELEMENT_CODE, FLAG;


            SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || ' (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, MAIN_CAT_BATCH_NUMBER,
                                                                             FACTOR_VERSION, UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE,
                                                                             UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, MAT_WT, MAT_STATUS,
                                                                             DEPT_NAME, DEPT_CODE, DEPT_MID_NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME,
                                                                             C1, C2, C3, C4, C5, C_INSITE, C_OUTSITE, C_CYCLE, REC_CREATE_TIME)
                       SELECT HEX(RAND()),
                              ''' || V_SUBCLASS_TAB_NAME || ''',
                              ''' || V_COMPANY_CODE || ''',
                              ''' || V_MAIN_CAT_BATCH_NUMBER || ''',
                              ''' || V_FACTOR_VERSION || ''',
                              UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              DEPT_NAME,
                              DEPT_CODE,
                              DEPT_MID_NAME,
                              LCI_ELEMENT_CODE,
                              LCI_ELEMENT_CNAME,
                              SUM(C1),
                              SUM(C2),
                              SUM(C3),
                              SUM(C4),
                              SUM(C5),
                              SUM(C1) + SUM(C2),
                              SUM(C3) + SUM(C4) + SUM(C5),
                              SUM(C1) + SUM(C2) + SUM(C3) + SUM(C4) + SUM(C5),
                              TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')
                       FROM (SELECT A.*, B.LCI_ELEMENT_CNAME, C.DEPT_NAME, C.DEPT_CODE, C.DEPT_MID_NAME
                              FROM SESSION.RESULT_BUFFER A
                                       LEFT JOIN (SELECT LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME
                                                  FROM BG00MAC102.T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION WHERE VERSION = ''' ||
                              V_FACTOR_VERSION || ''') B
                                                 ON A.LCI_ELEMENT_CODE = B.LCI_ELEMENT_CODE
                                       LEFT JOIN (SELECT *
                                                  FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022
                                                  WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || ''') C
                                          ON A.UNIT_CODE = C.UNIT_CODE)
                       GROUP BY UPDATE_DATE,
                                INDEX_CODE,
                                MAT_NO,
                                MAT_TRACK_NO,
                                MAT_SEQ_NO,
                                FAMILY_CODE,
                                UNIT_CODE,
                                UNIT_NAME,
                                PRODUCT_CODE,
                                PRODUCT_NAME,
                                PRODUCT_VALUE,
                                MAT_WT,
                                MAT_STATUS,
                                DEPT_NAME,
                                DEPT_CODE,
                                DEPT_MID_NAME,
                                LCI_ELEMENT_CODE,
                                LCI_ELEMENT_CNAME
                       HAVING 1 = 1';
            IF V_START_MONTH IS NOT NULL THEN
                SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
            END IF;
            IF V_END_MONTH IS NOT NULL THEN
                SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
            END IF;
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_PARALLEL (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, MAIN_CAT_BATCH_NUMBER,
                                                                             FACTOR_VERSION, UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE,
                                                                             UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, MAT_WT, MAT_STATUS,
                                                                             TYPE_CODE, TYPE_NAME,
                                                                             ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC, UNIT_COST, DEPT_NAME, DEPT_CODE, DEPT_MID_NAME,
                                                                             LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME, C1, C2, C3, C4, C5, C_INSITE, C_OUTSITE, C_CYCLE, FLAG,
                                                                             REC_CREATE_TIME)
                       SELECT HEX(RAND()),
                              ''' || V_SUBCLASS_TAB_NAME || ''',
                              ''' || V_COMPANY_CODE || ''',
                              ''' || V_MAIN_CAT_BATCH_NUMBER || ''',
                              ''' || V_FACTOR_VERSION || ''',
                              UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              MAT_WT,
                              MAT_STATUS,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              VALUE,
                              UNITM_AC,
                              UNIT_COST,
                              DEPT_NAME,
                              DEPT_CODE,
                              DEPT_MID_NAME,
                              LCI_ELEMENT_CODE,
                              LCI_ELEMENT_CNAME,
                              C1,
                              C2,
                              C3,
                              C4,
                              C5,
                              C1 + C2,
                              C3 + C4 + C5,
                              C1 + C2 + C3 + C4 + C5,
                              FLAG,
                              TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')
                       FROM (SELECT A.*, B.LCI_ELEMENT_CNAME, C.DEPT_NAME, C.DEPT_CODE, C.DEPT_MID_NAME
                              FROM SESSION.RESULT_BUFFER A
                                       LEFT JOIN (SELECT LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME
                                                  FROM BG00MAC102.T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION WHERE VERSION = ''' ||
                              V_FACTOR_VERSION || ''') B
                                                 ON A.LCI_ELEMENT_CODE = B.LCI_ELEMENT_CODE
                                       LEFT JOIN (SELECT *
                                                  FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022
                                                  WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || ''') C
                                          ON A.UNIT_CODE = C.UNIT_CODE)
                       WHERE 1 = 1';
            IF V_START_MONTH IS NOT NULL THEN
                SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
            END IF;
            IF V_END_MONTH IS NOT NULL THEN
                SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
            END IF;
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_NORM_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
            VALUES ('seq', cast(V_MAX_RANK as VARCHAR(10)), cast(V_RANK as VARCHAR(10)), CURRENT_TIMESTAMP);

            SET V_RANK = V_RANK + 1;

        END WHILE;

    EXECUTE IMMEDIATE 'DROP TABLE SESSION.RANK_PROC';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.DIST_BUFFER';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.RESULT_BUFFER';

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

