CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_EPD_CONS_CALC_MONTH_RESOURCE(IN V_COMPANY_CODE VARCHAR(4),
                                                                                            IN V_START_MONTH VARCHAR(6),
                                                                                            IN V_END_MONTH VARCHAR(6),
                                                                                            IN V_FACTOR_YEAR VARCHAR(4),
                                                                                            IN V_MAIN_CAT_RESULT_TAB_NAME VARCHAR(100),
                                                                                            IN V_MAIN_CAT_BATCH_NUMBER VARCHAR(100),
                                                                                            IN V_SUBCLASS_TAB_NAME VARCHAR(100),
                                                                                            IN V_SUBCLASS_RESULT_TAB_NAME VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_EPD_CONS_CALC_MONTH_RESOURCE
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

    DECLARE V_START_TIME TIMESTAMP;
    DECLARE V_END_TIME TIMESTAMP;

    DECLARE V_LOG_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --日志表所在SCHEMA
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_EPD_CONS_CALC_MONTH_RESOURCE'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);
    DECLARE MESSAGE_TEXT VARCHAR(2048);

    ------------------------------------日志变量定义------------------------------------
    DECLARE TAR_SCHEMA1 VARCHAR(32) DEFAULT 'BG00MAC102'; --目标表SCHEMA
    DECLARE SRC_TAB_NAME1 VARCHAR(32) DEFAULT ' '; --源表SCHEMA.表名
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(100) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_EPD_CONS_CALC_MONTH_RESOURCE'; --临时表名
    DECLARE V_PNAME VARCHAR(100) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_EPD_CONS_CALC_MONTH_RESOURCE'; --存储过程名
    DECLARE V_QUERY_STR CLOB(1 M); --查询SQL
    DECLARE V_TMP_NAME VARCHAR(128);
    --完整的临时表名
    ------------------------------------存储过程变量定义---------------------------------

    DECLARE QUR_YEAR VARCHAR(4);
    DECLARE V_ROW_COUNT INT;
    DECLARE V_RANK INT DEFAULT 1;
    DECLARE V_MAX_RANK INT;
    DECLARE V_SUM_FLAG INT;

    --     DECLARE V_MAT_TRACK_NO VARCHAR(64);
--     DECLARE V_FAMILY_CODE VARCHAR(64);
--     DECLARE V_UNIT_CODE VARCHAR(64);
--     DECLARE V_PREV_FAMILY_CODE VARCHAR(64);
--     DECLARE V_MAT_SEQ_NO VARCHAR(64);

--     DECLARE DONE INT DEFAULT 0;
--     DECLARE CURSOR1 CURSOR FOR S1;
--     DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONE = 1;

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

    IF V_COMPANY_CODE != 'TA' THEN
        RETURN;
    END IF;

    TRUNCATE TABLE BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG IMMEDIATE;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'begin', null, CURRENT_TIMESTAMP);

    --取大类结果
    SET V_QUERY_STR = 'SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_RESULT_TAB_NAME || ' WHERE ' ||
                      'BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || ''' AND ' ||
                      'COMPANY_CODE = ''' || V_COMPANY_CODE || '''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ENERGY_RESULT', V_QUERY_STR);

    --取活动数据
    SET V_QUERY_STR = 'SELECT UPDATE_DATE, ' ||
                      'MAT_TRACK_NO, ' ||
                      'MAT_NO, ' ||
                      'MAT_SEQ_NO, ' ||
                      'CASE WHEN FAMILY_CODE IS NULL THEN ''00'' ' ||
                      'WHEN FAMILY_CODE = '''' THEN ''00'' ' ||
                      'ELSE FAMILY_CODE END AS FAMILY_CODE, ' ||
                      'UNIT_CODE, ' ||
                      'UNIT_NAME, ' ||
                      'TYPE_CODE, ' ||
                      'TYPE_NAME, ' ||
                      'ITEM_CODE, ' ||
                      'ITEM_NAME, ' ||
                      'CASE WHEN UNITM_AC = ''万度'' THEN VALUE * 10000 ' ||
                      'WHEN UNITM_AC = ''吨'' THEN VALUE * 1000 ' ||
                      'WHEN UNITM_AC = ''千立方米'' THEN VALUE * 1000 ' ||
                      'ELSE VALUE END AS VALUE, ' ||
                      'CASE WHEN UNITM_AC = ''万度'' THEN ''度'' ' ||
                      'WHEN UNITM_AC = ''吨'' THEN ''千克'' ' ||
                      'WHEN UNITM_AC = ''公斤'' THEN ''千克'' ' ||
                      'WHEN UNITM_AC = ''千立方米'' THEN ''立方米'' ' ||
                      'ELSE UNITM_AC END AS UNITM_AC ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || ' WHERE ' ||
                      'VALUE != 0 ' ||
                      'AND MAT_TRACK_NO NOT IN (' ||
                      'select DISTINCT MAT_TRACK_NO
                      from (select MAT_TRACK_NO, max(length(family_code) / 2) as max_rank
                            from ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                            group by MAT_TRACK_NO)
                      where max_rank > 18
                      union
                      select DISTINCT MAT_TRACK_NO
                      from (select MAT_TRACK_NO, max(FAMILY_CODE) as max_no
                            from ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                            group by MAT_TRACK_NO)
                      where MAX_NO is null) ';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);

    SET V_QUERY_STR = 'UPDATE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                        SET VALUE = VALUE * 0.9877
                        WHERE UNIT_CODE LIKE ''%BOF''
                          AND TYPE_CODE != ''04''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'SELECT DISTINCT MAT_TRACK_NO ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'MAT_TRACK_NO', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT A.* FROM (SELECT UPDATE_DATE,
               MAT_TRACK_NO,
               MAT_NO,
               MAT_SEQ_NO,
               CASE
                   WHEN FAMILY_CODE IS NULL THEN ''00''
                   WHEN FAMILY_CODE = '''' THEN ''00''
                   ELSE FAMILY_CODE
                   END
                    AS FAMILY_CODE,
               UNIT_CODE,
               UNIT_NAME,
               TYPE_CODE,
               TYPE_NAME,
               ITEM_CODE,
               ITEM_NAME,
               CASE
                   WHEN UNITM_AC = ''万度'' THEN VALUE * 10000
                   WHEN UNITM_AC = ''吨'' THEN VALUE * 1000
                   WHEN UNITM_AC = ''千立方米'' THEN VALUE * 1000
                   ELSE VALUE
                   END
                    AS VALUE,
               CASE
                   WHEN UNITM_AC = ''万度'' THEN ''度''
                   WHEN UNITM_AC = ''吨'' THEN ''千克''
                   WHEN UNITM_AC = ''公斤'' THEN ''千克''
                   WHEN UNITM_AC = ''千立方米'' THEN ''立方米''
                   ELSE UNITM_AC
                   END
                     AS UNITM_AC
        FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
        WHERE 1 = 1';
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    SET V_QUERY_STR = V_QUERY_STR || ' AND VALUE != 0) A JOIN (
        SELECT DISTINCT MAT_TRACK_NO
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PREV_TEMP_DATA', V_QUERY_STR);


    --PROC信息
    SET V_QUERY_STR = '
         SELECT DISTINCT
            UPDATE_DATE,
            MAT_TRACK_NO,
            FAMILY_CODE,
            UNIT_CODE,
            UNIT_NAME,
            ITEM_CODE AS PRODUCT_CODE,
            ITEM_NAME AS PRODUCT_NAME,
            VALUE AS PRODUCT_VALUE
        FROM
            ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PREV_TEMP_DATA
        WHERE TYPE_CODE = ''04''
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_PRODUCT_LIST', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT ''' || V_SUBCLASS_TAB_NAME || ''' AS SUBCLASS_TAB_NAME,
               A.UPDATE_DATE,
               MAT_NO,
               A.MAT_TRACK_NO,
               MAT_SEQ_NO,
               A.FAMILY_CODE,
               A.UNIT_CODE,
               A.UNIT_NAME,
               PRODUCT_CODE,
               PRODUCT_NAME,
               TYPE_CODE,
               TYPE_NAME,
               ITEM_CODE,
               ITEM_NAME,
               ABS(VALUE) AS VALUE,
               UNITM_AC,
               PRODUCT_VALUE,
               CAST(ABS(VALUE) AS DOUBLE) / CAST(PRODUCT_VALUE AS DOUBLE) AS UNIT_COST
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PREV_TEMP_DATA A
               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
               ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                AND A.FAMILY_CODE = B.FAMILY_CODE
                AND A.UNIT_CODE = B.UNIT_CODE
                AND A.UPDATE_DATE = B.UPDATE_DATE
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PREV_DATA', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT ''' || V_SUBCLASS_TAB_NAME || ''' AS SUBCLASS_TAB_NAME,
               A.UPDATE_DATE,
               MAT_NO,
               A.MAT_TRACK_NO,
               MAT_SEQ_NO,
               A.FAMILY_CODE,
               A.UNIT_CODE,
               A.UNIT_NAME,
               PRODUCT_CODE,
               PRODUCT_NAME,
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
                AND A.UNIT_CODE = B.UNIT_CODE
                AND A.UPDATE_DATE = B.UPDATE_DATE
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DATA', V_QUERY_STR);

    --工艺路径
    SET V_QUERY_STR = '
         SELECT DISTINCT UPDATE_DATE, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
           CASE
           WHEN FAMILY_CODE = ''00'' OR FAMILY_CODE = ''01'' THEN ''00''
           ELSE LEFT(FAMILY_CODE, LENGTH(FAMILY_CODE) - 2)
           END AS PREV_FAMILY_CODE
         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PREV_DATA
         ORDER BY FAMILY_CODE, MAT_SEQ_NO
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_SEQ', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT *,
               CASE
                   WHEN PREV_DEPT_CODE IS NULL THEN 0
                   WHEN PREV_DEPT_CODE = DEPT_CODE THEN 0
                   ELSE 1 END AS SUM_FLAG
        FROM (SELECT *,
                     LAG(UNIT_CODE, 1) OVER (PARTITION BY MAT_TRACK_NO ORDER BY RANK) AS PREV_UNIT_CODE,
                     LAG(DEPT_CODE, 1) OVER (PARTITION BY MAT_TRACK_NO ORDER BY RANK) AS PREV_DEPT_CODE
              FROM (SELECT *,
                           RANK() OVER (PARTITION BY MAT_TRACK_NO ORDER BY FAMILY_CODE, MAT_SEQ_NO) AS RANK
                    FROM (SELECT A.*,
                                 B.DEPT_CODE,
                                 B.DEPT_NAME,
                                 B.DEPT_MID_NAME
                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ A
                                   LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' ||
                      V_COMPANY_CODE || ''') B
                                        ON A.UNIT_CODE = B.UNIT_CODE)))
        WHERE PREV_FAMILY_CODE = ''00''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RANK_PREV', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT *,
               CASE
                   WHEN PREV_DEPT_CODE IS NULL THEN 0
                   WHEN PREV_DEPT_CODE = DEPT_CODE THEN 0
                   ELSE 1 END AS SUM_FLAG
        FROM (SELECT A.*,
                     CAST(LENGTH(A.PREV_FAMILY_CODE) / 2 AS BIGINT) AS RANK,
                     B.UNIT_CODE                                    AS PREV_UNIT_CODE,
                     C.DEPT_CODE                                    AS DEPT_CODE,
                     D.DEPT_CODE                                    AS PREV_DEPT_CODE
              FROM (SELECT *
                    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ
                    WHERE PREV_FAMILY_CODE != ''00'') A
                       LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ B
                                 ON A.PREV_FAMILY_CODE = B.FAMILY_CODE and A.MAT_TRACK_NO = B.MAT_TRACK_NO
                       LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' ||
                      V_COMPANY_CODE || ''') C
                                 ON A.UNIT_CODE = C.UNIT_CODE
                       LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' ||
                      V_COMPANY_CODE || ''') D
                                 ON B.UNIT_CODE = D.UNIT_CODE
              ORDER BY A.MAT_TRACK_NO, A.FAMILY_CODE, A.MAT_SEQ_NO)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RANK_POST', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT A.*,
               B.UNIT_CODE            AS PREV_UNIT_CODE,
               B.UNIT_NAME            AS PREV_UNIT_NAME,
               B.PRODUCT_CODE         AS PREV_PRODUCT_CODE,
               B.PRODUCT_NAME         AS PREV_PRODUCT_NAME,
               B.PRODUCT_VALUE        AS PREV_PRODUCT_VALUE
        FROM (SELECT *
              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ
              WHERE NOT (FAMILY_CODE = ''00'' AND MAT_SEQ_NO = 1)) A
                 JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
                      ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                          AND A.PREV_FAMILY_CODE = B.FAMILY_CODE
                          AND NOT (A.FAMILY_CODE = B.FAMILY_CODE AND A.UNIT_CODE = B.UNIT_CODE)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PREV_ITEM', V_QUERY_STR);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'begin', null, CURRENT_TIMESTAMP);

    --数据项和系数
    SET V_QUERY_STR = 'SELECT DISTINCT ITEM_CODE, ITEM_NAME
         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA';
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


    SET V_QUERY_STR = '
        WITH STREAM AS (SELECT UUID AS STREAM_ID,
                       NAME AS STREAM_NAME,
                       LCI_ELEMENT_ID,
                       LCI_ELEMENT_NAME,
                       LCI_ELEMENT_VALUE
                FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                WHERE VERSION = ''易碳''
                  AND FLAG = ''STREAM'')
        SELECT DISTINCT A.DATA_CODE,
                        A.UUID AS STREAM_ID,
                        B.STREAM_NAME,
                        B.LCI_ELEMENT_ID,
                        B.LCI_ELEMENT_NAME,
                        B.LCI_ELEMENT_VALUE
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI A
                 LEFT JOIN STREAM B ON A.UUID = B.STREAM_ID';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_STREAM', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT CONS_LCI_ELEMENT_NAME AS LCI_ELEMENT_NAME
                        FROM ' || V_TMP_SCHEMA || '.T_ADS_FACT_LCA_EPD_LCI_ELEMENT_NORM_CONS_MAP
                        WHERE CONS_LCI_ELEMENT_NAME IN (''hazardous waste disposed'',
                                                        ''non-hazardous waste disposed'',
                                                        ''radioactive waste disposed'',
                                                        ''total use of non-renewable primary energy resources (primary energy and primary energy resources used as raw materials)'',
                                                        ''total use of renewable primary energy resources (primary energy and primary energy resources used as raw materials)'',
                                                        ''use of net fresh water'',
                                                        ''use of non-renewable primary energy excluding non-renewable primary energy resources used as raw materials'',
                                                        ''use of non-renewable primary energy resources used as raw materials'',
                                                        ''use of non-renewable secondary fuels'',
                                                        ''use of renewable primary energy excluding renewable primary energy resources used as raw materials'',
                                                        ''use of renewable primary energy resources used as raw materials'',
                                                        ''use of renewable secondary fuels'',
                                                        ''use of secondary material'')';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_LIST', V_QUERY_STR);


    SET V_QUERY_STR = '
            SELECT DATA_CODE AS ITEM_CODE,
                   LCI_ELEMENT_ID,
                   LCI_ELEMENT_NAME,
                   ABS(LCI_ELEMENT_VALUE) * 1000 AS LCI_ELEMENT_VALUE
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM
            WHERE LCI_ELEMENT_NAME IN (''use of secondary material'', ''hazardous waste disposed'',
                   ''non-hazardous waste disposed'', ''radioactive waste disposed'')
            UNION
            SELECT DATA_CODE AS ITEM_CODE,
                   LCI_ELEMENT_ID,
                   LCI_ELEMENT_NAME,
                   LCI_ELEMENT_VALUE
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM
            WHERE LCI_ELEMENT_NAME = ''use of net fresh water''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_EP', V_QUERY_STR);


    SET V_QUERY_STR = '
            SELECT A.ITEM_CODE AS ITEM_CODE,
                   NULL AS LCI_ELEMENT_ID,
                   ''total use of non-renewable primary energy resources (primary energy and primary energy resources used as raw materials)'' AS LCI_ELEMENT_NAME,
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
                   NULL AS LCI_ELEMENT_ID,
                   ''use of non-renewable primary energy excluding non-renewable primary energy resources used as raw materials''       AS LCI_ELEMENT_NAME,
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
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                           WHERE VERSION = ''易碳''
                             AND LCI_ELEMENT_NAME IN (SELECT LCI_ELEMENT_NAME
                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST)
                             AND NAME IN (''海运'', ''河运'', ''铁运'', ''汽运'')),
     ALL_COMBINED AS (SELECT F.ITEM_CODE,
                             F.ITEM_NAME,
                             T.LCI_ELEMENT_ID,
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
                   LCI_ELEMENT_ID,
                   LCI_ELEMENT_NAME,
                   SUM(LCI_ELEMENT_VALUE) AS LCI_ELEMENT_VALUE
            FROM ALL_COMBINED
            GROUP BY ITEM_CODE, ITEM_NAME, LCI_ELEMENT_ID, LCI_ELEMENT_NAME)
      WHERE LCI_ELEMENT_VALUE != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_TRANSPORT', V_QUERY_STR);


    SET V_QUERY_STR = '
    WITH MAT AS (SELECT A.DATA_CODE,
                    A.FLAG,
                    B.NAME,
                    B.LCI_ELEMENT_ID,
                    B.LCI_ELEMENT_NAME,
                    B.LCI_ELEMENT_VALUE
             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_UUID A
                      JOIN (SELECT *
                            FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS
                            WHERE VERSION = ''易碳''
                              AND FLAG = ''MAT''
                              AND LCI_ELEMENT_NAME IN
                                  (SELECT LCI_ELEMENT_NAME
                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST)) B
                           ON A.UUID = B.UUID)
    SELECT DISTINCT A.ITEM_CODE,
                    A.ITEM_NAME,
                    B.FLAG,
                    B.NAME AS MAT_NAME,
                    B.LCI_ELEMENT_ID,
                    B.LCI_ELEMENT_NAME,
                    B.LCI_ELEMENT_VALUE
    FROM (SELECT ITEM_CODE, ITEM_NAME
          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE) A
         JOIN
         (SELECT * FROM MAT) B
         ON A.ITEM_CODE = B.DATA_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_MAT', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT ITEM_CODE, LCI_ELEMENT_ID, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT
    WHERE FLAG = ''FCP''
      AND LCI_ELEMENT_VALUE != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_FCP', V_QUERY_STR);


    SET V_QUERY_STR = '
    SELECT ITEM_CODE, LCI_ELEMENT_ID, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE
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
    SET V_QUERY_STR = '
        SELECT A.*
        FROM (SELECT *
              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
              WHERE TYPE_CODE IN (''01'', ''02'', ''03'')) A
                 INNER JOIN (SELECT *
                             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PREV_ITEM) B
                            ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                AND A.UNIT_CODE = B.UNIT_CODE AND A.FAMILY_CODE = B.FAMILY_CODE AND
                               A.MAT_SEQ_NO = B.MAT_SEQ_NO AND A.ITEM_CODE = B.PREV_PRODUCT_CODE
    ';
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


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('current', 'begin', null, CURRENT_TIMESTAMP);

    --本工序结果

    SET V_QUERY_STR = 'SELECT SUBCLASS_TAB_NAME,
                       UPDATE_DATE,
                       MAT_NO,
                       MAT_TRACK_NO,
                       MAT_SEQ_NO,
                       FAMILY_CODE,
                       UNIT_CODE,
                       UNIT_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       PRODUCT_VALUE,
                       LCI_ELEMENT_NAME,
                       SUM(C1) AS C1,
                       SUM(C2) AS C2,
                       SUM(C3) AS C3,
                       SUM(C4) AS C4,
                       SUM(C5) AS C5,
                       FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_NAME,
                             0                                              AS C1,
                             COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C2,
                             0                                              AS C3,
                             0                                              AS C4,
                             0                                              AS C5,
                             ''RESOURCE''                                     AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_RESOURCE A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FCP B
                                    ON A.ITEM_CODE = B.ITEM_CODE
                      UNION
                      SELECT A.*,
                             B.LCI_ELEMENT_NAME,
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
                             B.LCI_ELEMENT_NAME,
                             0                                              AS C1,
                             0                                              AS C2,
                             0                                              AS C3,
                             0                                              AS C4,
                             COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C5,
                             ''RESOURCE''                                     AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_RESOURCE A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT B
                                    ON A.ITEM_CODE = B.ITEM_CODE)
                GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                         PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, LCI_ELEMENT_NAME, FLAG';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_RESOURCE', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT SUBCLASS_TAB_NAME,
                       UPDATE_DATE,
                       MAT_NO,
                       MAT_TRACK_NO,
                       MAT_SEQ_NO,
                       FAMILY_CODE,
                       UNIT_CODE,
                       UNIT_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       PRODUCT_VALUE,
                       LCI_ELEMENT_NAME,
                       SUM(C1) AS C1,
                       SUM(C2) AS C2,
                       SUM(C3) AS C3,
                       SUM(C4) AS C4,
                       SUM(C5) AS C5,
                       FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_NAME,
                             COALESCE(A.UNIT_COST * B.C1_DIRECT, 0) AS C1,
                             COALESCE(A.UNIT_COST * B.C2_BP, 0)     AS C2,
                             COALESCE(A.UNIT_COST * B.C3_OUT, 0)    AS C3,
                             COALESCE(A.UNIT_COST * B.C4_BP_NEG, 0) AS C4,
                             COALESCE(A.UNIT_COST * B.C5_TRANS, 0)  AS C5,
                             ''ENERGY''                               AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_ENERGY A
                               JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT
                                  WHERE LCI_ELEMENT_NAME IN
                                  (SELECT LCI_ELEMENT_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST)) B
                                    ON A.ITEM_CODE = B.PRODUCT_CODE)
                GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                         PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, LCI_ELEMENT_NAME, FLAG';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_ENERGY', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT SUBCLASS_TAB_NAME,
                       UPDATE_DATE,
                       MAT_NO,
                       MAT_TRACK_NO,
                       MAT_SEQ_NO,
                       FAMILY_CODE,
                       UNIT_CODE,
                       UNIT_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       PRODUCT_VALUE,
                       LCI_ELEMENT_NAME,
                       SUM(C1) AS C1,
                       SUM(C2) AS C2,
                       SUM(C3) AS C3,
                       SUM(C4) AS C4,
                       SUM(C5) AS C5,
                       FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_NAME,
                             0                                                    AS C1,
                             0                                                    AS C2,
                             0                                                    AS C3,
                             COALESCE(-ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C4,
                             0                                                    AS C5,
                             ''DISCOUNT''                                           AS FLAG
                      FROM (SELECT *
                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_BY_PRODUCT
                            WHERE UNIT_CODE NOT LIKE ''%BOF'') A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT B
                                    ON A.ITEM_CODE = B.ITEM_CODE
                      UNION
                      SELECT A.*,
                             B.LCI_ELEMENT_NAME,
                             0                                                    AS C1,
                             0                                                    AS C2,
                             0                                                    AS C3,
                             COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0)  AS C4,
                             0                                                    AS C5,
                             ''DISCOUNT''                                         AS FLAG
                      FROM (SELECT *
                            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_OUTPUT_WASTE) A
                               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_MAT B
                                    ON A.ITEM_CODE = B.ITEM_CODE)
                GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                         PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, LCI_ELEMENT_NAME, FLAG';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_DISCOUNT', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT SUBCLASS_TAB_NAME,
                              UPDATE_DATE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              LCI_ELEMENT_NAME,
                              SUM(C1) AS C1,
                              SUM(C2) AS C2,
                              SUM(C3) AS C3,
                              SUM(C4) AS C4,
                              SUM(C5) AS C5,
                              FLAG
                       FROM (SELECT A.*,
                                    B.LCI_ELEMENT_NAME,
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
                                    B.LCI_ELEMENT_NAME,
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
                                           ON A.ITEM_CODE = B.ITEM_CODE)
                       GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                                PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, LCI_ELEMENT_NAME, FLAG';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_FUEL', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT SUBCLASS_TAB_NAME,
                       UPDATE_DATE,
                       MAT_NO,
                       MAT_TRACK_NO,
                       MAT_SEQ_NO,
                       FAMILY_CODE,
                       UNIT_CODE,
                       UNIT_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       PRODUCT_VALUE,
                       LCI_ELEMENT_NAME,
                       SUM(C1) AS C1,
                       SUM(C2) AS C2,
                       SUM(C3) AS C3,
                       SUM(C4) AS C4,
                       SUM(C5) AS C5,
                       FLAG
                FROM (SELECT A.*,
                             B.LCI_ELEMENT_NAME,
                             COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C1,
                             0                                                   AS C2,
                             0                                                   AS C3,
                             0                                                   AS C4,
                             0                                                   AS C5,
                             ''EP''                                              AS FLAG
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                               JOIN (SELECT *
                                     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_EP) B
                                    ON A.ITEM_CODE = B.ITEM_CODE)
                GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                         PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, LCI_ELEMENT_NAME, FLAG';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST_EP', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT SUBCLASS_TAB_NAME,
                       UPDATE_DATE,
                       MAT_NO,
                       MAT_TRACK_NO,
                       MAT_SEQ_NO,
                       FAMILY_CODE,
                       UNIT_CODE,
                       UNIT_NAME,
                       PRODUCT_CODE,
                       PRODUCT_NAME,
                       PRODUCT_VALUE,
                       LCI_ELEMENT_NAME,
                       SUM(C1)     AS C1,
                       SUM(C2)     AS C2,
                       SUM(C3)     AS C3,
                       SUM(C4)     AS C4,
                       SUM(C5)     AS C5,
                       ''CURRENT'' AS FLAG
                FROM (SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_RESOURCE
                      UNION
                      SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_ENERGY
                      UNION
                      SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_DISCOUNT
                      UNION
                      SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_FUEL
                      UNION
                      SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_EP)
                GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                         PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, LCI_ELEMENT_NAME';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST', V_QUERY_STR);


    SET V_QUERY_STR = 'UPDATE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST
                       SET C1 = 0,
                           C2 = 0,
                           C3 = 0,
                           C4 = 0,
                           C5 = 0
                       WHERE LCI_ELEMENT_NAME IN (''use of non-renewable primary energy resources used as raw materials'', ''use of renewable primary energy resources used as raw materials'')';
    PREPARE stmt FROM v_query_str;
    EXECUTE stmt;


    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST
                            (SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO,
                             MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME,
                             PRODUCT_VALUE, LCI_ELEMENT_NAME, C1, C2, C3, C4, C5, FLAG)
                            SELECT A.*
                            FROM (SELECT SUBCLASS_TAB_NAME,
                                         UPDATE_DATE,
                                         MAT_NO,
                                         MAT_TRACK_NO,
                                         MAT_SEQ_NO,
                                         FAMILY_CODE,
                                         UNIT_CODE,
                                         UNIT_NAME,
                                         PRODUCT_CODE,
                                         PRODUCT_NAME,
                                         PRODUCT_VALUE,
                                         LCI_ELEMENT_NAME,
                                         C1,
                                         C2,
                                         C3,
                                         C4,
                                         C5,
                                         ''BEFORE'' AS FLAG
                                  FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '
                                  WHERE SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || '''
                                    AND UPDATE_DATE < ''' || V_START_MONTH || ''') A
                                     JOIN (SELECT DISTINCT MAT_TRACK_NO
                                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO) B
                                          ON A.MAT_TRACK_NO = B.MAT_TRACK_NO';
        PREPARE stmt FROM v_query_str;
        EXECUTE stmt;

    END IF;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('current', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('seq', 'pre', '0', CURRENT_TIMESTAMP);

    --遍历结转工序

    SET V_QUERY_STR = 'SELECT DISTINCT MAX(RANK) AS MAX_RANK ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    SET V_RANK = 1;

    WHILE V_RANK <= V_MAX_RANK
        DO
            SET V_QUERY_STR = 'SELECT COUNT(*) FROM (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV ' ||
                              'WHERE RANK = ' || V_RANK || ') B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE)';
            CALL BG00MAC102.P_EXEC_INTO(V_QUERY_STR, V_ROW_COUNT);

            IF V_ROW_COUNT > 0 THEN
                SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST
                                   SELECT im.SUBCLASS_TAB_NAME,
                                          im.UPDATE_DATE,
                                          im.MAT_NO,
                                          im.MAT_TRACK_NO,
                                          im.MAT_SEQ_NO,
                                          im.FAMILY_CODE,
                                          im.UNIT_CODE,
                                          im.UNIT_NAME,
                                          im.PRODUCT_CODE,
                                          im.PRODUCT_NAME,
                                          im.PRODUCT_VALUE,
                                          dist.LCI_ELEMENT_NAME,
                                          SUM(im.UNIT_COST * dist.C1) AS C1,
                                          SUM(im.UNIT_COST * dist.C2) AS C2,
                                          SUM(im.UNIT_COST * dist.C3) AS C3,
                                          SUM(im.UNIT_COST * dist.C4) AS C4,
                                          SUM(im.UNIT_COST * dist.C5) AS C5,
                                          ''PREV''                    AS FLAG
                                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN im
                                            JOIN (SELECT MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE
                                                  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
                                                  WHERE RANK = ' || V_RANK || ') rp
                                                 ON im.MAT_TRACK_NO = rp.MAT_TRACK_NO
                                                     AND im.FAMILY_CODE = rp.FAMILY_CODE
                                                     AND im.UNIT_CODE = rp.UNIT_CODE
                                            JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST dist
                                                 ON im.MAT_TRACK_NO = dist.MAT_TRACK_NO
                                                     AND im.ITEM_CODE = dist.PRODUCT_CODE
                                   GROUP BY im.SUBCLASS_TAB_NAME, im.UPDATE_DATE, im.MAT_NO, im.MAT_TRACK_NO, im.MAT_SEQ_NO,
                                            im.FAMILY_CODE, im.UNIT_CODE, im.UNIT_NAME, im.PRODUCT_CODE, im.PRODUCT_NAME,
                                            im.PRODUCT_VALUE, dist.LCI_ELEMENT_NAME';
                PREPARE stmt FROM V_QUERY_STR;
                EXECUTE stmt;
            END IF;

            INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
            VALUES ('seq', 'pre', cast(V_RANK as VARCHAR(10)), CURRENT_TIMESTAMP);

            SET V_RANK = V_RANK + 1;

        END WHILE;

    SET V_QUERY_STR = 'SELECT DISTINCT MAX(RANK) AS MAX_RANK ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('seq', 'post', '0', CURRENT_TIMESTAMP);


    SET V_RANK = 1;
    WHILE V_RANK <= V_MAX_RANK
        DO
            SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST (SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO,
                                                                                         MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME,
                                                                                         PRODUCT_VALUE, LCI_ELEMENT_NAME, C1, C2, C3, C4, C5, FLAG)
                               WITH RANKED AS (SELECT MAT_TRACK_NO, FAMILY_CODE, PREV_FAMILY_CODE
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
                                               WHERE RANK = ' || V_RANK || '),
                                    BASE AS (SELECT M.SUBCLASS_TAB_NAME,
                                                    M.UPDATE_DATE,
                                                    M.MAT_NO,
                                                    M.MAT_TRACK_NO,
                                                    M.MAT_SEQ_NO,
                                                    M.FAMILY_CODE,
                                                    M.UNIT_CODE,
                                                    M.UNIT_NAME,
                                                    M.PRODUCT_CODE,
                                                    M.PRODUCT_NAME,
                                                    M.PRODUCT_VALUE,
                                                    M.UNIT_COST,
                                                    R.PREV_FAMILY_CODE
                                             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_INPUT_MAIN M
                                                      JOIN RANKED R
                                                           ON M.MAT_TRACK_NO = R.MAT_TRACK_NO
                                                               AND M.FAMILY_CODE = R.FAMILY_CODE),
                                    DIST_AGG AS (SELECT MAT_TRACK_NO,
                                                        FAMILY_CODE,
                                                        LCI_ELEMENT_NAME,
                                                        SUM(C1) AS C1,
                                                        SUM(C2) AS C2,
                                                        SUM(C3) AS C3,
                                                        SUM(C4) AS C4,
                                                        SUM(C5) AS C5
                                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST
                                                 GROUP BY MAT_TRACK_NO, FAMILY_CODE, LCI_ELEMENT_NAME)
                               SELECT B.SUBCLASS_TAB_NAME,
                                      B.UPDATE_DATE,
                                      B.MAT_NO,
                                      B.MAT_TRACK_NO,
                                      B.MAT_SEQ_NO,
                                      B.FAMILY_CODE,
                                      B.UNIT_CODE,
                                      B.UNIT_NAME,
                                      B.PRODUCT_CODE,
                                      B.PRODUCT_NAME,
                                      B.PRODUCT_VALUE,
                                      D.LCI_ELEMENT_NAME,
                                      SUM(B.UNIT_COST * D.C1) AS C1,
                                      SUM(B.UNIT_COST * D.C2) AS C2,
                                      SUM(B.UNIT_COST * D.C3) AS C3,
                                      SUM(B.UNIT_COST * D.C4) AS C4,
                                      SUM(B.UNIT_COST * D.C5) AS C5,
                                      ''PREV''                AS FLAG
                               FROM BASE B
                                        JOIN DIST_AGG D
                                             ON B.MAT_TRACK_NO = D.MAT_TRACK_NO
                                                 AND B.PREV_FAMILY_CODE = D.FAMILY_CODE
                               GROUP BY B.SUBCLASS_TAB_NAME, B.UPDATE_DATE, B.MAT_NO, B.MAT_TRACK_NO, B.MAT_SEQ_NO,
                                        B.FAMILY_CODE, B.UNIT_CODE, B.UNIT_NAME, B.PRODUCT_CODE, B.PRODUCT_NAME,
                                        B.PRODUCT_VALUE, D.LCI_ELEMENT_NAME';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
            VALUES ('seq', 'post', cast(V_RANK as VARCHAR(10)), CURRENT_TIMESTAMP);

            SET V_RANK = V_RANK + 1;
        END WHILE;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('insert', 'begin', null, CURRENT_TIMESTAMP);


    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || ' WHERE ' ||
                      'SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || ''' ' ||
                      'AND COMPANY_CODE = ''' || V_COMPANY_CODE || ''' ' ||
                      'AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_CAT_BATCH_NUMBER || '''';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || ' (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, MAIN_CAT_BATCH_NUMBER,
                                                                             UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE,
                                                                             UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE,
                                                                             LCI_ELEMENT_NAME, C1, C2, C3, C4, C5, C_INSITE, C_OUTSITE,
                                                                             C_CYCLE, REC_CREATE_TIME)
                       SELECT HEX(RAND()),
                              SUBCLASS_TAB_NAME,
                              ''' || V_COMPANY_CODE || ''',
                              ''' || V_MAIN_CAT_BATCH_NUMBER || ''',
                              UPDATE_DATE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              CONS_LCI_ELEMENT_CNAME AS LCI_ELEMENT_NAME,
                              SUM(C1),
                              SUM(C2),
                              SUM(C3),
                              SUM(C4),
                              SUM(C5),
                              SUM(C1) + SUM(C2),
                              SUM(C3) + SUM(C4) + SUM(C5),
                              SUM(C1) + SUM(C2) + SUM(C3) + SUM(C4) + SUM(C5),
                              TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')
                       FROM (SELECT A.*, B.CONS_LCI_ELEMENT_CNAME
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST A
                                       LEFT JOIN (SELECT CONS_LCI_ELEMENT_NAME, CONS_LCI_ELEMENT_CNAME
                                                  FROM BG00MAC102.T_ADS_FACT_LCA_EPD_LCI_ELEMENT_NORM_CONS_MAP) B
                                                 ON A.LCI_ELEMENT_NAME = B.CONS_LCI_ELEMENT_NAME)
                       GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                                PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, CONS_LCI_ELEMENT_CNAME
                       HAVING 1 = 1';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('insert', 'end', null, CURRENT_TIMESTAMP);
    ------------------------------------处理逻辑(结束)------------------------------------

--     删除生成的临时表
--     CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
--     COMMIT;


    --过程结束时间
--     SET V_LAST_TIMESTAMP = CURRENT_TIMESTAMP;

    --执行成功，记录日志
--     CALL BG00MAC102.P_WRITE_LOG(V_LOG_SCHEMA,
--                                 V_ROUTINE_NAME,
--                                 V_START_TIMESTAMP,
--                                 V_LAST_TIMESTAMP,
--                                 'T',
--                                 V_PARM_INFO,
--                                 'OK');
END;

