CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH(IN V_COMPANY_CODE VARCHAR(4),
                                                                              IN V_START_MONTH VARCHAR(6),
                                                                              IN V_END_MONTH VARCHAR(6),
                                                                              IN V_FACTOR_YEAR VARCHAR(4),
                                                                              IN V_MAIN_TAB_NAME VARCHAR(100),
                                                                              IN V_MAIN_BATCH_NUMBER VARCHAR(100),
                                                                              IN V_SUBCLASS_TAB_NAME VARCHAR(100),
                                                                              IN V_SUBCLASS_RESULT_TAB_NAME VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH
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

    DECLARE V_LOG_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102 '; --日志表所在SCHEMA
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);
    DECLARE MESSAGE_TEXT VARCHAR(2048);

    ------------------------------------日志变量定义------------------------------------
    DECLARE TAR_SCHEMA1 VARCHAR(32) DEFAULT 'BG00MAC102'; --目标表SCHEMA
    DECLARE SRC_TAB_NAME1 VARCHAR(32) DEFAULT ' '; --源表SCHEMA.表名
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH'; --临时表名
    DECLARE V_PNAME VARCHAR(50) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH'; --存储过程名
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

    TRUNCATE TABLE BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG IMMEDIATE;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'begin', null, CURRENT_TIMESTAMP);

    --取大类结果
    SET V_QUERY_STR = 'SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_TAB_NAME || ' WHERE ' ||
                      'BATCH_NUMBER = ''' || V_MAIN_BATCH_NUMBER || ''' AND ' ||
                      'COMPANY_CODE = ''' || V_COMPANY_CODE || ''' AND ' ||
                      'LCI_ELEMENT_NAME = ''全球变暖潜力(GWP100):合计''';
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
                      'VALUE != 0 ';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);


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
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
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
               VALUE,
               UNITM_AC,
               PRODUCT_VALUE,
               CAST(VALUE AS DOUBLE) / CAST(PRODUCT_VALUE AS DOUBLE) AS UNIT_COST
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
               VALUE,
               UNITM_AC,
               PRODUCT_VALUE,
               CAST(VALUE AS DOUBLE) / CAST(PRODUCT_VALUE AS DOUBLE) AS UNIT_COST
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

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'begin', null, CURRENT_TIMESTAMP);

    --数据项和系数
    SET V_QUERY_STR = '
         SELECT DISTINCT *
             FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
             WHERE FLAG = ''FCP''
               AND BASE_CODE = ''' || V_COMPANY_CODE || '''
               AND START_TIME = ''' || V_FACTOR_YEAR || '''
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FCP', V_QUERY_STR);

    SET V_QUERY_STR = '
         SELECT DISTINCT *
             FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
              WHERE FLAG = ''SY''
               AND BASE_CODE = ''' || V_COMPANY_CODE || '''
               AND START_TIME = ''' || V_FACTOR_YEAR || '''
               AND DATA_CODE NOT IN (SELECT DATA_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FCP)
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'SY', V_QUERY_STR);

    SET V_QUERY_STR = '
         SELECT DATA_CODE, UUID, FLAG
             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FCP
             UNION
             (SELECT DATA_CODE, UUID, FLAG FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_SY)
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'UUID', V_QUERY_STR);

    SET V_QUERY_STR = '
           SELECT A.DATA_CODE,
                  A.FLAG,
                  B.NAME,
                  B.LCI_ELEMENT_VALUE
           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_UUID A
                    JOIN (SELECT *
                           FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI
                           WHERE YEAR = ''' || V_FACTOR_YEAR || '''
                             AND FLAG = ''MAT''
                             AND LCI_ELEMENT_NAME = ''全球变暖潜力(GWP100):合计'') B ON A.UUID = B.UUID
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'GWP_SUBSET', V_QUERY_STR);

    SET V_QUERY_STR = '
           SELECT DISTINCT A.ITEM_CODE,
                           A.ITEM_NAME,
                           B.FLAG,
                           B.LCI_ELEMENT_VALUE
           FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME
                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA) A
                    JOIN
                (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_GWP_SUBSET)
                    B
                ON A.ITEM_CODE = B.DATA_CODE
           ORDER BY A.ITEM_CODE
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_GWP', V_QUERY_STR);

    SET V_QUERY_STR = '
         SELECT ITEM_CODE, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS FACTOR_INDIRECT
                    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_GWP
                    WHERE FLAG = ''FCP''
                      AND LCI_ELEMENT_VALUE != 0
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_BP', V_QUERY_STR);

    SET V_QUERY_STR = '
         SELECT ITEM_CODE, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS FACTOR_INDIRECT
             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_GWP
             WHERE FLAG = ''SY''
               AND LCI_ELEMENT_VALUE != 0
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_UP', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(LCI_ELEMENT_VALUE AS DOUBLE) AS FACTOR_INDIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_GWP
            WHERE LCI_ELEMENT_VALUE != 0
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_INDIRECT', V_QUERY_STR);

    --直排系数
    SET V_QUERY_STR = '
         SELECT A.ITEM_CODE, A.ITEM_NAME, B.DISCH_COEFF AS FACTOR_DIRECT
                      FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA) A
                               LEFT JOIN
                           (SELECT DISTINCT ITEM_CODE, DISCH_COEFF
                            FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                            WHERE ORG_CODE = ''' || V_COMPANY_CODE || '''
                              AND START_TIME = ''' || V_FACTOR_YEAR || ''')
                               B
                           ON A.ITEM_CODE = B.ITEM_CODE
                      WHERE B.DISCH_COEFF IS NOT NULL
                      AND DISCH_COEFF != 0
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_DIRECT', V_QUERY_STR);

    --运输系数
    SET V_QUERY_STR = '
             WITH FACTOR_DISTANCE AS (SELECT A.ITEM_CODE,
                                           A.ITEM_NAME,
                                           B.RIVER_CAR_TRANS_VALUE / 1000 AS RIVER_CAR_TRANS_VALUE,
                                           B.TRUCK_CAR_TRANS_VALUE / 1000 AS TRUCK_CAR_TRANS_VALUE,
                                           B.TRAIN_TRANS_VALUE / 1000     AS TRAIN_TRANS_VALUE,
                                           B.CUSTOMS_TRANS_VALUE / 1000   AS CUSTOMS_TRANS_VALUE
                                    FROM (SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' ||
                      V_TMP_TAB || '_DATA) A
                                             LEFT JOIN
                                         (SELECT *
                                          FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                                          WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                                            AND START_TIME = ''' || V_FACTOR_YEAR || ''')
                                             AS B
                                         ON A.ITEM_CODE = B.LCA_DATA_ITEM_CODE),
             FACTOR_TRANSPORT1 AS (SELECT DISTINCT *
                                   FROM BG00MAC102.T_ADS_WH_LCA_FACTOR_LIBRARY_LCI
                          WHERE YEAR = ''' || V_FACTOR_YEAR || '''
                            AND LCI_ELEMENT_NAME = ''全球变暖潜力(GWP100):合计''
                            AND NAME IN (''海运'', ''河运'', ''铁运'', ''汽运'')),
             RIVER_CAR_TRANS_VALUE AS (SELECT ITEM_CODE,
                                              ITEM_NAME,
                                              COALESCE(A.RIVER_CAR_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                                       FROM FACTOR_DISTANCE A
                                                JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE NAME = ''河运'') B ON 1 = 1),
             CUSTOMS_TRANS_VALUE AS (SELECT ITEM_CODE,
                                            ITEM_NAME,
                                            COALESCE(A.CUSTOMS_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                                     FROM FACTOR_DISTANCE A
                                              JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE NAME = ''海运'') B ON 1 = 1),
             TRAIN_TRANS_VALUE AS (SELECT ITEM_CODE,
                                          ITEM_NAME,
                                          COALESCE(A.TRAIN_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                                   FROM FACTOR_DISTANCE A
                                            JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE NAME = ''铁运'') B ON 1 = 1),
             TRUCK_CAR_TRANS_VALUE AS (SELECT ITEM_CODE,
                                              ITEM_NAME,
                                              COALESCE(A.TRUCK_CAR_TRANS_VALUE, 0) * B.LCI_ELEMENT_VALUE AS LCI_ELEMENT_VALUE
                                       FROM FACTOR_DISTANCE A
                                                JOIN (SELECT * FROM FACTOR_TRANSPORT1 WHERE NAME = ''汽运'') B ON 1 = 1),
             TRANS_VALUE AS (SELECT *
                             FROM RIVER_CAR_TRANS_VALUE
                             UNION
                             SELECT * FROM CUSTOMS_TRANS_VALUE
                             UNION
                             SELECT * FROM TRAIN_TRANS_VALUE
                             UNION
                             SELECT * FROM TRUCK_CAR_TRANS_VALUE)
             SELECT ITEM_CODE,
                    ITEM_NAME,
                    SUM(LCI_ELEMENT_VALUE) AS FACTOR_TRANSPORT
             FROM TRANS_VALUE
             GROUP BY ITEM_CODE, ITEM_NAME
             HAVING SUM(LCI_ELEMENT_VALUE) != 0
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_TRANSPORT', V_QUERY_STR);

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
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE_MAIN', V_QUERY_STR);


    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''01'', ''02'', ''03'')
        AND ITEM_CODE IN (SELECT DISTINCT PRODUCT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_PRODUCT)
        EXCEPT
        (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN)
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE_ENERGY', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''01'', ''02'', ''03'')
        EXCEPT
        (
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY
        UNION
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN)
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''01'', ''02'', ''03'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE_ALL', V_QUERY_STR);

    --副产品数据
    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''05'', ''08'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'BY_PRODUCT', V_QUERY_STR);

    --产品副产品数据
    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_CODE IN (''04'', ''05'', ''08'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PRODUCT_BY_PRODUCT', V_QUERY_STR);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('current', 'begin', null, CURRENT_TIMESTAMP);

    --本工序结果
    SET V_QUERY_STR = 'SELECT A.*, ' ||
                      'COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0)     AS C1, ' ||
                      'COALESCE(A.UNIT_COST * C.FACTOR_INDIRECT, 0)   AS C2, ' ||
                      'COALESCE(A.UNIT_COST * D.FACTOR_INDIRECT, 0)   AS C3, ' ||
                      '0                                              AS C4, ' ||
                      'COALESCE(A.UNIT_COST * E.FACTOR_TRANSPORT, 0)  AS C5, ' ||
                      'B.FACTOR_DIRECT                                AS FACTOR_DIRECT, ' ||
                      'COALESCE(C.FACTOR_INDIRECT, D.FACTOR_INDIRECT) AS FACTOR_INDIRECT, ' ||
                      'E.FACTOR_TRANSPORT                             AS FACTOR_TRANSPORT, ' ||
                      '''RESOURCE''                                   AS FLAG ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE A ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B ' ||
                      'ON A.ITEM_CODE = B.ITEM_CODE ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_BP C ' ||
                      'ON A.ITEM_CODE = C.ITEM_CODE ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_UP D ' ||
                      'ON A.ITEM_CODE = D.ITEM_CODE ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT E ' ||
                      'ON A.ITEM_CODE = E.ITEM_CODE UNION ' ||
                      'SELECT A.*, ' ||
                      'COALESCE(-A.UNIT_COST * FACTOR_DIRECT, 0)     AS C1, ' ||
                      '0                                             AS C2, ' ||
                      '0                                             AS C3, ' ||
                      '0                                             AS C4, ' ||
                      '0                                             AS C5, ' ||
                      'B.FACTOR_DIRECT                               AS FACTOR_DIRECT, ' ||
                      'NULL                                          AS FACTOR_INDIRECT, ' ||
                      'NULL                                          AS FACTOR_TRANSPORT, ' ||
                      '''DIRECT''                                    AS FLAG ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PRODUCT_BY_PRODUCT A ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B ' ||
                      'ON A.ITEM_CODE = B.ITEM_CODE UNION ' ||
                      'SELECT A.*, ' ||
                      '0                                             AS C1, ' ||
                      '0                                             AS C2, ' ||
                      '0                                             AS C3, ' ||
                      'COALESCE(-A.UNIT_COST * B.FACTOR_INDIRECT, 0) AS C4, ' ||
                      '0                                             AS C5, ' ||
                      'NULL                                          AS FACTOR_DIRECT, ' ||
                      'B.FACTOR_INDIRECT                             AS FACTOR_INDIRECT, ' ||
                      'NULL                                          AS FACTOR_TRANSPORT, ' ||
                      '''BYPROD''                                    AS FLAG ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_BY_PRODUCT A ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_INDIRECT B ' ||
                      'ON A.ITEM_CODE = B.ITEM_CODE UNION ' ||
                      'SELECT A.*, ' ||
                      'COALESCE(A.UNIT_COST * B.C1_DIRECT, 0) AS C1, ' ||
                      'COALESCE(A.UNIT_COST * B.C2_BP, 0)     AS C2, ' ||
                      'COALESCE(A.UNIT_COST * B.C3_OUT, 0)    AS C3, ' ||
                      'COALESCE(A.UNIT_COST * B.C4_BP_NEG, 0) AS C4, ' ||
                      'COALESCE(A.UNIT_COST * B.C5_TRANS, 0)  AS C5, ' ||
                      'NULL                                   AS FACTOR_DIRECT, ' ||
                      'NULL                                   AS FACTOR_INDIRECT, ' ||
                      'NULL                                   AS FACTOR_TRANSPORT, ' ||
                      '''ENERGY''                             AS FLAG ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY A ' ||
                      'LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT B ' ||
                      'ON A.ITEM_CODE = B.PRODUCT_CODE UNION ' ||
                      'SELECT A.*, ' ||
                      'COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0) AS C1, ' ||
                      '0                                          AS C2, ' ||
                      '0                                          AS C3, ' ||
                      '0                                          AS C4, ' ||
                      '0                                          AS C5, ' ||
                      'B.FACTOR_DIRECT                            AS FACTOR_DIRECT, ' ||
                      'NULL                                       AS FACTOR_INDIRECT, ' ||
                      'NULL                                       AS FACTOR_TRANSPORT, ' ||
                      '''DIRECT''                                 AS FLAG ' ||
                      'FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY UNION ' ||
                      'SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A ' ||
                      'INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B ' ||
                      'ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT_DIST', V_QUERY_STR);

    SET v_query_str = 'CREATE TABLE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL ( ' ||
                      'SUBCLASS_TAB_NAME     VARCHAR(64), ' ||
                      'UPDATE_DATE           VARCHAR(6), ' ||
                      'MAT_NO                VARCHAR(64), ' ||
                      'MAT_TRACK_NO          VARCHAR(64), ' ||
                      'MAT_SEQ_NO            BIGINT, ' ||
                      'FAMILY_CODE           VARCHAR(1000), ' ||
                      'UNIT_CODE             VARCHAR(100), ' ||
                      'UNIT_NAME             VARCHAR(256), ' ||
                      'PRODUCT_CODE          VARCHAR(100), ' ||
                      'PRODUCT_NAME          VARCHAR(256), ' ||
                      'TYPE_CODE             VARCHAR(20), ' ||
                      'TYPE_NAME             VARCHAR(64), ' ||
                      'ITEM_CODE             VARCHAR(100), ' ||
                      'ITEM_NAME             VARCHAR(256), ' ||
                      'VALUE                 DECIMAL(27, 6), ' ||
                      'UNITM_AC              VARCHAR(64), ' ||
                      'PRODUCT_VALUE         DECIMAL(27, 6), ' ||
                      'UNIT_COST             DOUBLE, ' ||
                      'C1                    DOUBLE, ' ||
                      'C2                    DOUBLE, ' ||
                      'C3                    DOUBLE, ' ||
                      'C4                    DOUBLE, ' ||
                      'C5                    DOUBLE, ' ||
                      'FACTOR_DIRECT         DOUBLE, ' ||
                      'FACTOR_INDIRECT       DOUBLE, ' ||
                      'FACTOR_TRANSPORT      DOUBLE, ' ||
                      'FLAG                  VARCHAR(32))';
    PREPARE stmt FROM v_query_str;
    EXECUTE stmt;

    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST ' ||
                          '(SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, ' ||
                          'MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, ' ||
                          'TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC, PRODUCT_VALUE, ' ||
                          'UNIT_COST, C1, C2, C3, C4, C5, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG) ' ||
                          'SELECT A.* FROM (SELECT SUBCLASS_TAB_NAME, ' ||
                          'UPDATE_DATE, ' ||
                          'MAT_NO, ' ||
                          'MAT_TRACK_NO, ' ||
                          'MAT_SEQ_NO, ' ||
                          'FAMILY_CODE, ' ||
                          'UNIT_CODE, ' ||
                          'UNIT_NAME, ' ||
                          'PRODUCT_CODE, ' ||
                          'PRODUCT_NAME, ' ||
                          'TYPE_CODE, ' ||
                          'TYPE_NAME, ' ||
                          'ITEM_CODE, ' ||
                          'ITEM_NAME, ' ||
                          'VALUE, ' ||
                          'UNITM_AC, ' ||
                          'PRODUCT_VALUE, ' ||
                          'UNIT_COST, ' ||
                          'C1, ' ||
                          'C2, ' ||
                          'C3, ' ||
                          'C4, ' ||
                          'C5, ' ||
                          'FACTOR_DIRECT, ' ||
                          'FACTOR_INDIRECT, ' ||
                          'FACTOR_TRANSPORT, ' ||
                          'FLAG FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_DIST ' ||
                          'WHERE SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || ''' ' ||
                          'AND UPDATE_DATE < ''' || V_START_MONTH || ''') A JOIN (SELECT DISTINCT MAT_TRACK_NO ' ||
                          'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                          '_MAT_TRACK_NO) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO';
        PREPARE stmt FROM v_query_str;
        EXECUTE stmt;

        SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL ' ||
                          '(SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, ' ||
                          'FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ' ||
                          'ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC, PRODUCT_VALUE, UNIT_COST, C1, C2, C3, C4, C5, ' ||
                          'FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG) ' ||
                          'SELECT A.* FROM (SELECT SUBCLASS_TAB_NAME, ' ||
                          'UPDATE_DATE, ' ||
                          'MAT_NO, ' ||
                          'MAT_TRACK_NO, ' ||
                          'MAT_SEQ_NO, ' ||
                          'FAMILY_CODE, ' ||
                          'UNIT_CODE, ' ||
                          'UNIT_NAME, ' ||
                          'PRODUCT_CODE, ' ||
                          'PRODUCT_NAME, ' ||
                          'TYPE_CODE, ' ||
                          'TYPE_NAME, ' ||
                          'ITEM_CODE, ' ||
                          'ITEM_NAME, ' ||
                          'VALUE, ' ||
                          'UNITM_AC, ' ||
                          'PRODUCT_VALUE, ' ||
                          'UNIT_COST, ' ||
                          'C1, ' ||
                          'C2, ' ||
                          'C3, ' ||
                          'C4, ' ||
                          'C5, ' ||
                          'FACTOR_DIRECT, ' ||
                          'FACTOR_INDIRECT, ' ||
                          'FACTOR_TRANSPORT, ' ||
                          'FLAG ' ||
                          'FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_DIST_PARALLEL ' ||
                          'WHERE SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || ''' ' ||
                          'AND UPDATE_DATE < ''' || V_START_MONTH || ''') A JOIN (SELECT DISTINCT MAT_TRACK_NO ' ||
                          'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO) B ' ||
                          'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO';
        PREPARE stmt FROM v_query_str;
        EXECUTE stmt;
    end if;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('current', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('seq', 'pre', '0', CURRENT_TIMESTAMP);

    --遍历结转工序

    SET V_QUERY_STR = 'SELECT DISTINCT MAX(RANK) AS MAX_RANK ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    SET V_RANK = 1;

    WHILE V_RANK <= V_MAX_RANK
        DO
            SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL ' ||
                              'SELECT SUBCLASS_TAB_NAME, ' ||
                              'UPDATE_DATE, ' ||
                              'MAT_NO, ' ||
                              'MAT_TRACK_NO, ' ||
                              'MAT_SEQ_NO, ' ||
                              'FAMILY_CODE, ' ||
                              'UNIT_CODE, ' ||
                              'UNIT_NAME, ' ||
                              'PRODUCT_CODE, ' ||
                              'PRODUCT_NAME, ' ||
                              'TYPE_CODE, ' ||
                              'TYPE_NAME, ' ||
                              'ITEM_CODE, ' ||
                              'ITEM_NAME, ' ||
                              'SUM(VALUE)     AS VALUE, ' ||
                              'UNITM_AC, ' ||
                              'PRODUCT_VALUE, ' ||
                              'SUM(UNIT_COST) AS UNIT_COST, ' ||
                              'SUM(C1)        AS C1, ' ||
                              'SUM(C2)        AS C2, ' ||
                              'SUM(C3)        AS C3, ' ||
                              'SUM(C4)        AS C4, ' ||
                              'SUM(C5)        AS C5, ' ||
                              'FACTOR_DIRECT, ' ||
                              'FACTOR_INDIRECT, ' ||
                              'FACTOR_TRANSPORT, ' ||
                              'FLAG ' ||
                              'FROM (SELECT A.* ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV ' ||
                              'WHERE RANK = ' || V_RANK || ') B ' ||
                              'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'UNION SELECT A.SUBCLASS_TAB_NAME, ' ||
                              'A.UPDATE_DATE, ' ||
                              'A.MAT_NO, ' ||
                              'A.MAT_TRACK_NO, ' ||
                              'A.MAT_SEQ_NO, ' ||
                              'A.FAMILY_CODE, ' ||
                              'A.UNIT_CODE, ' ||
                              'A.UNIT_NAME, ' ||
                              'A.PRODUCT_CODE, ' ||
                              'A.PRODUCT_NAME, ' ||
                              'B.TYPE_CODE, ' ||
                              'B.TYPE_NAME, ' ||
                              'B.ITEM_CODE, ' ||
                              'B.ITEM_NAME, ' ||
                              'A.UNIT_COST * B.VALUE, ' ||
                              'B.UNITM_AC, ' ||
                              'A.PRODUCT_VALUE, ' ||
                              'A.UNIT_COST * B.UNIT_COST, ' ||
                              'A.UNIT_COST * B.C1, ' ||
                              'A.UNIT_COST * B.C2, ' ||
                              'A.UNIT_COST * B.C3, ' ||
                              'A.UNIT_COST * B.C4, ' ||
                              'A.UNIT_COST * B.C5, ' ||
                              'B.FACTOR_DIRECT, ' ||
                              'B.FACTOR_INDIRECT, ' ||
                              'B.FACTOR_TRANSPORT, ' ||
                              'B.FLAG ' ||
                              'FROM (SELECT A.* ' ||
                              'FROM (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV ' ||
                              'WHERE RANK = ' || V_RANK || ' AND SUM_FLAG = 0) B ' ||
                              'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE) A ' ||
                              'INNER JOIN (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL) B ' ||
                              'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE ' ||
                              'UNION SELECT A.SUBCLASS_TAB_NAME, ' ||
                              'A.UPDATE_DATE, ' ||
                              'A.MAT_NO, ' ||
                              'A.MAT_TRACK_NO, ' ||
                              'A.MAT_SEQ_NO, ' ||
                              'A.FAMILY_CODE, ' ||
                              'A.UNIT_CODE, ' ||
                              'A.UNIT_NAME, ' ||
                              'A.PRODUCT_CODE, ' ||
                              'A.PRODUCT_NAME, ' ||
                              'A.TYPE_CODE, ' ||
                              'A.TYPE_NAME, ' ||
                              'A.ITEM_CODE, ' ||
                              'A.ITEM_NAME, ' ||
                              'A.VALUE, ' ||
                              'A.UNITM_AC, ' ||
                              'A.PRODUCT_VALUE, ' ||
                              'A.UNIT_COST, ' ||
                              'A.UNIT_COST * B.C1, ' ||
                              'A.UNIT_COST * B.C2, ' ||
                              'A.UNIT_COST * B.C3, ' ||
                              'A.UNIT_COST * B.C4, ' ||
                              'A.UNIT_COST * B.C5, ' ||
                              'NULL, ' ||
                              'NULL, ' ||
                              'NULL, ' ||
                              '''PREV_SUM'' ' ||
                              'FROM (SELECT A.* FROM (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A ' ||
                              'INNER JOIN (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV ' ||
                              'WHERE RANK = ' || V_RANK || ' AND SUM_FLAG = 1) B ' ||
                              'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE) A ' ||
                              'INNER JOIN (SELECT SUBCLASS_TAB_NAME, ' ||
                              'UPDATE_DATE, ' ||
                              'MAT_NO, ' ||
                              'MAT_TRACK_NO, ' ||
                              'MAT_SEQ_NO, ' ||
                              'FAMILY_CODE, ' ||
                              'UNIT_CODE, ' ||
                              'UNIT_NAME, ' ||
                              'PRODUCT_CODE, ' ||
                              'PRODUCT_NAME, ' ||
                              'PRODUCT_VALUE, ' ||
                              'SUM(C1) AS C1, ' ||
                              'SUM(C2) AS C2, ' ||
                              'SUM(C3) AS C3, ' ||
                              'SUM(C4) AS C4, ' ||
                              'SUM(C5) AS C5 ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL ' ||
                              'GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, ' ||
                              'UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE) B ' ||
                              'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE) ' ||
                              'GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, ' ||
                              'FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ' ||
                              'ITEM_CODE, ITEM_NAME, UNITM_AC, PRODUCT_VALUE, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            SET V_QUERY_STR = 'SELECT COUNT(*) FROM (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV ' ||
                              'WHERE RANK = ' || V_RANK || ') B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE)';
            CALL BG00MAC102.P_EXEC_INTO(V_QUERY_STR, V_ROW_COUNT);

            IF V_ROW_COUNT > 0 THEN
                SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST ' ||
                                  'SELECT A.*, ' ||
                                  'A.UNIT_COST * B.C1, ' ||
                                  'A.UNIT_COST * B.C2, ' ||
                                  'A.UNIT_COST * B.C3, ' ||
                                  'A.UNIT_COST * B.C4, ' ||
                                  'A.UNIT_COST * B.C5, ' ||
                                  'NULL, ' ||
                                  'NULL, ' ||
                                  'NULL, ' ||
                                  '''UPSTREAM'' FROM (SELECT A.* ' ||
                                  'FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A ' ||
                                  'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV ' ||
                                  'WHERE RANK = ' || V_RANK || ') B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                                  'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                                  'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                                  'AND A.UNIT_CODE = B.UNIT_CODE) A INNER JOIN (SELECT MAT_TRACK_NO, ' ||
                                  'FAMILY_CODE, ' ||
                                  'UNIT_CODE, ' ||
                                  'PRODUCT_CODE, ' ||
                                  'SUM(C1) AS C1, ' ||
                                  'SUM(C2) AS C2, ' ||
                                  'SUM(C3) AS C3, ' ||
                                  'SUM(C4) AS C4, ' ||
                                  'SUM(C5) AS C5 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST ' ||
                                  'GROUP BY MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B ' ||
                                  'ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE';
                PREPARE stmt FROM V_QUERY_STR;
                EXECUTE stmt;
            END IF;

            INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
            VALUES ('seq', 'pre', cast(V_RANK as VARCHAR(10)), CURRENT_TIMESTAMP);

            SET V_RANK = V_RANK + 1;

        END WHILE;

    SET V_QUERY_STR = 'SELECT DISTINCT MAX(RANK) AS MAX_RANK ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('seq', 'post', '0', CURRENT_TIMESTAMP);


    SET V_RANK = 1;
    WHILE V_RANK <= V_MAX_RANK
        DO
            SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL ' ||
                              'SELECT SUBCLASS_TAB_NAME, ' ||
                              'UPDATE_DATE, ' ||
                              'MAT_NO, ' ||
                              'MAT_TRACK_NO, ' ||
                              'MAT_SEQ_NO, ' ||
                              'FAMILY_CODE, ' ||
                              'UNIT_CODE, ' ||
                              'UNIT_NAME, ' ||
                              'PRODUCT_CODE, ' ||
                              'PRODUCT_NAME, ' ||
                              'TYPE_CODE, ' ||
                              'TYPE_NAME, ' ||
                              'ITEM_CODE, ' ||
                              'ITEM_NAME, ' ||
                              'SUM(VALUE)     AS VALUE, ' ||
                              'UNITM_AC, ' ||
                              'PRODUCT_VALUE, ' ||
                              'SUM(UNIT_COST) AS UNIT_COST, ' ||
                              'SUM(C1)        AS C1, ' ||
                              'SUM(C2)        AS C2, ' ||
                              'SUM(C3)        AS C3, ' ||
                              'SUM(C4)        AS C4, ' ||
                              'SUM(C5)        AS C5, ' ||
                              'FACTOR_DIRECT, ' ||
                              'FACTOR_INDIRECT, ' ||
                              'FACTOR_TRANSPORT, ' ||
                              'FLAG FROM (SELECT A.* ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST ' ||
                              'WHERE RANK = ' || V_RANK || ') B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE ' ||
                              'UNION ' ||
                              'SELECT A.SUBCLASS_TAB_NAME, ' ||
                              'A.UPDATE_DATE, ' ||
                              'A.MAT_NO, ' ||
                              'A.MAT_TRACK_NO, ' ||
                              'A.MAT_SEQ_NO, ' ||
                              'A.FAMILY_CODE, ' ||
                              'A.UNIT_CODE, ' ||
                              'A.UNIT_NAME, ' ||
                              'A.PRODUCT_CODE, ' ||
                              'A.PRODUCT_NAME, ' ||
                              'C.TYPE_CODE, ' ||
                              'C.TYPE_NAME, ' ||
                              'C.ITEM_CODE, ' ||
                              'C.ITEM_NAME, ' ||
                              'A.UNIT_COST * C.VALUE, ' ||
                              'C.UNITM_AC, ' ||
                              'A.PRODUCT_VALUE, ' ||
                              'A.UNIT_COST * C.UNIT_COST, ' ||
                              'A.UNIT_COST * C.C1, ' ||
                              'A.UNIT_COST * C.C2, ' ||
                              'A.UNIT_COST * C.C3, ' ||
                              'A.UNIT_COST * C.C4, ' ||
                              'A.UNIT_COST * C.C5, ' ||
                              'C.FACTOR_DIRECT, ' ||
                              'C.FACTOR_INDIRECT, ' ||
                              'C.FACTOR_TRANSPORT, ' ||
                              'C.FLAG FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB ||
                              '_RESOURCE_MAIN) A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST ' ||
                              'WHERE RANK = ' || V_RANK || ' AND SUM_FLAG = 0) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE INNER JOIN ' ||
                              '(SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL) C ' ||
                              'ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE ' ||
                              'UNION ' ||
                              'SELECT A.SUBCLASS_TAB_NAME, ' ||
                              'A.UPDATE_DATE, ' ||
                              'A.MAT_NO, ' ||
                              'A.MAT_TRACK_NO, ' ||
                              'A.MAT_SEQ_NO, ' ||
                              'A.FAMILY_CODE, ' ||
                              'A.UNIT_CODE, ' ||
                              'A.UNIT_NAME, ' ||
                              'A.PRODUCT_CODE, ' ||
                              'A.PRODUCT_NAME, ' ||
                              'A.TYPE_CODE, ' ||
                              'A.TYPE_NAME, ' ||
                              'A.ITEM_CODE, ' ||
                              'A.ITEM_NAME, ' ||
                              'A.VALUE, ' ||
                              'A.UNITM_AC, ' ||
                              'A.PRODUCT_VALUE, ' ||
                              'A.UNIT_COST, ' ||
                              'A.UNIT_COST * C.C1, ' ||
                              'A.UNIT_COST * C.C2, ' ||
                              'A.UNIT_COST * C.C3, ' ||
                              'A.UNIT_COST * C.C4, ' ||
                              'A.UNIT_COST * C.C5, ' ||
                              'NULL, ' ||
                              'NULL, ' ||
                              'NULL, ' ||
                              '''PREV_SUM'' FROM ' ||
                              '(SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST ' ||
                              'WHERE RANK = ' || V_RANK || ' AND SUM_FLAG = 1) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE INNER JOIN (SELECT SUBCLASS_TAB_NAME, ' ||
                              'UPDATE_DATE, ' ||
                              'MAT_NO, ' ||
                              'MAT_TRACK_NO, ' ||
                              'MAT_SEQ_NO, ' ||
                              'FAMILY_CODE, ' ||
                              'UNIT_CODE, ' ||
                              'UNIT_NAME, ' ||
                              'PRODUCT_CODE, ' ||
                              'PRODUCT_NAME, ' ||
                              'PRODUCT_VALUE, ' ||
                              'SUM(C1) AS C1, ' ||
                              'SUM(C2) AS C2, ' ||
                              'SUM(C3) AS C3, ' ||
                              'SUM(C4) AS C4, ' ||
                              'SUM(C5) AS C5 ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL ' ||
                              'GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, ' ||
                              'MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE) C ' ||
                              'ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE) ' ||
                              'GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, ' ||
                              'FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ' ||
                              'ITEM_CODE, ITEM_NAME, UNITM_AC, PRODUCT_VALUE, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST ' ||
                              'SELECT A.*, ' ||
                              'A.UNIT_COST * C.C1, ' ||
                              'A.UNIT_COST * C.C2, ' ||
                              'A.UNIT_COST * C.C3, ' ||
                              'A.UNIT_COST * C.C4, ' ||
                              'A.UNIT_COST * C.C5, ' ||
                              'NULL, ' ||
                              'NULL, ' ||
                              'NULL, ' ||
                              '''UPSTREAM'' FROM (SELECT * ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A ' ||
                              'INNER JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST ' ||
                              'WHERE RANK = ' || V_RANK || ') B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO ' ||
                              'AND A.UPDATE_DATE = B.UPDATE_DATE ' ||
                              'AND A.FAMILY_CODE = B.FAMILY_CODE ' ||
                              'AND A.UNIT_CODE = B.UNIT_CODE INNER JOIN (SELECT MAT_TRACK_NO, ' ||
                              'FAMILY_CODE, ' ||
                              'UNIT_CODE, ' ||
                              'PRODUCT_CODE, ' ||
                              'SUM(C1) AS C1, ' ||
                              'SUM(C2) AS C2, ' ||
                              'SUM(C3) AS C3, ' ||
                              'SUM(C4) AS C4, ' ||
                              'SUM(C5) AS C5 ' ||
                              'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST ' ||
                              'GROUP BY MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) C ' ||
                              'ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
            VALUES ('seq', 'post', cast(V_RANK as VARCHAR(10)), CURRENT_TIMESTAMP);

            SET V_RANK = V_RANK + 1;
        END WHILE;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('insert', 'begin', null, CURRENT_TIMESTAMP);


    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || ' WHERE ' ||
                      'SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || ''' ' ||
                      'AND COMPANY_CODE = ''' || V_COMPANY_CODE || ''' ' ||
                      'AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_BATCH_NUMBER || '''';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_DIST ' ||
                      'WHERE SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || ''' ' ||
                      'AND COMPANY_CODE = ''' || V_COMPANY_CODE || ''' ' ||
                      'AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_BATCH_NUMBER || '''';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'DELETE FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_DIST_PARALLEL ' ||
                      'WHERE SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || ''' ' ||
                      'AND COMPANY_CODE = ''' || V_COMPANY_CODE || ''' ' ||
                      'AND MAIN_CAT_BATCH_NUMBER = ''' || V_MAIN_BATCH_NUMBER || '''';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_DIST ' ||
                      '(REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, MAIN_CAT_BATCH_NUMBER, UPDATE_DATE, ' ||
                      'MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, ' ||
                      'TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC, PRODUCT_VALUE, UNIT_COST, C1, C2, ' ||
                      'C3, C4, C5, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG, REC_CREATOR, REC_CREATE_TIME, ' ||
                      'REC_REVISOR, REC_REVISE_TIME) ' ||
                      'SELECT HEX(RAND()), ' ||
                      'SUBCLASS_TAB_NAME, ' ||
                      '''' || V_COMPANY_CODE || ''', ' ||
                      '''' || V_MAIN_BATCH_NUMBER || ''', ' ||
                      'UPDATE_DATE, ' ||
                      'MAT_NO, ' ||
                      'MAT_TRACK_NO, ' ||
                      'MAT_SEQ_NO, ' ||
                      'FAMILY_CODE, ' ||
                      'UNIT_CODE, ' ||
                      'UNIT_NAME, ' ||
                      'PRODUCT_CODE, ' ||
                      'PRODUCT_NAME, ' ||
                      'TYPE_CODE, ' ||
                      'TYPE_NAME, ' ||
                      'ITEM_CODE, ' ||
                      'ITEM_NAME, ' ||
                      'VALUE, ' ||
                      'UNITM_AC, ' ||
                      'PRODUCT_VALUE, ' ||
                      'UNIT_COST, ' ||
                      'C1, ' ||
                      'C2, ' ||
                      'C3, ' ||
                      'C4, ' ||
                      'C5, ' ||
                      'FACTOR_DIRECT, ' ||
                      'FACTOR_INDIRECT, ' ||
                      'FACTOR_TRANSPORT, ' ||
                      'FLAG, ' ||
                      'NULL, ' ||
                      'TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''), ' ||
                      'NULL, ' ||
                      'NULL ' ||
                      'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST WHERE 1=1';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || '_DIST_PARALLEL ' ||
                      '(REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, MAIN_CAT_BATCH_NUMBER, UPDATE_DATE, ' ||
                      'MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, ' ||
                      'TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC, PRODUCT_VALUE, UNIT_COST, DEPT_NAME, ' ||
                      'DEPT_CODE, DEPT_MID_NAME, C1, C2, C3, C4, C5, C_CYCLE, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, ' ||
                      'FLAG, REC_CREATOR, REC_CREATE_TIME, REC_REVISOR, REC_REVISE_TIME) ' ||
                      'SELECT HEX(RAND()), ' ||
                      'SUBCLASS_TAB_NAME, ' ||
                      '''' || V_COMPANY_CODE || ''', ' ||
                      '''' || V_MAIN_BATCH_NUMBER || ''', ' ||
                      'UPDATE_DATE, ' ||
                      'MAT_NO, ' ||
                      'MAT_TRACK_NO, ' ||
                      'MAT_SEQ_NO, ' ||
                      'FAMILY_CODE, ' ||
                      'A.UNIT_CODE, ' ||
                      'A.UNIT_NAME, ' ||
                      'PRODUCT_CODE, ' ||
                      'PRODUCT_NAME, ' ||
                      'TYPE_CODE, ' ||
                      'TYPE_NAME, ' ||
                      'ITEM_CODE, ' ||
                      'ITEM_NAME, ' ||
                      'VALUE, ' ||
                      'UNITM_AC, ' ||
                      'PRODUCT_VALUE, ' ||
                      'UNIT_COST, ' ||
                      'B.DEPT_NAME, ' ||
                      'B.DEPT_CODE, ' ||
                      'B.DEPT_MID_NAME, ' ||
                      'C1, ' ||
                      'C2, ' ||
                      'C3, ' ||
                      'C4, ' ||
                      'C5, ' ||
                      'C1 + C2 + C3 + C4 + C5, ' ||
                      'FACTOR_DIRECT, ' ||
                      'FACTOR_INDIRECT, ' ||
                      'FACTOR_TRANSPORT, ' ||
                      'FLAG, ' ||
                      'NULL, ' ||
                      'TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''), ' ||
                      'NULL, ' ||
                      'NULL FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL A ' ||
                      'LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' ||
                      V_COMPANY_CODE || ''') B ' ||
                      'ON A.UNIT_CODE = B.UNIT_CODE WHERE 1=1 ';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = 'INSERT INTO ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_RESULT_TAB_NAME || ' (REC_ID, ' ||
                      'SUBCLASS_TAB_NAME, COMPANY_CODE, MAIN_CAT_BATCH_NUMBER, UPDATE_DATE, MAT_NO, ' ||
                      'MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, ' ||
                      'PRODUCT_VALUE, C1, C2, C3, C4, C5, C_INSITE, C_OUTSITE, C_CYCLE, REC_CREATOR, REC_CREATE_TIME, ' ||
                      'REC_REVISOR, REC_REVISE_TIME) SELECT HEX(RAND()), ' ||
                      'SUBCLASS_TAB_NAME, ' ||
                      '''' || V_COMPANY_CODE || ''', ' ||
                      '''' || V_MAIN_BATCH_NUMBER || ''', ' ||
                      'UPDATE_DATE, ' ||
                      'MAT_NO, ' ||
                      'MAT_TRACK_NO, ' ||
                      'MAT_SEQ_NO, ' ||
                      'FAMILY_CODE, ' ||
                      'UNIT_CODE, ' ||
                      'UNIT_NAME, ' ||
                      'PRODUCT_CODE, ' ||
                      'PRODUCT_NAME, ' ||
                      'PRODUCT_VALUE, ' ||
                      'SUM(C1), ' ||
                      'SUM(C2), ' ||
                      'SUM(C3), ' ||
                      'SUM(C4), ' ||
                      'SUM(C5), ' ||
                      'SUM(C1) + SUM(C2), ' ||
                      'SUM(C3) + SUM(C4) + SUM(C5), ' ||
                      'SUM(C1) + SUM(C2) + SUM(C3) + SUM(C4) + SUM(C5), ' ||
                      'NULL, ' ||
                      'TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''), ' ||
                      'NULL, ' ||
                      'NULL FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST ' ||
                      'GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, ' ||
                      'FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE HAVING 1=1 ';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_MONTH_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('insert', 'end', null, CURRENT_TIMESTAMP);
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

