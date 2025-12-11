CREATE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC(IN V_COMPANY_CODE VARCHAR(4),
                                                             IN V_START_DATE VARCHAR(6),
                                                             IN V_END_DATE VARCHAR(6),
                                                             IN V_FACTOR_YEAR VARCHAR(4),
                                                             IN V_SUBCLASS_TAB_NAME VARCHAR(100),
                                                             IN V_SUB_BATCH_NUMBER VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC
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
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);
    DECLARE MESSAGE_TEXT VARCHAR(2048);

    ------------------------------------日志变量定义------------------------------------
    DECLARE TAR_SCHEMA1 VARCHAR(32) DEFAULT 'BG00MAC102'; --目标表SCHEMA
    DECLARE TAR_TAB1 VARCHAR(50) DEFAULT 'T_ADS_FACT_LCA_SUBCLASS_RESULT'; --目标表名
    DECLARE SRC_TAB_NAME1 VARCHAR(32) DEFAULT ' '; --源表SCHEMA.表名
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(32) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC'; --临时表名
    DECLARE V_PNAME VARCHAR(50) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC'; --存储过程名
    DECLARE V_QUERY_STR CLOB(1097 M); --查询SQL
    DECLARE V_TMP_NAME VARCHAR(128);
    --完整的临时表名
    ------------------------------------存储过程变量定义---------------------------------

    DECLARE QUR_YEAR VARCHAR(4);
    DECLARE V_MAX_BATCH_NUMBER VARCHAR(100);
    DECLARE V_MAT_TRACK_NO VARCHAR(64);
    DECLARE V_FAMILY_CODE VARCHAR(64);
    DECLARE V_UNIT_CODE VARCHAR(64);
    DECLARE V_PREV_FAMILY_CODE VARCHAR(64);
    DECLARE V_MAT_SEQ_NO VARCHAR(64);
    DECLARE V_ROW_COUNT INT;

    DECLARE DONE INT DEFAULT 0;
    DECLARE CURSOR1 CURSOR FOR S1;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONE = 1;

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
    SET QUR_YEAR = LEFT(V_START_DATE, 4);

    --删除此存储过程创建的所有临时表（如果上次执行出错的话，有可能有些临时表没删）
    CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
    COMMIT;

    ------------------------------------处理逻辑(开始)------------------------------------

    --取大类批次号
    SET V_QUERY_STR = '
        SELECT DISTINCT MAX(BATCH_NUMBER) AS BATCH_NUMBER
		FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
		WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || '''
        AND FLAG = ''Y''
        AND TIME_FLAG = ''Y''
        AND YEAR = ''' || QUR_YEAR || '''
        AND MONTH = ''01''
        AND END_MONTH = ''12''
    ';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_BATCH_NUMBER);

    --取大类结果
    SET V_QUERY_STR = '
                SELECT *
                FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_ENERGY_RESULT
                WHERE BATCH_NUMBER = ''' || V_MAX_BATCH_NUMBER || '''
                AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                AND LCI_ELEMENT_NAME = ''全球变暖潜力(GWP100):合计''
        ';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ENERGY_RESULT', V_QUERY_STR);

    --取活动数据
    SET V_QUERY_STR = '
        SELECT BATCH_NUMBER,
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
        WHERE
        BATCH_NUMBER = ''' || V_SUB_BATCH_NUMBER || '''
    ';
--     AND MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);

    --PROC信息
    SET V_QUERY_STR = '
         SELECT
            MAT_TRACK_NO,
            FAMILY_CODE,
            UNIT_CODE,
            UNIT_NAME,
            ITEM_CODE AS PRODUCT_CODE,
            ITEM_NAME AS PRODUCT_NAME,
            VALUE
        FROM
            ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
        WHERE TYPE_NAME = ''产品''
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_PRODUCT_LIST', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT ''' || V_SUBCLASS_TAB_NAME || ''' AS SUBCLASS_TAB_NAME,
               BATCH_NUMBER,
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
               A.VALUE,
               UNITM_AC,
               B.VALUE AS PRODUCT_VALUE,
               CAST(A.VALUE AS DOUBLE) / CAST(B.VALUE AS DOUBLE) AS UNIT_COST
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA A
               JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
               ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                AND A.FAMILY_CODE = B.FAMILY_CODE
                AND A.UNIT_CODE = B.UNIT_CODE
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DATA', V_QUERY_STR);

    --工艺路径
    SET V_QUERY_STR = '
         SELECT DISTINCT MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
           CASE
           WHEN FAMILY_CODE = ''00'' OR FAMILY_CODE = ''01'' THEN ''00''
           ELSE LEFT(FAMILY_CODE, LENGTH(FAMILY_CODE) - 2)
           END AS PREV_FAMILY_CODE
         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
         ORDER BY FAMILY_CODE, MAT_SEQ_NO
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_SEQ', V_QUERY_STR);

    --数据项和系数
    SET V_QUERY_STR = '
         SELECT DISTINCT a.ITEM_CODE,
                         b.DISCH_COEFF,
                         d.GWP,
                         e.CUSTOMS_TRANS_VALUE,
                         e.TRAIN_TRANS_VALUE,
                         e.TRUCK_CAR_TRANS_VALUE,
                         e.RIVER_CAR_TRANS_VALUE,
                         c.FLAG
                FROM (SELECT DISTINCT ITEM_CODE
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA) AS a
                         LEFT JOIN (SELECT DISTINCT *
                                    FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
                                    WHERE org_code = ''' || V_COMPANY_CODE || '''
                                      AND start_time = ''' || V_FACTOR_YEAR || ''') AS b
                                ON a.ITEM_CODE = b.ITEM_CODE
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
                                    WHERE base_code = ''' || V_COMPANY_CODE || '''
                                      AND start_time = ''' || V_FACTOR_YEAR || ''') AS c
                                ON a.ITEM_CODE = c.DATA_code
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES
                                    WHERE company_code = ''' || V_COMPANY_CODE || '''
                                      AND start_time = ''' || V_FACTOR_YEAR || ''') AS d
                                ON c.uuid = d.uuid
                         LEFT JOIN (SELECT *
                                    FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
                                    WHERE company_code = ''' || V_COMPANY_CODE || '''
                                      AND start_time = ''' || V_FACTOR_YEAR || ''') AS e
                                ON a.ITEM_CODE = e.LCA_DATA_ITEM_CODE
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR', V_QUERY_STR);

    --将运输系数汇总
    SET V_QUERY_STR = '
        SELECT B.ITEM_CODE, CAST(SUM(B.FACTOR_TRANSPORT) AS DOUBLE) AS FACTOR_TRANSPORT
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR A,
        TABLE (
            VALUES
            (A.ITEM_CODE, A.CUSTOMS_TRANS_VALUE/1000*0.00667220376406485),
            (A.ITEM_CODE, A.TRAIN_TRANS_VALUE/1000*0.0438528213459938),
            (A.ITEM_CODE, A.TRUCK_CAR_TRANS_VALUE/1000*0.138092140707126),
            (A.ITEM_CODE, A.RIVER_CAR_TRANS_VALUE/1000*0.0487214598443683)
            ) AS B (ITEM_CODE, FACTOR_TRANSPORT)
        WHERE B.FACTOR_TRANSPORT IS NOT NULL
        GROUP BY B.ITEM_CODE
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_TRANSPORT', V_QUERY_STR);

    --直排系数
    SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(DISCH_COEFF AS DOUBLE) AS FACTOR_DIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR
            WHERE DISCH_COEFF IS NOT NULL
              AND DISCH_COEFF != 0
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_DIRECT', V_QUERY_STR);

    --间排系数 副产品 上游
    SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR
            WHERE GWP IS NOT NULL
              AND GWP != 0';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_INDIRECT', V_QUERY_STR);

    IF V_COMPANY_CODE = 'TA' THEN
        SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR
            WHERE FLAG = ''FCP''
              AND GWP IS NOT NULL
              AND GWP != 0';
    ELSE
        SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR
            WHERE ITEM_CODE IN (SELECT DISTINCT ITEM_CODE
                                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                                WHERE TYPE_NAME = ''副产品'')
              AND GWP IS NOT NULL
              AND GWP != 0';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_BP', V_QUERY_STR);

    IF V_COMPANY_CODE = 'TA' THEN
        SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR
            WHERE FLAG = ''SY''
                AND GWP IS NOT NULL
                AND GWP != 0
            EXCEPT
                SELECT *
                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_BP';
    ELSE
        SET V_QUERY_STR = '
        SELECT DISTINCT ITEM_CODE, CAST(GWP AS DOUBLE) AS FACTOR_INDIRECT
            FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR
            WHERE GWP IS NOT NULL
                AND GWP != 0
            EXCEPT
                SELECT *
                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_BP';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_UP', V_QUERY_STR);

    --能辅产品列表
    SET V_QUERY_STR = '
    SELECT DISTINCT PROC_KEY,
                    PRODUCT_CODE
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ENERGY_PRODUCT', V_QUERY_STR);

    --主工序产品列表
    SET V_QUERY_STR = '
        SELECT DISTINCT MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PRODUCT_CODE
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'MAIN_PRODUCT', V_QUERY_STR);

    --原材料数据
    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_NAME IN (''能源'' , ''原材料'' , ''辅助材料'')
        AND ITEM_CODE IN (SELECT DISTINCT PRODUCT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_PRODUCT)
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE_ENERGY', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT A.*
        FROM (SELECT *
              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
              WHERE TYPE_NAME IN (''能源'', ''原材料'', ''辅助材料'')) A
                 INNER JOIN (SELECT DISTINCT MAT_TRACK_NO, PRODUCT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAIN_PRODUCT) B
                            ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE_MAIN', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_NAME IN (''能源'', ''原材料'', ''辅助材料'')
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
        WHERE TYPE_NAME IN (''能源'' , ''原材料'' , ''辅助材料'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESOURCE_ALL', V_QUERY_STR);

    --副产品数据
    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_NAME IN (''副产品'', ''固废'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'BY_PRODUCT', V_QUERY_STR);

    --产品副产品数据
    SET V_QUERY_STR = '
        SELECT *
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE TYPE_NAME IN (''产品'', ''副产品'', ''固废'')
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PRODUCT_BY_PRODUCT', V_QUERY_STR);

    --本工序结果
    SET V_QUERY_STR = '
        SELECT A.*,
            COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0)     AS C1,
            COALESCE(A.UNIT_COST * C.FACTOR_INDIRECT, 0)   AS C2,
            COALESCE(A.UNIT_COST * D.FACTOR_INDIRECT, 0)   AS C3,
            0                                              AS C4,
            COALESCE(A.UNIT_COST * E.FACTOR_TRANSPORT, 0)  AS C5,
            B.FACTOR_DIRECT                                AS FACTOR_DIRECT,
            COALESCE(C.FACTOR_INDIRECT, D.FACTOR_INDIRECT) AS FACTOR_INDIRECT,
            E.FACTOR_TRANSPORT                             AS FACTOR_TRANSPORT,
            ''DIRECT''                                     AS FLAG
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE A
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
                           ON A.ITEM_CODE = B.ITEM_CODE
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_BP C
                           ON A.ITEM_CODE = C.ITEM_CODE
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_UP D
                           ON A.ITEM_CODE = D.ITEM_CODE
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT E
                           ON A.ITEM_CODE = E.ITEM_CODE
        UNION
        SELECT A.*,
               COALESCE(-A.UNIT_COST * FACTOR_DIRECT, 0)     AS C1,
               0                                             AS C2,
               0                                             AS C3,
               COALESCE(-A.UNIT_COST * C.FACTOR_INDIRECT, 0) AS C4,
               0                                             AS C5,
               B.FACTOR_DIRECT                               AS FACTOR_DIRECT,
               C.FACTOR_INDIRECT                             AS FACTOR_INDIRECT,
               NULL                                          AS FACTOR_TRANSPORT,
               ''DIRECT''                                    AS FLAG
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PRODUCT_BY_PRODUCT A
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
                           ON A.ITEM_CODE = B.ITEM_CODE
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_INDIRECT C
                           ON A.ITEM_CODE = C.ITEM_CODE
        UNION
        SELECT A.*,
               COALESCE(A.UNIT_COST * B.C1_DIRECT, 0) AS C1,
               COALESCE(A.UNIT_COST * B.C2_BP, 0)     AS C2,
               COALESCE(A.UNIT_COST * B.C3_OUT, 0)    AS C3,
               COALESCE(A.UNIT_COST * B.C4_BP_NEG, 0) AS C4,
               COALESCE(A.UNIT_COST * B.C5_TRANS, 0)  AS C5,
               NULL                                   AS FACTOR_DIRECT,
               NULL                                   AS FACTOR_INDIRECT,
               NULL                                   AS FACTOR_TRANSPORT,
               ''UPSTREAM''                           AS FLAG
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY A
                 LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT B
                           ON A.ITEM_CODE = B.PRODUCT_CODE
        UNION
        SELECT A.*,
               COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0) AS C1,
               0                                          AS C2,
               0                                          AS C3,
               0                                          AS C4,
               0                                          AS C5,
               B.FACTOR_DIRECT                            AS FACTOR_DIRECT,
               NULL                                       AS FACTOR_INDIRECT,
               NULL                                       AS FACTOR_TRANSPORT,
               ''DIRECT''                                 AS FLAG
        FROM (SELECT *
              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY
              UNION
              SELECT *
              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
                 INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
                            ON A.ITEM_CODE = B.ITEM_CODE
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT_DIST', V_QUERY_STR);

    --遍历结转工序
    SET V_QUERY_STR = 'SELECT DISTINCT MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PREV_FAMILY_CODE, MAT_SEQ_NO
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ
        ORDER BY MAT_TRACK_NO, FAMILY_CODE, MAT_SEQ_NO';
    PREPARE S1 FROM V_QUERY_STR;

    OPEN CURSOR1;

    FETCH_LOOP:
    LOOP
        FETCH CURSOR1 INTO V_MAT_TRACK_NO, V_FAMILY_CODE, V_UNIT_CODE, V_PREV_FAMILY_CODE, V_MAT_SEQ_NO;

        IF DONE = 1 THEN
            LEAVE FETCH_LOOP;
        END IF;

--         CALL DBMS_OUTPUT.PUT_LINE(V_MAT_TRACK_NO || ' ' || V_FAMILY_CODE || ' ' || V_UNIT_CODE);

        IF V_PREV_FAMILY_CODE = '00' THEN
            SET V_QUERY_STR = '
            SELECT COUNT(*)
            FROM (SELECT *
                  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN
                  WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                    AND FAMILY_CODE = ''' || V_FAMILY_CODE || '''
                    AND UNIT_CODE = ''' || V_UNIT_CODE || ''') A
                     JOIN (SELECT FAMILY_CODE,
                                  UNIT_CODE,
                                  PRODUCT_CODE,
                                  SUM(C1) AS C1,
                                  SUM(C2) AS C2,
                                  SUM(C3) AS C3,
                                  SUM(C4) AS C4,
                                  SUM(C5) AS C5
                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
                           WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                           GROUP BY FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B
                           ON A.ITEM_CODE = B.PRODUCT_CODE';
            CALL BG00MAC102.P_EXEC_INTO(V_QUERY_STR, V_ROW_COUNT);
            IF V_ROW_COUNT > 0 THEN
                SET V_QUERY_STR = '
                INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
                SELECT A.*,
                       A.UNIT_COST * B.C1,
                       A.UNIT_COST * B.C2,
                       A.UNIT_COST * B.C3,
                       A.UNIT_COST * B.C4,
                       A.UNIT_COST * B.C5,
                       NULL,
                       NULL,
                       NULL,
                       ''UPSTREAM''
                FROM (SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN
                      WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                      AND FAMILY_CODE = ''' || V_FAMILY_CODE || '''
                      AND UNIT_CODE = ''' || V_UNIT_CODE || ''') A
                         JOIN (SELECT FAMILY_CODE,
                                      UNIT_CODE,
                                      PRODUCT_CODE,
                                      SUM(C1) AS C1,
                                      SUM(C2) AS C2,
                                      SUM(C3) AS C3,
                                      SUM(C4) AS C4,
                                      SUM(C5) AS C5
                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
                               WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                               GROUP BY FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B
                               ON A.ITEM_CODE = B.PRODUCT_CODE
                ';
                PREPARE stmt FROM V_QUERY_STR;
                EXECUTE stmt;
            END IF;
        ELSE
            SET V_QUERY_STR = '
            SELECT COUNT(*)
            FROM (SELECT *
                  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN
                  WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                    AND FAMILY_CODE = ''' || V_FAMILY_CODE || '''
                    AND UNIT_CODE = ''' || V_UNIT_CODE || ''') A
                     JOIN (SELECT FAMILY_CODE,
                                  UNIT_CODE,
                                  PRODUCT_CODE,
                                  SUM(C1) AS C1,
                                  SUM(C2) AS C2,
                                  SUM(C3) AS C3,
                                  SUM(C4) AS C4,
                                  SUM(C5) AS C5
                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
                           WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                           AND FAMILY_CODE = ''' || V_PREV_FAMILY_CODE || '''
                           GROUP BY FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B
                           ON A.ITEM_CODE = B.PRODUCT_CODE';
            CALL BG00MAC102.P_EXEC_INTO(V_QUERY_STR, V_ROW_COUNT);
            IF V_ROW_COUNT > 0 THEN
                SET V_QUERY_STR = '
                INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
                SELECT A.*,
                       A.UNIT_COST * B.C1,
                       A.UNIT_COST * B.C2,
                       A.UNIT_COST * B.C3,
                       A.UNIT_COST * B.C4,
                       A.UNIT_COST * B.C5,
                       NULL,
                       NULL,
                       NULL,
                       ''UPSTREAM''
                FROM (SELECT *
                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN
                      WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                        AND FAMILY_CODE = ''' || V_FAMILY_CODE || '''
                        AND UNIT_CODE = ''' || V_UNIT_CODE || ''') A
                         JOIN (SELECT FAMILY_CODE,
                                      UNIT_CODE,
                                      PRODUCT_CODE,
                                      SUM(C1) AS C1,
                                      SUM(C2) AS C2,
                                      SUM(C3) AS C3,
                                      SUM(C4) AS C4,
                                      SUM(C5) AS C5
                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
                               WHERE MAT_TRACK_NO = ''' || V_MAT_TRACK_NO || '''
                               AND FAMILY_CODE = ''' || V_PREV_FAMILY_CODE || '''
                               GROUP BY FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B
                               ON A.ITEM_CODE = B.PRODUCT_CODE
                ';
                PREPARE stmt FROM V_QUERY_STR;
                EXECUTE stmt;
            END IF;
        END IF;
    END LOOP FETCH_LOOP;

    CLOSE CURSOR1;

    DELETE
    FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT
    WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
      AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
      AND COMPANY_CODE = V_COMPANY_CODE;

    DELETE
    FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST
    WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
      AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
      AND COMPANY_CODE = V_COMPANY_CODE;

    SET V_QUERY_STR = '
    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST
    (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, BATCH_NUMBER, MAIN_CAT_BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO,
     FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE,
     UNITM_AC, PRODUCT_VALUE, UNIT_COST, C1, C2, C3, C4, C5, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG,
     REC_CREATOR, REC_CREATE_TIME, REC_REVISOR, REC_REVISE_TIME)
    SELECT HEX(RAND()),
           SUBCLASS_TAB_NAME,
           ''' || V_COMPANY_CODE || ''',
           BATCH_NUMBER,
           ''' || V_MAX_BATCH_NUMBER || ''',
           MAT_NO,
           MAT_TRACK_NO,
           MAT_SEQ_NO,
           FAMILY_CODE,
           UNIT_CODE,
           UNIT_NAME,
           PRODUCT_CODE,
           PRODUCT_NAME,
           TYPE_CODE,
           TYPE_NAME,
           ITEM_CODE,
           ITEM_NAME,
           VALUE,
           UNITM_AC,
           PRODUCT_VALUE,
           UNIT_COST,
           C1,
           C2,
           C3,
           C4,
           C5,
           FACTOR_DIRECT,
           FACTOR_INDIRECT,
           FACTOR_TRANSPORT,
           FLAG,
           NULL,
           TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''),
           NULL,
           NULL
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST';

    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    SET V_QUERY_STR = '
    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT
    (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, BATCH_NUMBER,
     MAIN_CAT_BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO,
     FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME,
     PRODUCT_VALUE, C1, C2, C3, C4, C5, C_INSITE, C_OUTSITE,
     C_CYCLE, REC_CREATOR, REC_CREATE_TIME, REC_REVISOR,
     REC_REVISE_TIME)
    SELECT HEX(RAND()),
           SUBCLASS_TAB_NAME,
           ''' || V_COMPANY_CODE || ''',
           BATCH_NUMBER,
           ''' || V_MAX_BATCH_NUMBER || ''',
           MAT_NO,
           MAT_TRACK_NO,
           MAT_SEQ_NO,
           FAMILY_CODE,
           UNIT_CODE,
           UNIT_NAME,
           PRODUCT_CODE,
           PRODUCT_NAME,
           PRODUCT_VALUE,
           SUM(C1),
           SUM(C2),
           SUM(C3),
           SUM(C4),
           SUM(C5),
           SUM(C1) + SUM(C2),
           SUM(C3) + SUM(C4) + SUM(C5),
           SUM(C1) + SUM(C2) + SUM(C3) + SUM(C4) + SUM(C5),
           NULL,
           TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''),
           NULL,
           NULL
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
    GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
             PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE
    ORDER BY MAT_TRACK_NO, FAMILY_CODE, MAT_SEQ_NO';

    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    --     DELETE
--     FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_SEQ_DIST_UNIT_COST
--     WHERE BATCH_NUMBER = V_MAX_BATCH_NUMBER
--       AND COMPANY_CODE = V_COMPANY_CODE;
--
--     SET V_QUERY_STR = '
--     CREATE TABLE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_UNIT_COST
--     (
--         PROC_KEY        VARCHAR(64),
--         PROC_CODE       VARCHAR(64),
--         PROC_NAME       VARCHAR(256),
--         PRODUCT_CODE    VARCHAR(64),
--         PRODUCT_NAME    VARCHAR(128),
--         ITEM_CODE       VARCHAR(64),
--         ITEM_NAME       VARCHAR(64),
--         SOURCE_PROC_KEY VARCHAR(64),
--         UNIT_COST       DECIMAL(24, 10)
--     )';
--
--     PREPARE stmt FROM V_QUERY_STR;
--     EXECUTE stmt;

--     FOR cursor1
--         AS
--         SELECT proc_key,
--                PROC_SEQ
--         FROM BG00MAC102.T_ADS_FACT_LCA_MAIN_CAT_PROC_SEQ
--         WHERE YEAR = QUR_YEAR
--           AND COMPANY_CODE = V_COMPANY_CODE
--         ORDER BY PROC_SEQ
--         DO
--             SET v_proc_key = cursor1.proc_key;
--             SET v_PROC_SEQ = cursor1.PROC_SEQ;
--
--             SET V_QUERY_STR = '
--             INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_UNIT_COST(PROC_KEY,
--                                                                         PROC_CODE,
--                                                                         PROC_NAME,
--                                                                         PRODUCT_CODE,
--                                                                         PRODUCT_NAME,
--                                                                         ITEM_CODE,
--                                                                         ITEM_NAME,
--                                                                         SOURCE_PROC_KEY,
--                                                                         UNIT_COST)
--             WITH ENERGY_CONSUMPTION AS (SELECT A.PROC_KEY,
--                                                A.PROC_CODE,
--                                                A.PROC_NAME,
--                                                A.PRODUCT_CODE,
--                                                A.PRODUCT_NAME,
--                                                A.ITEM_CODE,
--                                                A.ITEM_NAME,
--                                                B.PROC_KEY AS SOURCE_PROC_KEY,
--                                                A.UNIT_COST
--                                         FROM (SELECT *
--                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_DATA) A
--                                                  INNER JOIN
--                                              ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_PRODUCT B
--                                              ON
--                                                  A.ITEM_CODE = B.PRODUCT_CODE),
--                  MAIN_CONSUMPTION AS (SELECT A.PROC_KEY,
--                                              A.PROC_CODE,
--                                              A.PROC_NAME,
--                                              A.PRODUCT_CODE,
--                                              A.PRODUCT_NAME,
--                                              A.ITEM_CODE,
--                                              A.ITEM_NAME,
--                                              B.PROC_KEY AS SOURCE_PROC_KEY,
--                                              A.UNIT_COST
--                                       FROM (SELECT *
--                                             FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_DATA) A
--                                                INNER JOIN
--                                            ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAIN_PRODUCT B
--                                            ON
--                                                A.ITEM_CODE = B.PRODUCT_CODE)
--             SELECT PROC_KEY,
--                    PROC_CODE,
--                    PROC_NAME,
--                    PRODUCT_CODE,
--                    PRODUCT_NAME,
--                    ITEM_CODE,
--                    ITEM_NAME,
--                    SOURCE_PROC_KEY,
--                    SUM(UNIT_COST) AS UNIT_COST
--             FROM (SELECT *
--                   FROM ENERGY_CONSUMPTION
--                   WHERE PROC_KEY = ''' || V_PROC_KEY || '''
--                   UNION
--                   SELECT *
--                   FROM MAIN_CONSUMPTION
--                   WHERE PROC_KEY = ''' || V_PROC_KEY || '''
--                   UNION
--                   (SELECT B.PROC_KEY,
--                           B.PROC_CODE,
--                           B.PROC_NAME,
--                           B.PRODUCT_CODE,
--                           B.PRODUCT_NAME,
--                           A.ITEM_CODE,
--                           A.ITEM_NAME,
--                           A.SOURCE_PROC_KEY,
--                           A.UNIT_COST * B.UNIT_COST
--                    FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST_UNIT_COST) A
--                             INNER JOIN
--                         (SELECT *
--                          FROM MAIN_CONSUMPTION
--                          WHERE PROC_KEY = ''' || V_PROC_KEY || ''') B
--                         ON A.PROC_KEY = B.SOURCE_PROC_KEY))
--             GROUP BY PROC_KEY,
--                      PROC_CODE,
--                      PROC_NAME,
--                      PRODUCT_CODE,
--                      PRODUCT_NAME,
--                      ITEM_CODE,
--                      ITEM_NAME,
--                      SOURCE_PROC_KEY';
--
--             PREPARE stmt FROM V_QUERY_STR;
--             EXECUTE stmt;
--
--         END FOR;

    ------------------------------------处理逻辑(结束)------------------------------------

    --删除生成的临时表
    CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
    COMMIT;


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

