SET CURRENT SCHEMA = BG00MAC102;

SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,G0MAC102;

CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC (
    IN V_COMPANY_CODE	VARCHAR(4),
    IN V_START_DATE	VARCHAR(6),
    IN V_END_DATE	VARCHAR(6),
    IN V_FACTOR_YEAR	VARCHAR(4),
    IN V_SUBCLASS_TAB_NAME	VARCHAR(100),
    IN V_SUB_BATCH_NUMBER	VARCHAR(100) )
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    CALLED ON NULL INPUT
    INHERIT SPECIAL REGISTERS
    OLD SAVEPOINT LEVEL
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
    DECLARE V_QUERY_STR CLOB(1 M); --查询SQL
    DECLARE V_TMP_NAME VARCHAR(128);
    --完整的临时表名
    ------------------------------------存储过程变量定义---------------------------------

    DECLARE QUR_YEAR VARCHAR(4);
    DECLARE V_MAX_BATCH_NUMBER VARCHAR(100);
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
            (A.ITEM_CODE, A.CUSTOMS_TRANS_VALUE / 1000 * 0.00667220376406485),
            (A.ITEM_CODE, A.TRAIN_TRANS_VALUE / 1000 * 0.0438528213459938),
            (A.ITEM_CODE, A.TRUCK_CAR_TRANS_VALUE / 1000 * 0.138092140707126),
            (A.ITEM_CODE, A.RIVER_CAR_TRANS_VALUE / 1000 * 0.0487214598443683)
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
--     SET V_QUERY_STR = '
--         SELECT A.*,
--             COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0)     AS C1,
--             COALESCE(A.UNIT_COST * C.FACTOR_INDIRECT, 0)   AS C2,
--             COALESCE(A.UNIT_COST * D.FACTOR_INDIRECT, 0)   AS C3,
--             0                                              AS C4,
--             COALESCE(A.UNIT_COST * E.FACTOR_TRANSPORT, 0)  AS C5,
--             B.FACTOR_DIRECT                                AS FACTOR_DIRECT,
--             COALESCE(C.FACTOR_INDIRECT, D.FACTOR_INDIRECT) AS FACTOR_INDIRECT,
--             E.FACTOR_TRANSPORT                             AS FACTOR_TRANSPORT,
--             ''DIRECT''                                     AS FLAG
--         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE A
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
--                            ON A.ITEM_CODE = B.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_BP C
--                            ON A.ITEM_CODE = C.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_UP D
--                            ON A.ITEM_CODE = D.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT E
--                            ON A.ITEM_CODE = E.ITEM_CODE
--         UNION
--         SELECT A.*,
--                COALESCE(-A.UNIT_COST * FACTOR_DIRECT, 0)     AS C1,
--                0                                             AS C2,
--                0                                             AS C3,
--                COALESCE(-A.UNIT_COST * C.FACTOR_INDIRECT, 0) AS C4,
--                0                                             AS C5,
--                B.FACTOR_DIRECT                               AS FACTOR_DIRECT,
--                C.FACTOR_INDIRECT                             AS FACTOR_INDIRECT,
--                NULL                                          AS FACTOR_TRANSPORT,
--                ''DIRECT''                                    AS FLAG
--         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PRODUCT_BY_PRODUCT A
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
--                            ON A.ITEM_CODE = B.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_INDIRECT C
--                            ON A.ITEM_CODE = C.ITEM_CODE
--         UNION
--         SELECT A.*,
--                COALESCE(A.UNIT_COST * B.C1_DIRECT, 0) AS C1,
--                COALESCE(A.UNIT_COST * B.C2_BP, 0)     AS C2,
--                COALESCE(A.UNIT_COST * B.C3_OUT, 0)    AS C3,
--                COALESCE(A.UNIT_COST * B.C4_BP_NEG, 0) AS C4,
--                COALESCE(A.UNIT_COST * B.C5_TRANS, 0)  AS C5,
--                NULL                                   AS FACTOR_DIRECT,
--                NULL                                   AS FACTOR_INDIRECT,
--                NULL                                   AS FACTOR_TRANSPORT,
--                ''UPSTREAM''                           AS FLAG
--         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY A
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT B
--                            ON A.ITEM_CODE = B.PRODUCT_CODE
--         UNION
--         SELECT A.*,
--                COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0) AS C1,
--                0                                          AS C2,
--                0                                          AS C3,
--                0                                          AS C4,
--                0                                          AS C5,
--                B.FACTOR_DIRECT                            AS FACTOR_DIRECT,
--                NULL                                       AS FACTOR_INDIRECT,
--                NULL                                       AS FACTOR_TRANSPORT,
--                ''DIRECT''                                 AS FLAG
--         FROM (SELECT *
--               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY
--               UNION
--               SELECT *
--               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                  INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
--                             ON A.ITEM_CODE = B.ITEM_CODE
--     ';
--     CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT_DIST', V_QUERY_STR);

    SET v_query_str = '
    CREATE TABLE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL
    (
        SUBCLASS_TAB_NAME     VARCHAR(64),
        BATCH_NUMBER          VARCHAR(64),
        MAT_NO                VARCHAR(64),
        MAT_TRACK_NO          VARCHAR(64),
        MAT_SEQ_NO            BIGINT,
        FAMILY_CODE           VARCHAR(100),
        UNIT_CODE             VARCHAR(100),
        UNIT_NAME             VARCHAR(256),
        PRODUCT_CODE          VARCHAR(100),
        PRODUCT_NAME          VARCHAR(256),
        TYPE_CODE             VARCHAR(20),
        TYPE_NAME             VARCHAR(64),
        ITEM_CODE             VARCHAR(100),
        ITEM_NAME             VARCHAR(256),
        VALUE                 DECIMAL(27, 6),
        UNITM_AC              VARCHAR(64),
        PRODUCT_VALUE         DECIMAL(27, 6),
        UNIT_COST             DOUBLE,
        C1                    DOUBLE,
        C2                    DOUBLE,
        C3                    DOUBLE,
        C4                    DOUBLE,
        C5                    DOUBLE,
        FACTOR_DIRECT         DOUBLE,
        FACTOR_INDIRECT       DOUBLE,
        FACTOR_TRANSPORT      DOUBLE,
        FLAG                  VARCHAR(32)
    )';

    PREPARE stmt FROM v_query_str;
    EXECUTE stmt;

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
            ''RESOURCE''                                   AS FLAG
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
               ''DEDUCTION''                                 AS FLAG
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
               ''ENERGY''                             AS FLAG
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
                            ON A.ITEM_CODE = B.ITEM_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT_DIST_CURRENT', V_QUERY_STR);


    --     SET V_QUERY_STR = '
--         SELECT A.*,
--             A.FAMILY_CODE                                  AS SOURCE_FAMILY_CODE,
--             A.UNIT_CODE                                    AS SOURCE_UNIT_CODE,
--             A.UNIT_NAME                                    AS SOURCE_UNIT_NAME,
--             COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0)     AS C1,
--             COALESCE(A.UNIT_COST * C.FACTOR_INDIRECT, 0)   AS C2,
--             COALESCE(A.UNIT_COST * D.FACTOR_INDIRECT, 0)   AS C3,
--             0                                              AS C4,
--             COALESCE(A.UNIT_COST * E.FACTOR_TRANSPORT, 0)  AS C5,
--             B.FACTOR_DIRECT                                AS FACTOR_DIRECT,
--             COALESCE(C.FACTOR_INDIRECT, D.FACTOR_INDIRECT) AS FACTOR_INDIRECT,
--             E.FACTOR_TRANSPORT                             AS FACTOR_TRANSPORT,
--             ''THIS_RESOURCE''                                       AS FLAG
--         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE A
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
--                            ON A.ITEM_CODE = B.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_BP C
--                            ON A.ITEM_CODE = C.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_UP D
--                            ON A.ITEM_CODE = D.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT E
--                            ON A.ITEM_CODE = E.ITEM_CODE
--         UNION
--         SELECT A.*,
--                A.FAMILY_CODE                                 AS SOURCE_FAMILY_CODE,
--                A.UNIT_CODE                                   AS SOURCE_UNIT_CODE,
--                A.UNIT_NAME                                   AS SOURCE_UNIT_NAME,
--                COALESCE(-A.UNIT_COST * FACTOR_DIRECT, 0)     AS C1,
--                0                                             AS C2,
--                0                                             AS C3,
--                COALESCE(-A.UNIT_COST * C.FACTOR_INDIRECT, 0) AS C4,
--                0                                             AS C5,
--                B.FACTOR_DIRECT                               AS FACTOR_DIRECT,
--                C.FACTOR_INDIRECT                             AS FACTOR_INDIRECT,
--                NULL                                          AS FACTOR_TRANSPORT,
--                ''THIS_DEDUCTION''                                      AS FLAG
--         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PRODUCT_BY_PRODUCT A
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
--                            ON A.ITEM_CODE = B.ITEM_CODE
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_INDIRECT C
--                            ON A.ITEM_CODE = C.ITEM_CODE
--         UNION
--         SELECT A.*,
--                A.FAMILY_CODE                          AS SOURCE_FAMILY_CODE,
--                A.UNIT_CODE                            AS SOURCE_UNIT_CODE,
--                A.UNIT_NAME                            AS SOURCE_UNIT_NAME,
--                COALESCE(A.UNIT_COST * B.C1_DIRECT, 0) AS C1,
--                COALESCE(A.UNIT_COST * B.C2_BP, 0)     AS C2,
--                COALESCE(A.UNIT_COST * B.C3_OUT, 0)    AS C3,
--                COALESCE(A.UNIT_COST * B.C4_BP_NEG, 0) AS C4,
--                COALESCE(A.UNIT_COST * B.C5_TRANS, 0)  AS C5,
--                NULL                                   AS FACTOR_DIRECT,
--                NULL                                   AS FACTOR_INDIRECT,
--                NULL                                   AS FACTOR_TRANSPORT,
--                ''THIS_ENERGY''                             AS FLAG
--         FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY A
--                  LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT B
--                            ON A.ITEM_CODE = B.PRODUCT_CODE
--         UNION
--         SELECT A.*,
--                A.FAMILY_CODE                              AS SOURCE_FAMILY_CODE,
--                A.UNIT_CODE                                AS SOURCE_UNIT_CODE,
--                A.UNIT_NAME                                AS SOURCE_UNIT_NAME,
--                COALESCE(A.UNIT_COST * B.FACTOR_DIRECT, 0) AS C1,
--                0                                          AS C2,
--                0                                          AS C3,
--                0                                          AS C4,
--                0                                          AS C5,
--                B.FACTOR_DIRECT                            AS FACTOR_DIRECT,
--                NULL                                       AS FACTOR_INDIRECT,
--                NULL                                       AS FACTOR_TRANSPORT,
--                ''THIS_DIRECT_UPSTREAM''                                 AS FLAG
--         FROM (SELECT *
--               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ENERGY
--               UNION
--               SELECT *
--               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                  INNER JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_DIRECT B
--                             ON A.ITEM_CODE = B.ITEM_CODE
--     ';
--     CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT_DIST_PREV', V_QUERY_STR);

    --     SET V_QUERY_STR = '
--         SELECT SUBCLASS_TAB_NAME,
--                BATCH_NUMBER,
--                MAT_NO,
--                MAT_TRACK_NO,
--                MAT_SEQ_NO,
--                FAMILY_CODE,
--                UNIT_CODE,
--                UNIT_NAME,
--                PRODUCT_CODE,
--                PRODUCT_NAME,
--                PRODUCT_VALUE,
--                FAMILY_CODE AS SOURCE_FAMILY_CODE,
--                UNIT_CODE   AS SOURCE_UNIT_CODE,
--                UNIT_NAME   AS SOURCE_UNIT_NAME,
--                TYPE_CODE,
--                TYPE_NAME,
--                SUM(C1)     AS C1,
--                SUM(C2)     AS C2,
--                SUM(C3)     AS C3,
--                SUM(C4)     AS C4,
--                SUM(C5)     AS C5,
--                ''CURRENT'' AS FLAG
--         FROM (SELECT SUBCLASS_TAB_NAME,
--                      BATCH_NUMBER,
--                      MAT_NO,
--                      MAT_TRACK_NO,
--                      MAT_SEQ_NO,
--                      FAMILY_CODE,
--                      UNIT_CODE,
--                      UNIT_NAME,
--                      PRODUCT_CODE,
--                      PRODUCT_NAME,
--                      PRODUCT_VALUE,
--                      TYPE_CODE,
--                      TYPE_NAME,
--                      C1,
--                      C2,
--                      C3,
--                      C4,
--                      C5
--               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST)
--               GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
--                        PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, TYPE_CODE, TYPE_NAME
--     ';
--     CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RESULT_DIST_PREV_SUM', V_QUERY_STR);


    --建立上游还原临时表
--     SET v_query_str = '
--     CREATE TABLE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_UPSTREAM_COST
--     (
--         SUBCLASS_TAB_NAME  VARCHAR(64),
--         BATCH_NUMBER       VARCHAR(64),
--         MAT_NO             VARCHAR(64),
--         MAT_TRACK_NO       VARCHAR(64),
--         MAT_SEQ_NO         BIGINT,
--         FAMILY_CODE        VARCHAR(100),
--         UNIT_CODE          VARCHAR(100),
--         UNIT_NAME          VARCHAR(256),
--         PRODUCT_CODE       VARCHAR(100),
--         PRODUCT_NAME       VARCHAR(256),
--         TYPE_CODE          VARCHAR(20),
--         TYPE_NAME          VARCHAR(200),
--         ITEM_CODE          VARCHAR(100),
--         ITEM_NAME          VARCHAR(256),
--         VALUE              DECIMAL(27, 6),
--         UNITM_AC           VARCHAR(20),
--         PRODUCT_VALUE      DECIMAL(27, 6),
--         UNIT_COST          DOUBLE,
--         SOURCE_FAMILY_CODE VARCHAR(100),
--         SOURCE_UNIT_CODE   VARCHAR(100),
--         SOURCE_UNIT_NAME   VARCHAR(256)
--     )';
--
--     PREPARE stmt FROM v_query_str;
--     EXECUTE stmt;

    --遍历结转工序

    SET V_QUERY_STR = '
        SELECT DISTINCT MAX(RANK) AS MAX_RANK
		FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
    ';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    SET V_RANK = 1;

    WHILE V_RANK <= V_MAX_RANK
        DO
--             SET V_QUERY_STR = '
--                 SELECT COUNT(*)
--                 FROM (SELECT *
--                     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN A
--                        INNER JOIN (SELECT *
--                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
--                              WHERE RANK = ' || V_RANK || ') B
--                             ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                 AND A.FAMILY_CODE = B.FAMILY_CODE
--                                 AND A.UNIT_CODE = B.UNIT_CODE)';
--             CALL BG00MAC102.P_EXEC_INTO(V_QUERY_STR, V_ROW_COUNT);

--             IF V_ROW_COUNT > 0 THEN
--                 SET V_QUERY_STR = '
--                     INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
--                     SELECT A.*,
--                            A.UNIT_COST * B.C1,
--                            A.UNIT_COST * B.C2,
--                            A.UNIT_COST * B.C3,
--                            A.UNIT_COST * B.C4,
--                            A.UNIT_COST * B.C5,
--                            NULL,
--                            NULL,
--                            NULL,
--                            ''UPSTREAM''
--                     FROM (SELECT A.*
--                           FROM (SELECT *
--                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                                    INNER JOIN (SELECT *
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
--                                          WHERE RANK = ' || V_RANK || ') B
--                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                             AND A.FAMILY_CODE = B.FAMILY_CODE
--                                             AND A.UNIT_CODE = B.UNIT_CODE) A
--                              INNER JOIN (SELECT MAT_TRACK_NO,
--                                           FAMILY_CODE,
--                                           UNIT_CODE,
--                                           PRODUCT_CODE,
--                                           SUM(C1) AS C1,
--                                           SUM(C2) AS C2,
--                                           SUM(C3) AS C3,
--                                           SUM(C4) AS C4,
--                                           SUM(C5) AS C5
--                                    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
--                                    GROUP BY MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) B
--                                   ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE';
--                 PREPARE stmt FROM V_QUERY_STR;
--                 EXECUTE stmt;
--
--                 --                 SET V_QUERY_STR = '
-- --                     INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV
-- --                     SELECT A.SUBCLASS_TAB_NAME,
-- --                            A.BATCH_NUMBER,
-- --                            A.MAT_NO,
-- --                            A.MAT_TRACK_NO,
-- --                            A.MAT_SEQ_NO,
-- --                            A.FAMILY_CODE,
-- --                            A.UNIT_CODE,
-- --                            A.UNIT_NAME,
-- --                            A.PRODUCT_CODE,
-- --                            A.PRODUCT_NAME,
-- --                            B.TYPE_CODE,
-- --                            B.TYPE_NAME,
-- --                            B.ITEM_CODE,
-- --                            B.ITEM_NAME,
-- --                            A.UNIT_COST * B.VALUE     AS VALUE,
-- --                            B.UNITM_AC,
-- --                            A.PRODUCT_VALUE,
-- --                            A.UNIT_COST * B.UNIT_COST AS UNIT_COST,
-- --                            B.SOURCE_FAMILY_CODE      AS SOURCE_FAMILY_CODE,
-- --                            B.SOURCE_UNIT_CODE        AS SOURCE_UNIT_CODE,
-- --                            B.SOURCE_UNIT_NAME        AS SOURCE_UNIT_NAME,
-- --                            A.UNIT_COST * B.C1        AS C1,
-- --                            A.UNIT_COST * B.C2        AS C2,
-- --                            A.UNIT_COST * B.C3        AS C3,
-- --                            A.UNIT_COST * B.C4        AS C4,
-- --                            A.UNIT_COST * B.C5        AS C5,
-- --                            B.FACTOR_DIRECT,
-- --                            B.FACTOR_INDIRECT,
-- --                            B.FACTOR_TRANSPORT,
-- --                            CASE
-- --                                WHEN B.FLAG = ''PREV_SUM'' THEN ''PREV_SUM''
-- --                                ELSE ''PROCESS'' END    AS FLAG
-- --                     FROM (SELECT A.*
-- --                           FROM (SELECT *
-- --                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
-- --                                    INNER JOIN (SELECT *
-- --                                                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
-- --                                                WHERE RANK = ' || V_RANK || '
-- --                                                  AND SUM_FLAG = 0) B
-- --                                               ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
-- --                                                   AND A.FAMILY_CODE = B.FAMILY_CODE
-- --                                                   AND A.UNIT_CODE = B.UNIT_CODE) A
-- --                              INNER JOIN (SELECT *
-- --                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV) B
-- --                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE
-- --                     UNION
-- --                     SELECT A.SUBCLASS_TAB_NAME,
-- --                            A.BATCH_NUMBER,
-- --                            A.MAT_NO,
-- --                            A.MAT_TRACK_NO,
-- --                            A.MAT_SEQ_NO,
-- --                            A.FAMILY_CODE,
-- --                            A.UNIT_CODE,
-- --                            A.UNIT_NAME,
-- --                            A.PRODUCT_CODE,
-- --                            A.PRODUCT_NAME,
-- --                            A.TYPE_CODE,
-- --                            A.TYPE_NAME,
-- --                            A.ITEM_CODE,
-- --                            A.ITEM_NAME,
-- --                            A.VALUE,
-- --                            A.UNITM_AC,
-- --                            A.PRODUCT_VALUE,
-- --                            A.UNIT_COST,
-- --                            B.FAMILY_CODE      AS SOURCE_FAMILY_CODE,
-- --                            B.UNIT_CODE        AS SOURCE_UNIT_CODE,
-- --                            B.UNIT_NAME        AS SOURCE_UNIT_NAME,
-- --                            A.UNIT_COST * B.C1 AS C1,
-- --                            A.UNIT_COST * B.C2 AS C2,
-- --                            A.UNIT_COST * B.C3 AS C3,
-- --                            A.UNIT_COST * B.C4 AS C4,
-- --                            A.UNIT_COST * B.C5 AS C5,
-- --                            NULL,
-- --                            NULL,
-- --                            NULL,
-- --                            ''PREV_SUM''
-- --                     FROM (SELECT A.*
-- --                           FROM (SELECT *
-- --                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
-- --                                    INNER JOIN (SELECT *
-- --                                                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
-- --                                                WHERE RANK = ' || V_RANK || '
-- --                                                  AND SUM_FLAG = 1) B
-- --                                               ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
-- --                                                   AND A.FAMILY_CODE = B.FAMILY_CODE
-- --                                                   AND A.UNIT_CODE = B.UNIT_CODE) A
-- --                              INNER JOIN (SELECT SUBCLASS_TAB_NAME,
-- --                                                 BATCH_NUMBER,
-- --                                                 MAT_NO,
-- --                                                 MAT_TRACK_NO,
-- --                                                 MAT_SEQ_NO,
-- --                                                 FAMILY_CODE,
-- --                                                 UNIT_CODE,
-- --                                                 UNIT_NAME,
-- --                                                 PRODUCT_CODE,
-- --                                                 PRODUCT_NAME,
-- --                                                 PRODUCT_VALUE,
-- --                                                 SUM(C1) AS C1,
-- --                                                 SUM(C2) AS C2,
-- --                                                 SUM(C3) AS C3,
-- --                                                 SUM(C4) AS C4,
-- --                                                 SUM(C5) AS C5
-- --                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV
-- --                                          GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE,
-- --                                                   UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE) B
-- --                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE';
-- --                 PREPARE stmt FROM V_QUERY_STR;
-- --                 EXECUTE stmt;
--
--                 --                 SET V_QUERY_STR = '
-- --                     INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV_SUM
-- --                     SELECT A.SUBCLASS_TAB_NAME,
-- --                            A.BATCH_NUMBER,
-- --                            A.MAT_NO,
-- --                            A.MAT_TRACK_NO,
-- --                            A.MAT_SEQ_NO,
-- --                            A.FAMILY_CODE,
-- --                            A.UNIT_CODE,
-- --                            A.UNIT_NAME,
-- --                            A.PRODUCT_CODE,
-- --                            A.PRODUCT_NAME,
-- --                            A.PRODUCT_VALUE,
-- --                            B.SOURCE_FAMILY_CODE      AS SOURCE_FAMILY_CODE,
-- --                            B.SOURCE_UNIT_CODE        AS SOURCE_UNIT_CODE,
-- --                            B.SOURCE_UNIT_NAME        AS SOURCE_UNIT_NAME,
-- --                            B.TYPE_CODE,
-- --                            B.TYPE_NAME,
-- --                            A.UNIT_COST * B.C1        AS C1,
-- --                            A.UNIT_COST * B.C2        AS C2,
-- --                            A.UNIT_COST * B.C3        AS C3,
-- --                            A.UNIT_COST * B.C4        AS C4,
-- --                            A.UNIT_COST * B.C5        AS C5,
-- --                            ''PREV'' AS FLAG
-- --                     FROM (SELECT A.*
-- --                           FROM (SELECT *
-- --                                 FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
-- --                                    INNER JOIN (SELECT *
-- --                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
-- --                                          WHERE RANK = ' || V_RANK || ') B
-- --                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
-- --                                             AND A.FAMILY_CODE = B.FAMILY_CODE
-- --                                             AND A.UNIT_CODE = B.UNIT_CODE) A
-- --                              INNER JOIN (SELECT *
-- --                                    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV_SUM) B
-- --                                   ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE';
-- --                 PREPARE stmt FROM V_QUERY_STR;
-- --                 EXECUTE stmt;
--
--                 --                 SET V_QUERY_STR = '
-- --                 INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_UPSTREAM_COST
-- --                 WITH T1 AS (SELECT A.*,
-- --                                    B.FAMILY_CODE AS SOURCE_FAMILY_CODE,
-- --                                    B.UNIT_CODE   AS SOURCE_UNIT_CODE,
-- --                                    B.UNIT_NAME   AS SOURCE_UNIT_NAME
-- --                             FROM (SELECT A.*
-- --                                   FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
-- --                                            INNER JOIN (SELECT *
-- --                                                        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
-- --                                                        WHERE RANK = ' || V_RANK || ') B
-- --                                                       ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
-- --                                                           AND A.FAMILY_CODE = B.FAMILY_CODE
-- --                                                           AND A.UNIT_CODE = B.UNIT_CODE) A
-- --                                      INNER JOIN (SELECT DISTINCT MAT_TRACK_NO,
-- --                                                                  FAMILY_CODE,
-- --                                                                  UNIT_CODE,
-- --                                                                  UNIT_NAME,
-- --                                                                  PRODUCT_CODE,
-- --                                                                  PRODUCT_NAME
-- --                                                  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_ALL) B
-- --                                                 ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE)
-- --                 SELECT *
-- --                 FROM T1
-- --                 UNION
-- --                 SELECT A2.SUBCLASS_TAB_NAME,
-- --                        A2.BATCH_NUMBER,
-- --                        A2.MAT_NO,
-- --                        A2.MAT_TRACK_NO,
-- --                        A2.MAT_SEQ_NO,
-- --                        A2.FAMILY_CODE,
-- --                        A2.UNIT_CODE,
-- --                        A2.UNIT_NAME,
-- --                        A2.PRODUCT_CODE,
-- --                        A2.PRODUCT_NAME,
-- --                        B2.TYPE_CODE,
-- --                        B2.TYPE_NAME,
-- --                        B2.ITEM_CODE,
-- --                        B2.ITEM_NAME,
-- --                        B2.VALUE * A2.VALUE / B2.PRODUCT_VALUE,
-- --                        B2.UNITM_AC,
-- --                        A2.PRODUCT_VALUE,
-- --                        A2.UNIT_COST * B2.UNIT_COST,
-- --                        B2.SOURCE_FAMILY_CODE,
-- --                        B2.SOURCE_UNIT_CODE,
-- --                        B2.SOURCE_UNIT_NAME
-- --                 FROM (SELECT * FROM T1) A2
-- --                          JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_UPSTREAM_COST) B2
-- --                               ON A2.MAT_TRACK_NO = B2.MAT_TRACK_NO AND A2.SOURCE_FAMILY_CODE = B2.FAMILY_CODE AND
-- --                                  A2.SOURCE_UNIT_CODE = B2.UNIT_CODE';
-- --                 PREPARE stmt FROM V_QUERY_STR;
-- --                 EXECUTE stmt;
--
--             END IF;

            SET V_QUERY_STR = '
                    INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL
                    SELECT SUBCLASS_TAB_NAME,
                           BATCH_NUMBER,
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
                           SUM(VALUE)     AS VALUE,
                           UNITM_AC,
                           PRODUCT_VALUE,
                           SUM(UNIT_COST) AS UNIT_COST,
                           SUM(C1)        AS C1,
                           SUM(C2)        AS C2,
                           SUM(C3)        AS C3,
                           SUM(C4)        AS C4,
                           SUM(C5)        AS C5,
                           FACTOR_DIRECT,
                           FACTOR_INDIRECT,
                           FACTOR_TRANSPORT,
                           FLAG
                    FROM (SELECT A.*
                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_CURRENT A
                                   INNER JOIN (SELECT *
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
                                               WHERE RANK = ' || V_RANK || ') B
                                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                  AND A.FAMILY_CODE = B.FAMILY_CODE
                                                  AND A.UNIT_CODE = B.UNIT_CODE
                          UNION
                          SELECT A.SUBCLASS_TAB_NAME,
                                 A.BATCH_NUMBER,
                                 A.MAT_NO,
                                 A.MAT_TRACK_NO,
                                 A.MAT_SEQ_NO,
                                 A.FAMILY_CODE,
                                 A.UNIT_CODE,
                                 A.UNIT_NAME,
                                 A.PRODUCT_CODE,
                                 A.PRODUCT_NAME,
                                 B.TYPE_CODE,
                                 B.TYPE_NAME,
                                 B.ITEM_CODE,
                                 B.ITEM_NAME,
                                 A.UNIT_COST * B.VALUE,
                                 B.UNITM_AC,
                                 A.PRODUCT_VALUE,
                                 A.UNIT_COST * B.UNIT_COST,
                                 A.UNIT_COST * B.C1,
                                 A.UNIT_COST * B.C2,
                                 A.UNIT_COST * B.C3,
                                 A.UNIT_COST * B.C4,
                                 A.UNIT_COST * B.C5,
                                 B.FACTOR_DIRECT,
                                 B.FACTOR_INDIRECT,
                                 B.FACTOR_TRANSPORT,
                                 B.FLAG
                          FROM (SELECT A.*
                                FROM (SELECT *
                                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
                                         INNER JOIN (SELECT *
                                                     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
                                                     WHERE RANK = ' || V_RANK || '
                                                       AND SUM_FLAG = 0) B
                                                    ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                        AND A.FAMILY_CODE = B.FAMILY_CODE
                                                        AND A.UNIT_CODE = B.UNIT_CODE) A
                                   INNER JOIN (SELECT *
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL) B
                                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE
                          UNION
                          SELECT A.SUBCLASS_TAB_NAME,
                                 A.BATCH_NUMBER,
                                 A.MAT_NO,
                                 A.MAT_TRACK_NO,
                                 A.MAT_SEQ_NO,
                                 A.FAMILY_CODE,
                                 A.UNIT_CODE,
                                 A.UNIT_NAME,
                                 A.PRODUCT_CODE,
                                 A.PRODUCT_NAME,
                                 A.TYPE_CODE,
                                 A.TYPE_NAME,
                                 A.ITEM_CODE,
                                 A.ITEM_NAME,
                                 A.VALUE,
                                 A.UNITM_AC,
                                 A.PRODUCT_VALUE,
                                 A.UNIT_COST,
                                 A.UNIT_COST * B.C1,
                                 A.UNIT_COST * B.C2,
                                 A.UNIT_COST * B.C3,
                                 A.UNIT_COST * B.C4,
                                 A.UNIT_COST * B.C5,
                                 NULL,
                                 NULL,
                                 NULL,
                                 ''PREV_SUM''
                          FROM (SELECT A.*
                                FROM (SELECT *
                                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
                                         INNER JOIN (SELECT *
                                                     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_PREV
                                                     WHERE RANK = ' || V_RANK || '
                                                       AND SUM_FLAG = 1) B
                                                    ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                        AND A.FAMILY_CODE = B.FAMILY_CODE
                                                        AND A.UNIT_CODE = B.UNIT_CODE) A
                                   INNER JOIN (SELECT SUBCLASS_TAB_NAME,
                                                      BATCH_NUMBER,
                                                      MAT_NO,
                                                      MAT_TRACK_NO,
                                                      MAT_SEQ_NO,
                                                      FAMILY_CODE,
                                                      UNIT_CODE,
                                                      UNIT_NAME,
                                                      PRODUCT_CODE,
                                                      PRODUCT_NAME,
                                                      PRODUCT_VALUE,
                                                      SUM(C1) AS C1,
                                                      SUM(C2) AS C2,
                                                      SUM(C3) AS C3,
                                                      SUM(C4) AS C4,
                                                      SUM(C5) AS C5
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL
                                               GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE,
                                                        UNIT_CODE,
                                                        UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE) B
                                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND A.ITEM_CODE = B.PRODUCT_CODE)
                    GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                             PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, UNITM_AC, PRODUCT_VALUE, FACTOR_DIRECT,
                             FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG';

            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            SET V_RANK = V_RANK + 1;

        END WHILE;

    SET V_QUERY_STR = '
        SELECT DISTINCT MAX(RANK) AS MAX_RANK
		FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
    ';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    SET V_RANK = 1;
    WHILE V_RANK <= V_MAX_RANK
        DO

--             SET V_QUERY_STR = '
--                 INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
--                 SELECT A.*,
--                        A.UNIT_COST * C.C1,
--                        A.UNIT_COST * C.C2,
--                        A.UNIT_COST * C.C3,
--                        A.UNIT_COST * C.C4,
--                        A.UNIT_COST * C.C5,
--                        NULL,
--                        NULL,
--                        NULL,
--                        ''UPSTREAM''
--                 FROM (SELECT *
--                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                          INNER JOIN (SELECT *
--                                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
--                                      WHERE RANK = ' || V_RANK || ') B
--                                     ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                         AND A.FAMILY_CODE = B.FAMILY_CODE
--                                         AND A.UNIT_CODE = B.UNIT_CODE
--                          INNER JOIN (SELECT MAT_TRACK_NO,
--                                             FAMILY_CODE,
--                                             UNIT_CODE,
--                                             PRODUCT_CODE,
--                                             SUM(C1) AS C1,
--                                             SUM(C2) AS C2,
--                                             SUM(C3) AS C3,
--                                             SUM(C4) AS C4,
--                                             SUM(C5) AS C5
--                                      FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST
--                                      GROUP BY MAT_TRACK_NO, FAMILY_CODE, UNIT_CODE, PRODUCT_CODE) C
--                                     ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE';
--             PREPARE stmt FROM V_QUERY_STR;
--             EXECUTE stmt;

            SET V_QUERY_STR = '
                    INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL
                    SELECT SUBCLASS_TAB_NAME,
                           BATCH_NUMBER,
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
                           SUM(VALUE)     AS VALUE,
                           UNITM_AC,
                           PRODUCT_VALUE,
                           SUM(UNIT_COST) AS UNIT_COST,
                           SUM(C1)        AS C1,
                           SUM(C2)        AS C2,
                           SUM(C3)        AS C3,
                           SUM(C4)        AS C4,
                           SUM(C5)        AS C5,
                           FACTOR_DIRECT,
                           FACTOR_INDIRECT,
                           FACTOR_TRANSPORT,
                           FLAG
                    FROM (SELECT A.*
                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_CURRENT A
                                   INNER JOIN (SELECT *
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
                                               WHERE RANK = ' || V_RANK || ') B
                                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                  AND A.FAMILY_CODE = B.FAMILY_CODE
                                                  AND A.UNIT_CODE = B.UNIT_CODE
                          UNION
                          SELECT A.SUBCLASS_TAB_NAME,
                                 A.BATCH_NUMBER,
                                 A.MAT_NO,
                                 A.MAT_TRACK_NO,
                                 A.MAT_SEQ_NO,
                                 A.FAMILY_CODE,
                                 A.UNIT_CODE,
                                 A.UNIT_NAME,
                                 A.PRODUCT_CODE,
                                 A.PRODUCT_NAME,
                                 C.TYPE_CODE,
                                 C.TYPE_NAME,
                                 C.ITEM_CODE,
                                 C.ITEM_NAME,
                                 A.UNIT_COST * C.VALUE,
                                 C.UNITM_AC,
                                 A.PRODUCT_VALUE,
                                 A.UNIT_COST * C.UNIT_COST,
                                 A.UNIT_COST * C.C1,
                                 A.UNIT_COST * C.C2,
                                 A.UNIT_COST * C.C3,
                                 A.UNIT_COST * C.C4,
                                 A.UNIT_COST * C.C5,
                                 C.FACTOR_DIRECT,
                                 C.FACTOR_INDIRECT,
                                 C.FACTOR_TRANSPORT,
                                 C.FLAG
                          FROM (SELECT *
                                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
                                   INNER JOIN (SELECT *
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
                                               WHERE RANK = ' || V_RANK || '
                                                 AND SUM_FLAG = 0) B
                                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                  AND A.FAMILY_CODE = B.FAMILY_CODE
                                                  AND A.UNIT_CODE = B.UNIT_CODE
                                   INNER JOIN (SELECT *
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL) C
                                              ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE
                          UNION
                          SELECT A.SUBCLASS_TAB_NAME,
                                 A.BATCH_NUMBER,
                                 A.MAT_NO,
                                 A.MAT_TRACK_NO,
                                 A.MAT_SEQ_NO,
                                 A.FAMILY_CODE,
                                 A.UNIT_CODE,
                                 A.UNIT_NAME,
                                 A.PRODUCT_CODE,
                                 A.PRODUCT_NAME,
                                 A.TYPE_CODE,
                                 A.TYPE_NAME,
                                 A.ITEM_CODE,
                                 A.ITEM_NAME,
                                 A.VALUE,
                                 A.UNITM_AC,
                                 A.PRODUCT_VALUE,
                                 A.UNIT_COST,
                                 A.UNIT_COST * C.C1,
                                 A.UNIT_COST * C.C2,
                                 A.UNIT_COST * C.C3,
                                 A.UNIT_COST * C.C4,
                                 A.UNIT_COST * C.C5,
                                 NULL,
                                 NULL,
                                 NULL,
                                 ''PREV_SUM''
                          FROM (SELECT *
                                FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
                                   INNER JOIN (SELECT *
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
                                               WHERE RANK = ' || V_RANK || '
                                                 AND SUM_FLAG = 1) B
                                              ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
                                                  AND A.FAMILY_CODE = B.FAMILY_CODE
                                                  AND A.UNIT_CODE = B.UNIT_CODE
                                   INNER JOIN (SELECT SUBCLASS_TAB_NAME,
                                                      BATCH_NUMBER,
                                                      MAT_NO,
                                                      MAT_TRACK_NO,
                                                      MAT_SEQ_NO,
                                                      FAMILY_CODE,
                                                      UNIT_CODE,
                                                      UNIT_NAME,
                                                      PRODUCT_CODE,
                                                      PRODUCT_NAME,
                                                      PRODUCT_VALUE,
                                                      SUM(C1) AS C1,
                                                      SUM(C2) AS C2,
                                                      SUM(C3) AS C3,
                                                      SUM(C4) AS C4,
                                                      SUM(C5) AS C5
                                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL
                                               GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE,
                                                        UNIT_CODE,
                                                        UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE) C
                                              ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE)
                    GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
                             PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, UNITM_AC, PRODUCT_VALUE, FACTOR_DIRECT,
                             FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            --             SET V_QUERY_STR = '
--                     INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV
--                     SELECT A.SUBCLASS_TAB_NAME,
--                            A.BATCH_NUMBER,
--                            A.MAT_NO,
--                            A.MAT_TRACK_NO,
--                            A.MAT_SEQ_NO,
--                            A.FAMILY_CODE,
--                            A.UNIT_CODE,
--                            A.UNIT_NAME,
--                            A.PRODUCT_CODE,
--                            A.PRODUCT_NAME,
--                            C.TYPE_CODE,
--                            C.TYPE_NAME,
--                            C.ITEM_CODE,
--                            C.ITEM_NAME,
--                            A.UNIT_COST * C.VALUE     AS VALUE,
--                            C.UNITM_AC,
--                            A.PRODUCT_VALUE,
--                            A.UNIT_COST * C.UNIT_COST AS UNIT_COST,
--                            C.SOURCE_FAMILY_CODE      AS SOURCE_FAMILY_CODE,
--                            C.SOURCE_UNIT_CODE        AS SOURCE_UNIT_CODE,
--                            C.SOURCE_UNIT_NAME        AS SOURCE_UNIT_NAME,
--                            A.UNIT_COST * C.C1        AS C1,
--                            A.UNIT_COST * C.C2        AS C2,
--                            A.UNIT_COST * C.C3        AS C3,
--                            A.UNIT_COST * C.C4        AS C4,
--                            A.UNIT_COST * C.C5        AS C5,
--                            C.FACTOR_DIRECT,
--                            C.FACTOR_INDIRECT,
--                            C.FACTOR_TRANSPORT,
--                            CASE
--                                WHEN C.FLAG = ''PREV_SUM'' THEN ''PREV_SUM''
--                                ELSE ''PROCESS'' END    AS FLAG
--                     FROM (SELECT *
--                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                              INNER JOIN (SELECT *
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
--                                          WHERE RANK = ' || V_RANK || '
--                                            AND SUM_FLAG = 0) B
--                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                             AND A.FAMILY_CODE = B.FAMILY_CODE
--                                             AND A.UNIT_CODE = B.UNIT_CODE
--                              INNER JOIN (SELECT *
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV) C
--                                         ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE
--                     UNION
--                     SELECT A.SUBCLASS_TAB_NAME,
--                            A.BATCH_NUMBER,
--                            A.MAT_NO,
--                            A.MAT_TRACK_NO,
--                            A.MAT_SEQ_NO,
--                            A.FAMILY_CODE,
--                            A.UNIT_CODE,
--                            A.UNIT_NAME,
--                            A.PRODUCT_CODE,
--                            A.PRODUCT_NAME,
--                            A.TYPE_CODE,
--                            A.TYPE_NAME,
--                            A.ITEM_CODE,
--                            A.ITEM_NAME,
--                            A.VALUE,
--                            A.UNITM_AC,
--                            A.PRODUCT_VALUE,
--                            A.UNIT_COST,
--                            C.FAMILY_CODE      AS SOURCE_FAMILY_CODE,
--                            C.UNIT_CODE        AS SOURCE_UNIT_CODE,
--                            C.UNIT_NAME        AS SOURCE_UNIT_NAME,
--                            A.UNIT_COST * C.C1 AS C1,
--                            A.UNIT_COST * C.C2 AS C2,
--                            A.UNIT_COST * C.C3 AS C3,
--                            A.UNIT_COST * C.C4 AS C4,
--                            A.UNIT_COST * C.C5 AS C5,
--                            NULL,
--                            NULL,
--                            NULL,
--                            ''PREV_SUM''
--                     FROM (SELECT *
--                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                              INNER JOIN (SELECT *
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
--                                          WHERE RANK = ' || V_RANK || '
--                                            AND SUM_FLAG = 1) B
--                                         ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                             AND A.FAMILY_CODE = B.FAMILY_CODE
--                                             AND A.UNIT_CODE = B.UNIT_CODE
--                              INNER JOIN (SELECT SUBCLASS_TAB_NAME,
--                                                 BATCH_NUMBER,
--                                                 MAT_NO,
--                                                 MAT_TRACK_NO,
--                                                 MAT_SEQ_NO,
--                                                 FAMILY_CODE,
--                                                 UNIT_CODE,
--                                                 UNIT_NAME,
--                                                 PRODUCT_CODE,
--                                                 PRODUCT_NAME,
--                                                 PRODUCT_VALUE,
--                                                 SUM(C1) AS C1,
--                                                 SUM(C2) AS C2,
--                                                 SUM(C3) AS C3,
--                                                 SUM(C4) AS C4,
--                                                 SUM(C5) AS C5
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV
--                                          GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE,
--                                                   UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE) C
--                                         ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE';
--             PREPARE stmt FROM V_QUERY_STR;
--             EXECUTE stmt;

            --             SET V_QUERY_STR = '
--                     INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV_SUM
--                     SELECT A.SUBCLASS_TAB_NAME,
--                            A.BATCH_NUMBER,
--                            A.MAT_NO,
--                            A.MAT_TRACK_NO,
--                            A.MAT_SEQ_NO,
--                            A.FAMILY_CODE,
--                            A.UNIT_CODE,
--                            A.UNIT_NAME,
--                            A.PRODUCT_CODE,
--                            A.PRODUCT_NAME,
--                            A.PRODUCT_VALUE,
--                            C.SOURCE_FAMILY_CODE      AS SOURCE_FAMILY_CODE,
--                            C.SOURCE_UNIT_CODE        AS SOURCE_UNIT_CODE,
--                            C.SOURCE_UNIT_NAME        AS SOURCE_UNIT_NAME,
--                            C.TYPE_CODE,
--                            C.TYPE_NAME,
--                            A.UNIT_COST * C.C1        AS C1,
--                            A.UNIT_COST * C.C2        AS C2,
--                            A.UNIT_COST * C.C3        AS C3,
--                            A.UNIT_COST * C.C4        AS C4,
--                            A.UNIT_COST * C.C5        AS C5,
--                            ''PREV'' AS FLAG
--                     FROM (SELECT *
--                           FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN) A
--                              INNER JOIN (SELECT *
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
--                                          WHERE RANK = ' || V_RANK || ') B
--                                     ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                         AND A.FAMILY_CODE = B.FAMILY_CODE
--                                         AND A.UNIT_CODE = B.UNIT_CODE
--                              INNER JOIN (SELECT *
--                                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV_SUM) C
--                                     ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE';
--             PREPARE stmt FROM V_QUERY_STR;
--             EXECUTE stmt;

            --             SET V_QUERY_STR = '
--                 INSERT INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_UPSTREAM_COST
--                 WITH CURRENT_RANK AS (SELECT *
--                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RANK_POST
--                       WHERE RANK = ' || V_RANK || '),
--                      POST AS (SELECT A.*
--                               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESOURCE_MAIN A
--                                        INNER JOIN CURRENT_RANK B
--                                                   ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
--                                                       AND A.FAMILY_CODE = B.FAMILY_CODE
--                                                       AND A.UNIT_CODE = B.UNIT_CODE),
--                      PREV AS (SELECT A.MAT_TRACK_NO,
--                                      A.FAMILY_CODE AS POST_FAMILY_CODE,
--                                      B.FAMILY_CODE,
--                                      B.UNIT_CODE,
--                                      B.UNIT_NAME,
--                                      B.PRODUCT_CODE,
--                                      B.PRODUCT_NAME
--                               FROM CURRENT_RANK A
--                                        JOIN (SELECT DISTINCT MAT_TRACK_NO,
--                                                              FAMILY_CODE,
--                                                              UNIT_CODE,
--                                                              UNIT_NAME,
--                                                              PRODUCT_CODE,
--                                                              PRODUCT_NAME
--                                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST) B
--                                             ON A.MAT_TRACK_NO = B.MAT_TRACK_NO AND
--                                                A.PREV_FAMILY_CODE = B.FAMILY_CODE),
--                      T1 AS (SELECT A1.*,
--                                    B1.FAMILY_CODE AS SOURCE_FAMILY_CODE,
--                                    B1.UNIT_CODE   AS SOURCE_UNIT_CODE,
--                                    B1.UNIT_NAME   AS SOURCE_UNIT_NAME
--                             FROM POST A1
--                                      INNER JOIN PREV B1
--                                                 ON A1.MAT_TRACK_NO = B1.MAT_TRACK_NO
--                                                     AND A1.FAMILY_CODE = B1.POST_FAMILY_CODE
--                                                     AND A1.ITEM_CODE = B1.PRODUCT_CODE)
--                 SELECT *
--                 FROM T1
--                 UNION
--                 SELECT A2.SUBCLASS_TAB_NAME,
--                        A2.BATCH_NUMBER,
--                        A2.MAT_NO,
--                        A2.MAT_TRACK_NO,
--                        A2.MAT_SEQ_NO,
--                        A2.FAMILY_CODE,
--                        A2.UNIT_CODE,
--                        A2.UNIT_NAME,
--                        A2.PRODUCT_CODE,
--                        A2.PRODUCT_NAME,
--                        B2.TYPE_CODE,
--                        B2.TYPE_NAME,
--                        B2.ITEM_CODE,
--                        B2.ITEM_NAME,
--                        B2.VALUE * A2.VALUE / B2.PRODUCT_VALUE,
--                        B2.UNITM_AC,
--                        A2.PRODUCT_VALUE,
--                        A2.UNIT_COST * B2.UNIT_COST,
--                        B2.SOURCE_FAMILY_CODE,
--                        B2.SOURCE_UNIT_CODE,
--                        B2.SOURCE_UNIT_NAME
--                 FROM (SELECT * FROM T1) A2
--                          JOIN (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_UPSTREAM_COST) B2
--                               ON A2.MAT_TRACK_NO = B2.MAT_TRACK_NO AND A2.SOURCE_FAMILY_CODE = B2.FAMILY_CODE AND
--                                  A2.SOURCE_UNIT_CODE = B2.UNIT_CODE';
--             PREPARE stmt FROM V_QUERY_STR;
--             EXECUTE stmt;

            SET V_RANK = V_RANK + 1;
        END WHILE;

    DELETE
    FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT
    WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
      AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
      AND COMPANY_CODE = V_COMPANY_CODE;

--     DELETE
--     FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST
--     WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
--       AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
--       AND COMPANY_CODE = V_COMPANY_CODE;

    DELETE
    FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PARALLEL
    WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
      AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
      AND COMPANY_CODE = V_COMPANY_CODE;

    --     DELETE
--     FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV
--     WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
--       AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
--       AND COMPANY_CODE = V_COMPANY_CODE;

    --     DELETE
--     FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV_SUM
--     WHERE SUBCLASS_TAB_NAME = V_SUBCLASS_TAB_NAME
--       AND BATCH_NUMBER = V_SUB_BATCH_NUMBER
--       AND COMPANY_CODE = V_COMPANY_CODE;

--     SET V_QUERY_STR = '
--     INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST
--     (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, BATCH_NUMBER, MAIN_CAT_BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO,
--      FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE,
--      UNITM_AC, PRODUCT_VALUE, UNIT_COST, C1, C2, C3, C4, C5, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG,
--      REC_CREATOR, REC_CREATE_TIME, REC_REVISOR, REC_REVISE_TIME)
--     SELECT HEX(RAND()),
--            SUBCLASS_TAB_NAME,
--            ''' || V_COMPANY_CODE || ''',
--            BATCH_NUMBER,
--            ''' || V_MAX_BATCH_NUMBER || ''',
--            MAT_NO,
--            MAT_TRACK_NO,
--            MAT_SEQ_NO,
--            FAMILY_CODE,
--            UNIT_CODE,
--            UNIT_NAME,
--            PRODUCT_CODE,
--            PRODUCT_NAME,
--            TYPE_CODE,
--            TYPE_NAME,
--            ITEM_CODE,
--            ITEM_NAME,
--            VALUE,
--            UNITM_AC,
--            PRODUCT_VALUE,
--            UNIT_COST,
--            C1,
--            C2,
--            C3,
--            C4,
--            C5,
--            FACTOR_DIRECT,
--            FACTOR_INDIRECT,
--            FACTOR_TRANSPORT,
--            FLAG,
--            NULL,
--            TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''),
--            NULL,
--            NULL
--     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST';
--
--     PREPARE stmt FROM V_QUERY_STR;
--     EXECUTE stmt;

    SET V_QUERY_STR = '
    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PARALLEL
    (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, BATCH_NUMBER, MAIN_CAT_BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO,
     FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE,
     UNITM_AC, PRODUCT_VALUE, UNIT_COST, DEPT_NAME, DEPT_CODE, DEPT_MID_NAME, C1, C2, C3, C4, C5, C_CYCLE, FACTOR_DIRECT,
     FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG, REC_CREATOR, REC_CREATE_TIME, REC_REVISOR, REC_REVISE_TIME)
    SELECT HEX(RAND()),
           SUBCLASS_TAB_NAME,
           ''' || V_COMPANY_CODE || ''',
           BATCH_NUMBER,
           ''' || V_MAX_BATCH_NUMBER || ''',
           MAT_NO,
           MAT_TRACK_NO,
           MAT_SEQ_NO,
           FAMILY_CODE,
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
           UNIT_COST,
           B.DEPT_NAME,
           B.DEPT_CODE,
           B.DEPT_MID_NAME,
           C1,
           C2,
           C3,
           C4,
           C5,
           C1 + C2 + C3 + C4 + C5,
           FACTOR_DIRECT,
           FACTOR_INDIRECT,
           FACTOR_TRANSPORT,
           FLAG,
           NULL,
           TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''),
           NULL,
           NULL
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL A
         LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || ''') B
                   ON A.UNIT_CODE = B.UNIT_CODE';

    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    --     SET V_QUERY_STR = '
--     INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV
--     (REC_ID, SUBCLASS_TAB_NAME, COMPANY_CODE, BATCH_NUMBER, MAIN_CAT_BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO,
--      FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE,
--      UNITM_AC, PRODUCT_VALUE, UNIT_COST, SOURCE_FAMILY_CODE, SOURCE_UNIT_CODE, SOURCE_UNIT_NAME, C1, C2, C3, C4, C5,
--      C_CYCLE, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG, REC_CREATOR, REC_CREATE_TIME, REC_REVISOR, REC_REVISE_TIME)
--     SELECT HEX(RAND()),
--            SUBCLASS_TAB_NAME,
--            ''' || V_COMPANY_CODE || ''',
--            BATCH_NUMBER,
--            ''' || V_MAX_BATCH_NUMBER || ''',
--            MAT_NO,
--            MAT_TRACK_NO,
--            MAT_SEQ_NO,
--            FAMILY_CODE,
--            UNIT_CODE,
--            UNIT_NAME,
--            PRODUCT_CODE,
--            PRODUCT_NAME,
--            TYPE_CODE,
--            TYPE_NAME,
--            ITEM_CODE,
--            ITEM_NAME,
--            VALUE,
--            UNITM_AC,
--            PRODUCT_VALUE,
--            UNIT_COST,
--            SOURCE_FAMILY_CODE,
--            SOURCE_UNIT_CODE,
--            SOURCE_UNIT_NAME,
--            C1,
--            C2,
--            C3,
--            C4,
--            C5,
--            C1 + C2 + C3 + C4 + C5,
--            FACTOR_DIRECT,
--            FACTOR_INDIRECT,
--            FACTOR_TRANSPORT,
--            FLAG,
--            NULL,
--            TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''),
--            NULL,
--            NULL
--     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV';
--
--     PREPARE stmt FROM V_QUERY_STR;
--     EXECUTE stmt;

    --     SET V_QUERY_STR = '
--     INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV_SUM
--     SELECT HEX(RAND()),
--            SUBCLASS_TAB_NAME,
--            ''' || V_COMPANY_CODE || ''',
--            BATCH_NUMBER,
--            ''' || V_MAX_BATCH_NUMBER || ''',
--            MAT_NO,
--            MAT_TRACK_NO,
--            MAT_SEQ_NO,
--            FAMILY_CODE,
--            UNIT_CODE,
--            UNIT_NAME,
--            PRODUCT_CODE,
--            PRODUCT_NAME,
--            PRODUCT_VALUE,
--            SOURCE_FAMILY_CODE,
--            SOURCE_UNIT_CODE,
--            SOURCE_UNIT_NAME,
--            TYPE_CODE,
--            TYPE_NAME,
--            C1,
--            C2,
--            C3,
--            C4,
--            C5,
--            C1 + C2 + C3 + C4 + C5,
--            FLAG,
--            NULL,
--            TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI''),
--            NULL,
--            NULL
--     FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PREV_SUM';
--
--     PREPARE stmt FROM V_QUERY_STR;
--     EXECUTE stmt;

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
    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_RESULT_DIST_PARALLEL
    GROUP BY SUBCLASS_TAB_NAME, BATCH_NUMBER, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
             PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE
    ORDER BY MAT_TRACK_NO, FAMILY_CODE, MAT_SEQ_NO';

    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;

    ------------------------------------处理逻辑(结束)------------------------------------

    --删除生成的临时表
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

