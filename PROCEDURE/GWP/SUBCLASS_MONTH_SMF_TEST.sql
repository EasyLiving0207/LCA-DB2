CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH_SMF(IN V_COMPANY_CODE VARCHAR(4),
                                                                                  IN V_START_MONTH VARCHAR(6),
                                                                                  IN V_END_MONTH VARCHAR(6),
                                                                                  IN V_FACTOR_YEAR VARCHAR(4),
                                                                                  IN V_MAIN_TAB_NAME VARCHAR(100),
                                                                                  IN V_MAIN_BATCH_NUMBER VARCHAR(100),
                                                                                  IN V_SUBCLASS_TAB_NAME VARCHAR(100),
                                                                                  IN V_SUBCLASS_BATCH_NUMBER VARCHAR(100),
                                                                                  IN V_SUBCLASS_RESULT_TAB_NAME VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH_SMF
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
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH_SMF'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);
    DECLARE MESSAGE_TEXT VARCHAR(2048);

    ------------------------------------日志变量定义------------------------------------
    DECLARE TAR_SCHEMA1 VARCHAR(32) DEFAULT 'BG00MAC102'; --目标表SCHEMA
    DECLARE SRC_TAB_NAME1 VARCHAR(32) DEFAULT ' '; --源表SCHEMA.表名
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_SMF'; --临时表名
    DECLARE V_PNAME VARCHAR(50) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_SEQ_CALC_MONTH_SMF'; --存储过程名
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

    --取大类结果
    SET V_QUERY_STR = '
                SELECT *
                FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_TAB_NAME || '
                WHERE BATCH_NUMBER = ''' || V_MAIN_BATCH_NUMBER || '''
                AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                AND LCI_ELEMENT_NAME = ''全球变暖潜力(GWP100):合计''
        ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ENERGY_RESULT', V_QUERY_STR);

    --取活动数据
    SET V_QUERY_STR = '
        SELECT BATCH_NUMBER,
               UPDATE_DATE,
               MAT_TRACK_NO,
               MAT_NO,
               MAT_SEQ_NO,
               FAMILY_CODE,
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
        WHERE BATCH_NUMBER = ''' || V_SUBCLASS_BATCH_NUMBER || '''
        AND VALUE != 0
    ';
    IF V_START_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE >= ''' || V_START_MONTH || '''';
    END IF;
    IF V_END_MONTH IS NOT NULL THEN
        SET V_QUERY_STR = V_QUERY_STR || ' AND UPDATE_DATE <= ''' || V_END_MONTH || '''';
    END IF;
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);

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
            CASE
                   WHEN UNITM_AC = ''万度'' THEN VALUE * 10000
                   WHEN UNITM_AC = ''吨'' THEN VALUE * 1000
                   WHEN UNITM_AC = ''千立方米'' THEN VALUE * 1000
                   ELSE VALUE
                   END
            AS PRODUCT_VALUE
        FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
        WHERE BATCH_NUMBER = ''' || V_SUBCLASS_BATCH_NUMBER || '''
        AND TYPE_CODE = ''04''
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_PRODUCT_LIST', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT ''' || V_SUBCLASS_TAB_NAME || ''' AS SUBCLASS_TAB_NAME,
               BATCH_NUMBER,
               A.UPDATE_DATE,
               A.MAT_NO,
               A.MAT_TRACK_NO,
               A.MAT_SEQ_NO,
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

    SET V_QUERY_STR = '
        SELECT DISTINCT A.* FROM (SELECT DISTINCT MAT_TRACK_NO, MAT_NO
                FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                WHERE BATCH_NUMBER = ''' || V_SUBCLASS_BATCH_NUMBER || ''' AND FAMILY_CODE IS NULL) A
            JOIN (SELECT DISTINCT MAT_TRACK_NO
        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
        WHERE FAMILY_CODE = ''01'') B
        ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
     ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'SMF_NO', V_QUERY_STR);

    SET V_QUERY_STR = '
        SELECT BATCH_NUMBER,
               MAT_NO,
               MAT_SEQ_NO,
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
        WHERE BATCH_NUMBER = ''' || V_SUBCLASS_BATCH_NUMBER || '''
            AND FAMILY_CODE IS NULL
            AND MAT_NO IN (SELECT DISTINCT MAT_NO FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '_SMF_NO)
            AND VALUE != 0
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'SMF_TEMP_DATA', V_QUERY_STR);


    --工艺路径
    SET V_QUERY_STR = '
         SELECT DISTINCT UPDATE_DATE, MAT_TRACK_NO, MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME,
           CASE WHEN LENGTH(FAMILY_CODE) > 2 THEN LEFT(FAMILY_CODE, LENGTH(FAMILY_CODE) - 2)
                ELSE NULL END AS PREV_FAMILY_CODE
         FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
         WHERE BATCH_NUMBER = ''' || V_SUBCLASS_BATCH_NUMBER || '''
         AND FAMILY_CODE IS NOT NULL
         ORDER BY FAMILY_CODE, MAT_SEQ_NO
    ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_SEQ', V_QUERY_STR);

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
                    FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ) A
                       LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ B
                                 ON A.PREV_FAMILY_CODE = B.FAMILY_CODE and A.MAT_TRACK_NO = B.MAT_TRACK_NO
                       LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' ||
                      V_COMPANY_CODE || ''') C
                                 ON A.UNIT_CODE = C.UNIT_CODE
                       LEFT JOIN (SELECT * FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022 WHERE COMPANY_CODE = ''' ||
                      V_COMPANY_CODE || ''') D
                                 ON B.UNIT_CODE = D.UNIT_CODE
              ORDER BY A.MAT_TRACK_NO, A.FAMILY_CODE, A.MAT_SEQ_NO)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'RANK', V_QUERY_STR);


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

