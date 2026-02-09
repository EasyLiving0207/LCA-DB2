CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_READ_ACTI_DATA_DT(IN V_PCR VARCHAR(6),
                                                                                 IN V_COMPANY_CODE VARCHAR(6),
                                                                                 IN V_CERTIFICATION_NUMBER VARCHAR(100),
                                                                                 IN V_SUBCLASS_TAB_NAME VARCHAR(100))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_READ_ACTI_DATA_DT
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    CALLED ON NULL INPUT
    INHERIT SPECIAL REGISTERS
    OLD
        SAVEPOINT LEVEL
BEGIN

    ------------------------------------日志变量定义------------------------------------
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_CALC_DT'; --临时表名
    DECLARE V_QUERY_STR CLOB(1 M);

    ------------------------------------存储过程变量定义---------------------------------


    SET V_QUERY_STR =
            'SELECT DISTINCT MAT_TRACK_NO FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
             WHERE CERTIFICATION_NUMBER = ''' || V_CERTIFICATION_NUMBER || ''' ' ||
            'AND MAT_TRACK_NO NOT IN (SELECT DISTINCT MAT_TRACK_NO
                           FROM (SELECT MAT_TRACK_NO, MAX(FAMILY_CODE) AS MAX_NO
                                 FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                                 GROUP BY MAT_TRACK_NO)
                           WHERE MAX_NO IS NULL)';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'MAT_TRACK_NO', V_QUERY_STR);

    SET V_QUERY_STR = '
               SELECT DISTINCT UPDATE_DATE,
                      CASE
                        WHEN A.FAMILY_CODE IS NULL THEN CONCAT(CONCAT(CONCAT(CONCAT(A.MAT_TRACK_NO, ''_''), ''00''), ''_''), A.MAT_SEQ_NO)
                        ELSE CONCAT(CONCAT(A.MAT_TRACK_NO, ''_''), A.FAMILY_CODE) END      AS INDEX_CODE,
                      A.MAT_TRACK_NO,
                      MAT_NO,
                      MAT_SEQ_NO,
                      MAT_WT,
                      MAT_STATUS,
                      CASE WHEN FAMILY_CODE IS NULL THEN ''00''
                      WHEN FAMILY_CODE = '''' THEN ''00''
                      ELSE FAMILY_CODE END AS FAMILY_CODE,
                      UNIT_CODE,
                      UNIT_NAME,
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
               FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_SUBCLASS_TAB_NAME || '
                        WHERE CERTIFICATION_NUMBER = ''' || V_CERTIFICATION_NUMBER || ''') A
                 JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_MAT_TRACK_NO B
                   ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
               WHERE VALUE IS NOT NULL AND VALUE != 0 ';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'TEMP_DATA', V_QUERY_STR);


    IF V_PCR = 'CONS' THEN
        IF V_COMPANY_CODE = 'TA' THEN
            SET V_QUERY_STR = 'UPDATE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                           SET VALUE = VALUE * 0.9877
                           WHERE UNIT_CODE LIKE ''%BOF''
                             AND TYPE_CODE != ''04''';
        ELSEIF V_COMPANY_CODE = 'ZG' THEN
            SET V_QUERY_STR = 'UPDATE ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                           SET VALUE = VALUE * 0.9877
                           WHERE UNIT_CODE = ''LG04''
                             AND TYPE_CODE != ''04''';
        END IF;
        PREPARE stmt FROM V_QUERY_STR;
        EXECUTE stmt;
    END IF;


    --PROC信息
    SET V_QUERY_STR = 'SELECT DISTINCT UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              MAT_WT,
                              MAT_STATUS,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              ITEM_CODE AS PRODUCT_CODE,
                              ITEM_NAME AS PRODUCT_NAME,
                              VALUE AS PRODUCT_VALUE
                         FROM
                             ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA
                         WHERE TYPE_CODE = ''04''';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'PROC_PRODUCT_LIST', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT A.UPDATE_DATE,
                              A.INDEX_CODE,
                              A.MAT_NO,
                              A.MAT_TRACK_NO,
                              A.MAT_SEQ_NO,
                              A.MAT_WT,
                              A.MAT_STATUS,
                              A.FAMILY_CODE,
                              A.UNIT_CODE,
                              A.UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              TYPE_CODE,
                              TYPE_NAME,
                              ITEM_CODE,
                              ITEM_NAME,
                              ABS(VALUE) AS VALUE,
                              UNITM_AC,
                              CAST(ABS(VALUE) AS DOUBLE) / CAST(PRODUCT_VALUE AS DOUBLE) AS UNIT_COST,
                              CASE
                                  WHEN TYPE_CODE < ''04'' THEN ''RESOURCE''
                                  WHEN TYPE_CODE = ''04'' THEN ''PRODUCT''
                                  WHEN TYPE_CODE = ''05'' THEN ''BY_PRODUCT''
                                  WHEN TYPE_CODE = ''08'' THEN ''WASTE''
                                  WHEN TYPE_CODE IN (''06'', ''07'') THEN ''EMISSION''
                                  END AS FLAG
                        FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_TEMP_DATA A
                            JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
                              ON A.INDEX_CODE = B.INDEX_CODE';
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
                                         LEFT JOIN (SELECT DISTINCT UNIT_CODE,
                                                                    DEPT_CODE,
                                                                    DEPT_NAME,
                                                                    DEPT_MID_NAME
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


    SET V_QUERY_STR = 'SELECT DISTINCT ITEM_CODE, ITEM_NAME FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'ITEM_BASE', V_QUERY_STR);


END;