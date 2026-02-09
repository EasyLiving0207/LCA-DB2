CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_JOIN_DIST(
    IN V_PCR VARCHAR(6),
    IN V_COMPANY_CODE VARCHAR(6)
)
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_JOIN_DIST
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    CALLED ON NULL INPUT
    INHERIT SPECIAL REGISTERS
    OLD SAVEPOINT LEVEL

BEGIN

    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102';
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_CALC';
    DECLARE V_QUERY_STR CLOB(1 M);


    SET V_QUERY_STR = 'MERGE INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                       USING (SELECT A.INDEX_CODE, A.PREV_INDEX_CODE, B.PRODUCT_CODE AS PREV_PRODUCT
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ A
                                       JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST B
                                            ON A.PREV_INDEX_CODE = B.INDEX_CODE) B
                       ON A.FLAG = ''RESOURCE'' AND A.INDEX_CODE = B.INDEX_CODE AND A.ITEM_CODE = B.PREV_PRODUCT
                       WHEN
                           MATCHED THEN
                           UPDATE
                           SET A.FLAG = ''MAIN''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'MERGE INTO ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                       USING (SELECT DISTINCT PRODUCT_CODE
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT) B
                       ON A.FLAG = ''RESOURCE'' AND A.ITEM_CODE = B.PRODUCT_CODE
                       WHEN
                           MATCHED THEN
                           UPDATE
                           SET A.FLAG = ''ENERGY''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'SELECT B.PREV_INDEX_CODE AS PARENT_INDEX, A.INDEX_CODE AS CHILD_INDEX, A.UNIT_COST, A.VALUE
                        FROM (SELECT *
                              FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                              WHERE FLAG = ''MAIN'') A
                                 JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ B
                                            ON A.INDEX_CODE = B.INDEX_CODE
                                 JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST C
                                            ON B.PREV_INDEX_CODE = C.INDEX_CODE AND A.ITEM_CODE = C.PRODUCT_CODE';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'EDGE', V_QUERY_STR);


    SET V_QUERY_STR =
            'SELECT DISTINCT ITEM_CODE
               FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
          WHERE FLAG = ''BY_PRODUCT''';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'BY_PRODUCT_CODE', V_QUERY_STR);


    --本工序结果
    SET V_QUERY_STR =
            'WITH
             DATA_RESOURCE AS (
                 SELECT *
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
              WHERE FLAG = ''RESOURCE''
             ),
             DATA_ENERGY AS (
                 SELECT *
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                  WHERE FLAG = ''ENERGY''
             ),
             DATA_WASTE AS (
                 SELECT *
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                  WHERE FLAG = ''WASTE''
             ),
             DATA_BY_PRODUCT AS (
                 SELECT *
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                  WHERE FLAG = ''BY_PRODUCT'' ' ||
                CASE
                    WHEN V_PCR = 'CONS' AND V_COMPANY_CODE = 'TA' THEN
                        ' AND UNIT_CODE NOT LIKE ''%BOF'' '
                    WHEN V_PCR = 'CONS' AND V_COMPANY_CODE = 'ZG' THEN
                        ' AND UNIT_CODE != ''LG04'' '
                    ELSE ''
                    END ||
                '),
                 DATA_IN AS (
                     SELECT *
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                  WHERE TYPE_CODE < ''04''
             ),
             DATA_OUT AS (
                 SELECT *
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                  WHERE TYPE_CODE >= ''04''
             ),

             DIST_RESOURCE AS (
                 SELECT A.*,
                        B.LCI_ELEMENT_CODE,
                        0 AS C1,
                        COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C2,
                        0 AS C3,
                        0 AS C4,
                        0 AS C5
                   FROM DATA_RESOURCE A
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_SY B
                     ON A.ITEM_CODE = B.ITEM_CODE
                  WHERE EXISTS (
                        SELECT 1
                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_BY_PRODUCT_CODE BPC
                         WHERE BPC.ITEM_CODE = A.ITEM_CODE
                  )

                 UNION ALL

                 SELECT A.*,
                        B.LCI_ELEMENT_CODE,
                        0 AS C1,
                        0 AS C2,
                        COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C3,
                        0 AS C4,
                        0 AS C5
                   FROM DATA_RESOURCE A
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_SY B
                     ON A.ITEM_CODE = B.ITEM_CODE
                  WHERE NOT EXISTS (
                        SELECT 1
                          FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_BY_PRODUCT_CODE BPC
                         WHERE BPC.ITEM_CODE = A.ITEM_CODE
                  )

                 UNION ALL

                 SELECT A.*,
                        B.LCI_ELEMENT_CODE,
                        0 AS C1,
                        0 AS C2,
                        0 AS C3,
                        0 AS C4,
                        COALESCE(A.UNIT_COST * B.LCI_ELEMENT_VALUE, 0) AS C5
                   FROM DATA_RESOURCE A
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_TRANSPORT B
                     ON A.ITEM_CODE = B.ITEM_CODE
             ),

             DIST_ENERGY AS (
                 SELECT A.*,
                        C.LCI_ELEMENT_CODE,
                        COALESCE(A.UNIT_COST * C.C1_DIRECT, 0) AS C1,
                        COALESCE(A.UNIT_COST * C.C2_BP, 0)     AS C2,
                        COALESCE(A.UNIT_COST * C.C3_OUT, 0)    AS C3,
                        COALESCE(A.UNIT_COST * C.C4_BP_NEG, 0) AS C4,
                        COALESCE(A.UNIT_COST * C.C5_TRANS, 0)  AS C5
                   FROM DATA_ENERGY A
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATE_BATCH B
                     ON A.UPDATE_DATE = B.UPDATE_DATE
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ENERGY_RESULT C
                     ON B.BATCH_NUMBER = C.BATCH_NUMBER
                    AND A.ITEM_CODE    = C.PRODUCT_CODE
             ),

             DIST_FUEL AS (
                 SELECT D.UPDATE_DATE,
                        D.INDEX_CODE,
                        D.MAT_NO,
                        D.MAT_TRACK_NO,
                        D.MAT_SEQ_NO,
                        D.MAT_WT,
                        D.MAT_STATUS,
                        D.FAMILY_CODE,
                        D.UNIT_CODE,
                        D.UNIT_NAME,
                        D.PRODUCT_CODE,
                        D.PRODUCT_NAME,
                        D.PRODUCT_VALUE,
                        D.TYPE_CODE,
                        D.TYPE_NAME,
                        D.ITEM_CODE,
                        D.ITEM_NAME,
                        D.VALUE,
                        D.UNITM_AC,
                        D.UNIT_COST,
                        ''FUEL'' AS FLAG,
                        F.LCI_ELEMENT_CODE,
                        COALESCE(
                          (CASE
                             WHEN D.TYPE_CODE < ''04'' THEN D.UNIT_COST
                             ELSE -ABS(D.UNIT_COST)
                           END) * F.LCI_ELEMENT_VALUE
                        , 0) AS C1,
                        0 AS C2,
                        0 AS C3,
                        0 AS C4,
                        0 AS C5
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA D
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FUEL F
                     ON D.ITEM_CODE = F.ITEM_CODE
             ),

             DIST_EP AS (
                 SELECT A.UPDATE_DATE,
                        A.INDEX_CODE,
                        A.MAT_NO,
                        A.MAT_TRACK_NO,
                        A.MAT_SEQ_NO,
                        A.MAT_WT,
                        A.MAT_STATUS,
                        A.FAMILY_CODE,
                        A.UNIT_CODE,
                        A.UNIT_NAME,
                        A.PRODUCT_CODE,
                        A.PRODUCT_NAME,
                        A.PRODUCT_VALUE,
                        A.TYPE_CODE,
                        A.TYPE_NAME,
                        A.ITEM_CODE,
                        A.ITEM_NAME,
                        A.VALUE,
                        A.UNITM_AC,
                        A.UNIT_COST,
                        CASE WHEN A.FLAG <> ''EMISSION'' THEN ''STREAM'' ELSE A.FLAG END AS FLAG,
                        B.LCI_ELEMENT_CODE,
                        COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C1,
                        0 AS C2,
                        0 AS C3,
                        0 AS C4,
                        0 AS C5
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA A
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_EP B
                     ON A.ITEM_CODE = B.ITEM_CODE
             ),

             DIST_WASTE AS (
                 SELECT A.*,
                        B.LCI_ELEMENT_CODE,
                        0 AS C1,
                        0 AS C2,
                        0 AS C3,
                        COALESCE(ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C4,
                        0 AS C5
                   FROM DATA_WASTE A
                   JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FCP B
                     ON A.ITEM_CODE = B.ITEM_CODE
             ),

             FACTOR_FCP_FILTERED AS (
                 SELECT *
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_FACTOR_FCP ' ||
                CASE
                    WHEN V_PCR = 'CONS' THEN
                        ' WHERE MAT_NAME IN (''废铁处理'', ''富锌材料处理'', ''BOF废渣处理'', ''蒸汽（低压）'', ''蒸汽（中压）'')'
                    ELSE ''
                    END || '
             ),

             DIST_BY_PRODUCT AS (
                 SELECT A.*,
                        B.LCI_ELEMENT_CODE,
                        0 AS C1,
                        0 AS C2,
                        0 AS C3,
                        COALESCE(-ABS(A.UNIT_COST) * B.LCI_ELEMENT_VALUE, 0) AS C4,
                        0 AS C5
                   FROM DATA_BY_PRODUCT A
                   JOIN FACTOR_FCP_FILTERED B
                     ON A.ITEM_CODE = B.ITEM_CODE
             )

             SELECT *
               FROM DIST_RESOURCE
             UNION ALL
             SELECT *
               FROM DIST_ENERGY
             UNION ALL
             SELECT *
               FROM DIST_FUEL
             UNION ALL
             SELECT *
               FROM DIST_EP
             UNION ALL
             SELECT *
               FROM DIST_WASTE
             UNION ALL
             SELECT *
               FROM DIST_BY_PRODUCT';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'DIST', V_QUERY_STR);

END;