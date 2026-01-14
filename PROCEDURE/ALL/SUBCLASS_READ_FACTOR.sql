CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_READ_FACTOR(
    IN V_PCR VARCHAR(6),
    IN V_COMPANY_CODE VARCHAR(6),
    IN V_START_YM VARCHAR(6),
    IN V_END_YM VARCHAR(6),
    IN V_FACTOR_YEAR VARCHAR(6),
    IN V_FACTOR_VERSION VARCHAR(100),
    IN V_AUTO_BATCH BOOLEAN,
    IN V_BATCH_SUFFIX VARCHAR(100),
    IN V_MAIN_CAT_TAB_NAME VARCHAR(100),
    IN V_MAIN_CAT_BATCH_NUMBER VARCHAR(100),
    IN V_SUBCLASS_TAB_NAME VARCHAR(100),
    IN V_SUBCLASS_RESULT_TAB_NAME VARCHAR(100),
    IN V_SUBCLASS_RESULT_DIST_TAB_NAME VARCHAR(100)
)
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_READ_FACTOR
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
    DECLARE V_FACTOR_LIST VARCHAR(100);
    DECLARE V_FACTOR_LIB VARCHAR(100);
    DECLARE V_QUERY_STR CLOB(1 M);

    -- Escape inputs used in dynamic SQL
    DECLARE V_COMPANY_ESC VARCHAR(64);
    DECLARE V_YEAR_ESC VARCHAR(64);
    DECLARE V_VER_ESC VARCHAR(210);

    SET V_COMPANY_ESC = REPLACE(COALESCE(V_COMPANY_CODE, ''), '''', '''''');
    SET V_YEAR_ESC = REPLACE(COALESCE(V_FACTOR_YEAR, ''), '''', '''''');
    SET V_VER_ESC = REPLACE(COALESCE(V_FACTOR_VERSION, ''), '''', '''''');

    IF V_PCR = 'NORM' THEN
        SET V_FACTOR_LIST = 'T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION';
    ELSEIF V_PCR = 'CONS' THEN
        SET V_FACTOR_LIST = 'T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION';
    ELSE
        RETURN;
    END IF;

    SET V_FACTOR_LIB = 'T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY';

    ----------------------------------------------------------------------------
    -- EF  (unchanged logic, but avoid SELECT * and keep only needed cols)
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'SELECT A.ITEM_CODE, ' ||
            '       A.ITEM_NAME, ' ||
            '       B.SOURCE_ITEM_CODE, ' ||
            '       CASE WHEN B.NCV_UNIT = ''GJ/104Nm3'' THEN B.NCV / 10 ELSE B.NCV END AS NCV, ' ||
            '       CASE WHEN B.NCV_UNIT = ''GJ/104Nm3'' THEN B.EF  / 10 ELSE B.EF  END AS EF ' ||
            'FROM BG00MAC102.T_ADS_WH_LCA_ITEM_CONTRAST A ' ||
            'JOIN BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY B ' ||
            '  ON A.UUID = B.ITEM_CODE ' ||
            'WHERE A.COMPANY_CODE  = ''' || V_COMPANY_ESC || ''' ' ||
            '  AND A.FLAG = ''DIRECT''';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'EF', V_QUERY_STR);

    SET V_QUERY_STR = 'SELECT DISTINCT ITEM_CODE, ITEM_NAME, UUID
                        FROM BG00MAC102.T_ADS_WH_LCA_ITEM_CONTRAST
                        WHERE PCR = ''NORM''
                          AND VERSION = ''' || V_FACTOR_VERSION || '''
                          AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                          AND FLAG = ''MAT''
                          AND UUID IS NOT NULL';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'UUID_MAT', V_QUERY_STR);


    SET V_QUERY_STR = 'SELECT DISTINCT ITEM_CODE, ITEM_NAME, UUID
                        FROM BG00MAC102.T_ADS_WH_LCA_ITEM_CONTRAST
                        WHERE PCR = ''NORM''
                          AND VERSION = ''' || V_FACTOR_VERSION || '''
                          AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                          AND FLAG = ''STREAM''
                          AND UUID IS NOT NULL';
    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'UUID_STREAM', V_QUERY_STR);

    ----------------------------------------------------------------------------
    -- LCI_LIST (only needed columns; avoid DISTINCT *)
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'SELECT VERSION, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME ' ||
            'FROM ' || V_TMP_SCHEMA || '.' || V_FACTOR_LIST || ' ' ||
            'WHERE VERSION = ''' || V_VER_ESC || ''' ' ||
            '  AND LCI_ELEMENT_CODE IS NOT NULL ' ||
            'GROUP BY VERSION, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_LIST', V_QUERY_STR);

    ----------------------------------------------------------------------------
    -- LCI_STREAM: join UUID_STREAM to factor-lib STREAM rows filtered by LCI_LIST
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'WITH LCI AS ( ' ||
            '  SELECT LCI_ELEMENT_CODE, LCI_ELEMENT_NAME ' ||
            '  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST ' ||
            '), STREAM_LIB AS ( ' ||
            '  SELECT UUID AS STREAM_ID, VERSION, NAME AS STREAM_NAME, ' ||
            '         LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE ' ||
            '  FROM ' || V_TMP_SCHEMA || '.' || V_FACTOR_LIB || ' ' ||
            '  WHERE VERSION = ''' || V_VER_ESC || ''' ' ||
            '    AND PCR = ''' || V_PCR || ''' ' ||
            '    AND FLAG = ''STREAM'' ' ||
            ') ' ||
            'SELECT A.ITEM_CODE, A.UUID AS STREAM_ID, ' ||
            '       B.VERSION, B.STREAM_NAME, ' ||
            '       B.LCI_ELEMENT_CODE, B.LCI_ELEMENT_NAME, B.LCI_ELEMENT_VALUE ' ||
            'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_UUID_STREAM A ' ||
            'JOIN STREAM_LIB B ON A.UUID = B.STREAM_ID ' ||
            'JOIN LCI C ON B.LCI_ELEMENT_CODE = C.LCI_ELEMENT_CODE';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'LCI_STREAM', V_QUERY_STR);

    ----------------------------------------------------------------------------
    -- FACTOR_EP: avoid UNION duplication by UNION ALL then de-dup if needed later
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'SELECT ITEM_CODE, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE ' ||
            'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM ' ||
            'UNION ALL ' ||
            'SELECT A.ITEM_CODE, A.LCI_ELEMENT_CODE, A.LCI_ELEMENT_NAME, ' ||
            '       A.LCI_ELEMENT_VALUE * CAST(B.NCV AS DOUBLE) AS LCI_ELEMENT_VALUE ' ||
            'FROM ( ' ||
            '  SELECT ITEM_CODE, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE ' ||
            '  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_STREAM ' ||
            '  WHERE LCI_ELEMENT_CODE IN (''RSF'',''NRSF'') ' ||
            ') A ' ||
            'JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_EF B ' ||
            '  ON A.ITEM_CODE = B.ITEM_CODE';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_EP', V_QUERY_STR);

    ----------------------------------------------------------------------------
    -- FACTOR_FUEL: replace correlated subselects with join to LCI_LIST once
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'WITH ITEMS AS ( ' ||
            '  SELECT ITEM_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE ' ||
            '), LCI AS ( ' ||
            '  SELECT LCI_ELEMENT_CODE, LCI_ELEMENT_NAME ' ||
            '  FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST ' ||
            '  WHERE LCI_ELEMENT_CODE IN (''GWP-total'',''GWP-fossil'',''PENRT'',''PENRE'') ' ||
            '), EF AS ( ' ||
            '  SELECT ITEM_CODE, EF, NCV FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_EF ' ||
            ') ' ||
            'SELECT I.ITEM_CODE, L.LCI_ELEMENT_CODE, L.LCI_ELEMENT_NAME, ' ||
            '       CASE ' ||
            '         WHEN L.LCI_ELEMENT_CODE IN (''GWP-total'',''GWP-fossil'') THEN E.EF ' ||
            '         ELSE E.NCV ' ||
            '       END AS LCI_ELEMENT_VALUE ' ||
            'FROM ITEMS I ' ||
            'JOIN EF E ON I.ITEM_CODE = E.ITEM_CODE ' ||
            'JOIN LCI L ON 1=1';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_FUEL', V_QUERY_STR);

    ----------------------------------------------------------------------------
    -- FACTOR_TRANSPORT: main speedups are (1) avoid re-reading LCI_LIST repeatedly
    -- Note: kept YEAR=''2024'' to preserve your current semantics
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'WITH LCI AS ( ' ||
            '  SELECT LCI_ELEMENT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST ' ||
            '), RAW AS ( ' ||
            '  SELECT DISTINCT LCA_DATA_ITEM_CODE AS ITEM_CODE, ' ||
            '         LCA_DATA_ITEM_NAME AS ITEM_NAME, ' ||
            '         BIGCLASS_NAME, TRANS_MTHD_CODE, ' ||
            '         CASE WHEN UNIT = ''nm'' THEN TRANS_DISTANCE * 1.852 ELSE TRANS_DISTANCE END AS TRANS_DISTANCE, ' ||
            '         CASE WHEN UNIT = ''nm'' THEN ''km'' ELSE UNIT END AS UNIT ' ||
            '  FROM BG00MAC102.T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION_DATA_MERGE ' ||
            '  WHERE ORG_CODE = ''' || V_COMPANY_ESC || ''' ' ||
            '    AND YEAR = ''2024'' ' ||
            '    AND LCA_DATA_ITEM_CODE IS NOT NULL ' ||
            '), TRANS_DATA AS ( ' ||
            '  SELECT A.LCA_DATA_ITEM_CODE AS ITEM_CODE, ' ||
            '         A.LCA_DATA_ITEM_NAME AS ITEM_NAME, ' ||
            '         CASE ' ||
            '           WHEN A.CLASS_CODE IS NOT NULL THEN A.CLASS_CODE ' ||
            '           WHEN A.CLASS_CODE IS NULL AND A.LCA_DATA_ITEM_CODE LIKE ''125%'' THEN ''THJ001'' ' ||
            '           ELSE ''FYL001'' ' ||
            '         END AS CLASS_CODE ' ||
            '  FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA A ' ||
            '  JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE B ' ||
            '    ON A.LCA_DATA_ITEM_CODE = B.ITEM_CODE ' ||
            '  WHERE A.COMPANY_CODE = ''' || V_COMPANY_ESC || ''' ' ||
            '    AND A.START_TIME = ''' || V_YEAR_ESC || ''' ' ||
            '), OTHER AS ( ' ||
            '  SELECT * FROM TRANS_DATA ' ||
            '  WHERE ITEM_CODE NOT IN (SELECT ITEM_CODE FROM RAW) ' ||
            '), TRANS_AVG AS ( ' ||
            '  SELECT CASE ' ||
            '           WHEN BIGCLASS_NAME = ''铁矿石'' THEN ''TKS001'' ' ||
            '           WHEN BIGCLASS_NAME = ''废钢'' THEN ''FG001'' ' ||
            '           WHEN BIGCLASS_NAME = ''铁合金'' THEN ''THJ001'' ' ||
            '           ELSE ''FYL001'' ' ||
            '         END AS CLASS_CODE, ' ||
            '         TRANS_MTHD_CODE, ' ||
            '         CASE WHEN UNIT = ''nm'' THEN TRANS_DISTANCE * 1.852 ELSE TRANS_DISTANCE END AS TRANS_DISTANCE, ' ||
            '         CASE WHEN UNIT = ''nm'' THEN ''km'' ELSE UNIT END AS UNIT ' ||
            '  FROM BG00MAC102.T_ADS_FACT_RAW_MATERIAL_TRANSPORTATION ' ||
            '  WHERE ORG_CODE = ''' || V_COMPANY_ESC || ''' ' ||
            '    AND YEAR = ''2024'' ' ||
            '    AND BIGCLASS_NAME IN (''副原料'',''铁矿石'',''铁合金'',''废钢'') ' ||
            '), TRANS_OTHER AS ( ' ||
            '  SELECT O.ITEM_CODE, O.ITEM_NAME, O.CLASS_CODE, A.TRANS_MTHD_CODE, A.TRANS_DISTANCE, A.UNIT ' ||
            '  FROM OTHER O JOIN TRANS_AVG A ON O.CLASS_CODE = A.CLASS_CODE ' ||
            '), TRANS AS ( ' ||
            '  SELECT ITEM_CODE, ITEM_NAME, ' ||
            '         CASE ' ||
            '           WHEN TRANS_MTHD_CODE = ''carTrans''   THEN ''汽运'' ' ||
            '           WHEN TRANS_MTHD_CODE = ''riversTrans'' THEN ''河运'' ' ||
            '           WHEN TRANS_MTHD_CODE = ''seaTrans''   THEN ''海运'' ' ||
            '           WHEN TRANS_MTHD_CODE = ''railwayTrans'' THEN ''铁运'' ' ||
            '           ELSE TRANS_MTHD_CODE ' ||
            '         END AS NAME, ' ||
            '         TRANS_DISTANCE, UNIT ' ||
            '  FROM (SELECT * FROM RAW UNION ALL SELECT * FROM TRANS_OTHER) X ' ||
            '), FACTOR_TRANSPORT1 AS ( ' ||
            '  SELECT NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE ' ||
            '  FROM ' || V_TMP_SCHEMA || '.' || V_FACTOR_LIB || ' ' ||
            '  WHERE VERSION = ''' || V_VER_ESC || ''' ' ||
            '    AND PCR = ''' || V_PCR || ''' ' ||
            '    AND NAME IN (''海运'',''河运'',''铁运'',''汽运'',''柴油'') ' ||
            '    AND LCI_ELEMENT_VALUE <> 0 ' ||
            ') ' ||
            'SELECT T.ITEM_CODE, T.ITEM_NAME, F.LCI_ELEMENT_CODE, F.LCI_ELEMENT_NAME, ' ||
            '       SUM(T.TRANS_DISTANCE * F.LCI_ELEMENT_VALUE / 1000) AS LCI_ELEMENT_VALUE ' ||
            'FROM TRANS T ' ||
            'JOIN FACTOR_TRANSPORT1 F ON T.NAME = F.NAME ' ||
            'JOIN LCI L ON F.LCI_ELEMENT_CODE = L.LCI_ELEMENT_CODE ' ||
            'GROUP BY T.ITEM_CODE, T.ITEM_NAME, F.LCI_ELEMENT_CODE, F.LCI_ELEMENT_NAME';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_TRANSPORT', V_QUERY_STR);

    ----------------------------------------------------------------------------
    -- FACTOR_FCP / FACTOR_SY: share the same filtered MAT view of factor library
    ----------------------------------------------------------------------------
    SET V_QUERY_STR =
            'WITH LCI AS ( ' ||
            '  SELECT LCI_ELEMENT_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_LCI_LIST ' ||
            '), MAT_LIB AS ( ' ||
            '  SELECT UUID, NAME AS MAT_NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_NAME, LCI_ELEMENT_VALUE ' ||
            '  FROM ' || V_TMP_SCHEMA || '.' || V_FACTOR_LIB || ' ' ||
            '  WHERE VERSION = ''' || V_VER_ESC || ''' ' ||
            '    AND FLAG = ''MAT'' ' ||
            '    AND PCR = ''' || V_PCR || ''' ' ||
            '    AND LCI_ELEMENT_VALUE <> 0 ' ||
            '), ITEMS AS ( ' ||
            '  SELECT ITEM_CODE FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_ITEM_BASE ' ||
            ') ' ||
            'SELECT A.ITEM_CODE, B.MAT_NAME, B.LCI_ELEMENT_CODE, B.LCI_ELEMENT_NAME, B.LCI_ELEMENT_VALUE ' ||
            'FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_UUID_MAT A ' ||
            'JOIN MAT_LIB B ON A.UUID = B.UUID ' ||
            'JOIN LCI C ON B.LCI_ELEMENT_CODE = C.LCI_ELEMENT_CODE ' ||
            'JOIN ITEMS I ON A.ITEM_CODE = I.ITEM_CODE';

    CALL BG00MAC102.P_CREATE_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB, 'FACTOR_MAT', V_QUERY_STR);

END;
