CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_RECURSION_DT(
    IN V_PCR VARCHAR(6),
    IN V_COMPANY_CODE VARCHAR(6),
    IN V_FACTOR_VERSION VARCHAR(100),
    IN V_CERTIFICATION_NUMBER VARCHAR(100),
    IN V_SUBCLASS_TAB_NAME VARCHAR(100)
)
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_RECURSION_DT
    LANGUAGE SQL
    NOT DETERMINISTIC
    EXTERNAL ACTION
    MODIFIES SQL DATA
    CALLED ON NULL INPUT
    INHERIT SPECIAL REGISTERS
    OLD SAVEPOINT LEVEL

BEGIN

    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102';
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_CALC_DT';
    DECLARE V_QUERY_STR CLOB(1 M);

    DECLARE V_RANK INT DEFAULT 1;
    DECLARE V_MAX_RANK INT;
    DECLARE V_TAB_COUNT INTEGER DEFAULT 0;
    DECLARE V_FACTOR_LIST VARCHAR(100);
    DECLARE V_FACTOR_LIB VARCHAR(100);

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
        INDEX_CODE VARCHAR(1200),
        TYPE_CODE VARCHAR(20),
        TYPE_NAME VARCHAR(64),
        ITEM_CODE VARCHAR(100),
        ITEM_NAME VARCHAR(256),
        VALUE DECIMAL(27, 6),
        UNITM_AC VARCHAR(64),
        UNIT_COST DOUBLE,
        FLAG VARCHAR(32),
        LCI_ELEMENT_CODE VARCHAR(256),
        C1 DOUBLE,
        C2 DOUBLE,
        C3 DOUBLE,
        C4 DOUBLE,
        C5 DOUBLE
        ) ON COMMIT PRESERVE ROWS;

    DECLARE GLOBAL TEMPORARY TABLE SESSION.RESULT_BUFFER (
        INDEX_CODE VARCHAR(1200),
        TYPE_CODE VARCHAR(20),
        TYPE_NAME VARCHAR(64),
        ITEM_CODE VARCHAR(100),
        ITEM_NAME VARCHAR(256),
        VALUE DECIMAL(27, 6),
        UNITM_AC VARCHAR(64),
        UNIT_COST DOUBLE,
        FLAG VARCHAR(32),
        LCI_ELEMENT_CODE VARCHAR(256),
        C1 DOUBLE,
        C2 DOUBLE,
        C3 DOUBLE,
        C4 DOUBLE,
        C5 DOUBLE
        ) ON COMMIT PRESERVE ROWS;

    IF V_PCR = 'NORM' THEN
        SET V_FACTOR_LIST = 'T_ADS_WH_LCA_EPD_NORM_FACTOR_VERSION';
        SET V_FACTOR_LIB = 'T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_NORM';
    ELSEIF V_PCR = 'CONS' THEN
        SET V_FACTOR_LIST = 'T_ADS_WH_LCA_EPD_CONS_FACTOR_VERSION';
        SET V_FACTOR_LIB = 'T_ADS_WH_LCA_FACTOR_LIBRARY_LCI_CONS';
    ELSE
        RETURN;
    END IF;

    SET V_QUERY_STR = 'DELETE FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_DT_RESULT WHERE
                       SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || '''
                       AND PCR = ''' || V_PCR || '''
                       AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND CERTIFICATION_NUMBER = ''' || V_CERTIFICATION_NUMBER || '''
                       AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'DELETE FROM BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_DT_RESULT_DIST WHERE
                       SUBCLASS_TAB_NAME = ''' || V_SUBCLASS_TAB_NAME || '''
                       AND PCR = ''' || V_PCR || '''
                       AND COMPANY_CODE = ''' || V_COMPANY_CODE || '''
                       AND CERTIFICATION_NUMBER = ''' || V_CERTIFICATION_NUMBER || '''
                       AND FACTOR_VERSION = ''' || V_FACTOR_VERSION || '''';
    PREPARE stmt FROM V_QUERY_STR;
    EXECUTE stmt;


    SET V_QUERY_STR = 'SELECT DISTINCT MAX(RANK) AS MAX_RANK
                       FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_SEQ';
    CALL BG00MAC102.P_EXEC_INTO_C(V_QUERY_STR, V_MAX_RANK);

    SET V_RANK = 1;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
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
            SELECT INDEX_CODE,
                   TYPE_CODE,
                   TYPE_NAME,
                   ITEM_CODE,
                   ITEM_NAME,
                   VALUE,
                   UNITM_AC,
                   UNIT_COST,
                   NULL      AS FLAG,
                   LCI_ELEMENT_CODE,
                   SUM(C1)   AS C1,
                   SUM(C2)   AS C2,
                   SUM(C3)   AS C3,
                   SUM(C4)   AS C4,
                   SUM(C5)   AS C5
            FROM (SELECT A.*
                   FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DIST A
                            JOIN SESSION.RANK_PROC B ON A.INDEX_CODE = B.INDEX_CODE)
            GROUP BY INDEX_CODE, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME,
                     VALUE, UNITM_AC, UNIT_COST, LCI_ELEMENT_CODE';
            PREPARE STMT FROM V_QUERY_STR;
            EXECUTE STMT;


            SET V_QUERY_STR = 'INSERT INTO SESSION.DIST_BUFFER
                                   SELECT im.INDEX_CODE,
                                          dist.TYPE_CODE,
                                          dist.TYPE_NAME,
                                          dist.ITEM_CODE,
                                          dist.ITEM_NAME,
                                          im.VALUE * dist.UNIT_COST AS VALUE,
                                          dist.UNITM_AC,
                                          im.UNIT_COST * dist.UNIT_COST AS UNIT_COST,
                                          dist.FLAG,
                                          dist.LCI_ELEMENT_CODE,
                                          im.UNIT_COST * dist.C1 AS C1,
                                          im.UNIT_COST * dist.C2 AS C2,
                                          im.UNIT_COST * dist.C3 AS C3,
                                          im.UNIT_COST * dist.C4 AS C4,
                                          im.UNIT_COST * dist.C5 AS C5
                                   FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                                            WHERE FLAG = ''MAIN'') im
                                            JOIN (SELECT * FROM SESSION.RANK_PROC WHERE SUM_FLAG = 0) rp
                                                 ON im.INDEX_CODE = rp.INDEX_CODE
                                            JOIN SESSION.RESULT_BUFFER dist
                                                 ON rp.PREV_INDEX_CODE = dist.INDEX_CODE';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            SET V_QUERY_STR = 'INSERT INTO SESSION.DIST_BUFFER
                                   SELECT im.INDEX_CODE,
                                          im.TYPE_CODE,
                                          im.TYPE_NAME,
                                          im.ITEM_CODE,
                                          im.ITEM_NAME,
                                          im.VALUE,
                                          im.UNITM_AC,
                                          im.UNIT_COST,
                                          ''SUM''                    AS FLAG,
                                          dist.LCI_ELEMENT_CODE,
                                          SUM(im.UNIT_COST * dist.C1) AS C1,
                                          SUM(im.UNIT_COST * dist.C2) AS C2,
                                          SUM(im.UNIT_COST * dist.C3) AS C3,
                                          SUM(im.UNIT_COST * dist.C4) AS C4,
                                          SUM(im.UNIT_COST * dist.C5) AS C5
                                   FROM (SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA
                                            WHERE FLAG = ''MAIN'') im
                                            JOIN (SELECT * FROM SESSION.RANK_PROC WHERE SUM_FLAG = 1) rp
                                                 ON im.INDEX_CODE = rp.INDEX_CODE
                                            JOIN SESSION.RESULT_BUFFER dist
                                                 ON rp.PREV_INDEX_CODE = dist.INDEX_CODE
                                   GROUP BY im.INDEX_CODE,
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

            INSERT INTO SESSION.RESULT_BUFFER (INDEX_CODE, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC,
                                               UNIT_COST, FLAG, LCI_ELEMENT_CODE, C1, C2, C3, C4, C5)
            SELECT INDEX_CODE,
                   TYPE_CODE,
                   TYPE_NAME,
                   ITEM_CODE,
                   ITEM_NAME,
                   SUM(VALUE),
                   UNITM_AC,
                   SUM(UNIT_COST),
                   FLAG,
                   LCI_ELEMENT_CODE,
                   SUM(C1),
                   SUM(C2),
                   SUM(C3),
                   SUM(C4),
                   SUM(C5)
            FROM SESSION.DIST_BUFFER
            GROUP BY INDEX_CODE, TYPE_CODE, TYPE_NAME, ITEM_CODE, ITEM_NAME, UNITM_AC, FLAG, LCI_ELEMENT_CODE;


            SET V_QUERY_STR = 'INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_DT_RESULT ' ||
                              ' (REC_ID, PCR, FACTOR_VERSION, ' ||
                              'COMPANY_CODE, CERTIFICATION_NUMBER, SUBCLASS_TAB_NAME, UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, ' ||
                              'MAT_SEQ_NO, MAT_WT, MAT_STATUS, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, ' ||
                              'DEPT_NAME, DEPT_CODE, DEPT_MID_NAME, LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME, C_CYCLE, C1, C2, C3, C4, C5, ' ||
                              'C_INSITE, C_OUTSITE, REC_CREATE_TIME)
                       SELECT HEX(RAND()),
                              ''' || V_PCR || ''',
                              ''' || V_FACTOR_VERSION || ''',
                              ''' || V_COMPANY_CODE || ''',
                              ''' || V_CERTIFICATION_NUMBER || ''',
                              ''' || V_SUBCLASS_TAB_NAME || ''',
                              UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              MAT_WT,
                              MAT_STATUS,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
                              DEPT_NAME,
                              DEPT_CODE,
                              DEPT_MID_NAME,
                              LCI_ELEMENT_CODE,
                              LCI_ELEMENT_CNAME,
                              SUM(C1) + SUM(C2) + SUM(C3) + SUM(C4) + SUM(C5),
                              SUM(C1),
                              SUM(C2),
                              SUM(C3),
                              SUM(C4),
                              SUM(C5),
                              SUM(C1) + SUM(C2),
                              SUM(C3) + SUM(C4) + SUM(C5),
                              TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')
                       FROM (SELECT A.*, B.LCI_ELEMENT_CNAME, C.DEPT_NAME, C.DEPT_CODE, C.DEPT_MID_NAME,
                                    D.UPDATE_DATE,
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
                                    D.PRODUCT_VALUE
                              FROM SESSION.RESULT_BUFFER A
                                       LEFT JOIN (SELECT LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME
                                                  FROM ' || V_TMP_SCHEMA || '.' || V_FACTOR_LIST ||
                              ' WHERE VERSION = ''' || V_FACTOR_VERSION || ''') B
                                                 ON A.LCI_ELEMENT_CODE = B.LCI_ELEMENT_CODE
                                       LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST D
                                          ON A.INDEX_CODE = D.INDEX_CODE
                                       LEFT JOIN (SELECT DISTINCT UNIT_CODE,
                                                                  DEPT_CODE,
                                                                  DEPT_NAME,
                                                                  DEPT_MID_NAME
                                                  FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022
                                                  WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || ''') C
                                          ON D.UNIT_CODE = C.UNIT_CODE)
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
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;


            SET V_QUERY_STR =
                    'INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_DT_RESULT_DIST (REC_ID, PCR, ' ||
                    'FACTOR_VERSION, COMPANY_CODE, CERTIFICATION_NUMBER, SUBCLASS_TAB_NAME, ' ||
                    'UPDATE_DATE, INDEX_CODE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO, MAT_WT, MAT_STATUS, FAMILY_CODE, ' ||
                    'UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_VALUE, TYPE_CODE, TYPE_NAME, ' ||
                    'ITEM_CODE, ITEM_NAME, VALUE, UNITM_AC, UNIT_COST, DEPT_NAME, DEPT_CODE, DEPT_MID_NAME, FLAG, ' ||
                    'LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME, C_CYCLE, C1, C2, C3, C4, C5, C_INSITE, C_OUTSITE, ' ||
                    'REC_CREATE_TIME)
             SELECT HEX(RAND()),
                    ''' || V_PCR || ''',
                              ''' || V_FACTOR_VERSION || ''',
                              ''' || V_COMPANY_CODE || ''',
                              ''' || V_CERTIFICATION_NUMBER || ''',
                              ''' || V_SUBCLASS_TAB_NAME || ''',
                              UPDATE_DATE,
                              INDEX_CODE,
                              MAT_NO,
                              MAT_TRACK_NO,
                              MAT_SEQ_NO,
                              MAT_WT,
                              MAT_STATUS,
                              FAMILY_CODE,
                              UNIT_CODE,
                              UNIT_NAME,
                              PRODUCT_CODE,
                              PRODUCT_NAME,
                              PRODUCT_VALUE,
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
                              FLAG,
                              LCI_ELEMENT_CODE,
                              LCI_ELEMENT_CNAME,
                              COALESCE(C1, 0) + COALESCE(C2, 0) + COALESCE(C3, 0) + COALESCE(C4, 0) + COALESCE(C5, 0),
                              COALESCE(C1, 0),
                              COALESCE(C2, 0),
                              COALESCE(C3, 0),
                              COALESCE(C4, 0),
                              COALESCE(C5, 0),
                              COALESCE(C1, 0) + COALESCE(C2, 0),
                              COALESCE(C3, 0) + COALESCE(C4, 0) + COALESCE(C5, 0),
                              TO_CHAR(CURRENT_TIMESTAMP, ''yyyyMMddHH24MI'')
                       FROM (SELECT A.*, B.LCI_ELEMENT_CNAME, C.DEPT_NAME, C.DEPT_CODE, C.DEPT_MID_NAME,
                                    D.UPDATE_DATE,
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
                                    D.PRODUCT_VALUE
                              FROM SESSION.RESULT_BUFFER A
                                       LEFT JOIN (SELECT LCI_ELEMENT_CODE, LCI_ELEMENT_CNAME
                                                  FROM ' || V_TMP_SCHEMA || '.' || V_FACTOR_LIST ||
                    ' WHERE VERSION = ''' ||
                    V_FACTOR_VERSION || ''') B
                                                 ON A.LCI_ELEMENT_CODE = B.LCI_ELEMENT_CODE
                                       LEFT JOIN ' || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_PROC_PRODUCT_LIST D
                                          ON A.INDEX_CODE = D.INDEX_CODE
                                       LEFT JOIN (SELECT DISTINCT UNIT_CODE,
                                                                  DEPT_CODE,
                                                                  DEPT_NAME,
                                                                  DEPT_MID_NAME
                                                  FROM BG00MAC102.T_WH_LCA_UNIT_CODE_2022
                                                  WHERE COMPANY_CODE = ''' || V_COMPANY_CODE || ''') C
                                          ON D.UNIT_CODE = C.UNIT_CODE)
                       WHERE LCI_ELEMENT_CODE = ''GWP-total''';
            PREPARE stmt FROM V_QUERY_STR;
            EXECUTE stmt;

            INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
            VALUES ('seq', cast(V_MAX_RANK as VARCHAR(10)), cast(V_RANK as VARCHAR(10)), CURRENT_TIMESTAMP);

            SET V_RANK = V_RANK + 1;

        END WHILE;

    EXECUTE IMMEDIATE 'DROP TABLE SESSION.RANK_PROC';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.DIST_BUFFER';
    EXECUTE IMMEDIATE 'DROP TABLE SESSION.RESULT_BUFFER';


END

