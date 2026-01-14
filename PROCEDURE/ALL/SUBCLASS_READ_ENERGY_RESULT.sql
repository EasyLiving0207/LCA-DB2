CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_READ_ENERGY_RESULT(
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
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_READ_ENERGY_RESULT
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

    -- Derived helpers
    DECLARE V_UPDATE_SRC VARCHAR(400);
    DECLARE V_LIKE_TAIL VARCHAR(300);

    -- Escape user-provided strings for dynamic SQL safety
    DECLARE V_COMPANY_ESC VARCHAR(200);
    DECLARE V_FACTOR_ESC VARCHAR(400);
    DECLARE V_BATCHNO_ESC VARCHAR(400);

    SET V_COMPANY_ESC = REPLACE(COALESCE(V_COMPANY_CODE, ''), '''', '''''');
    SET V_FACTOR_ESC = REPLACE(COALESCE(V_FACTOR_VERSION, ''), '''', '''''');
    SET V_BATCHNO_ESC = REPLACE(COALESCE(V_MAIN_CAT_BATCH_NUMBER, ''), '''', '''''');
    SET V_UPDATE_SRC = V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATA';


    IF V_AUTO_BATCH THEN
        -- Build the LIKE tail by PCR + suffix
        IF V_PCR = 'NORM' THEN
            IF V_BATCH_SUFFIX IS NOT NULL THEN
                SET V_LIKE_TAIL = '%YS_' || V_BATCH_SUFFIX;
            ELSE
                SET V_LIKE_TAIL = '%YS';
            END IF;
        ELSEIF V_PCR = 'CONS' THEN
            IF V_BATCH_SUFFIX IS NOT NULL THEN
                SET V_LIKE_TAIL = '%YS_' || V_BATCH_SUFFIX || '_CONS';
            ELSE
                SET V_LIKE_TAIL = '%YS_CONS';
            END IF;
        ELSE
            -- Fallback: behave like NORM if PCR is unexpected
            SET V_LIKE_TAIL = '%YS';
        END IF;

        -- Use MAX(batch_number) to pick the latest (instead of DISTINCT + RANK window)
        SET V_QUERY_STR =
                'WITH UPDATE_DATE AS ( ' ||
                '    SELECT DISTINCT UPDATE_DATE ' ||
                '    FROM ' || V_UPDATE_SRC || ' ' ||
                '), MAIN_BATCH AS ( ' ||
                '    SELECT DISTINCT BATCH_NUMBER ' ||
                '    FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_TAB_NAME || ' ' ||
                '    WHERE COMPANY_CODE = ''' || V_COMPANY_ESC || ''' ' ||
                '      AND FACTOR_VERSION = ''' || V_FACTOR_ESC || ''' ' ||
                '), PICKED AS ( ' ||
                '    SELECT U.UPDATE_DATE, ' ||
                '           MAX(B.BATCH_NUMBER) AS BATCH_NUMBER ' ||
                '    FROM UPDATE_DATE U ' ||
                '    LEFT JOIN MAIN_BATCH B ' ||
                '      ON B.BATCH_NUMBER LIKE (U.UPDATE_DATE || U.UPDATE_DATE || ''' || V_LIKE_TAIL || ''') ' ||
                '    GROUP BY U.UPDATE_DATE ' ||
                ') ' ||
                'SELECT UPDATE_DATE, ' ||
                CASE
                    WHEN V_MAIN_CAT_BATCH_NUMBER IS NOT NULL THEN
                        '       COALESCE(BATCH_NUMBER, ''' || V_BATCHNO_ESC || ''') AS BATCH_NUMBER '
                    ELSE
                        '       BATCH_NUMBER '
                    END ||
                'FROM PICKED ' ||
                'ORDER BY UPDATE_DATE';

        CALL BG00MAC102.P_CREATE_TEMP_TABLE(
                V_TMP_SCHEMA,
                V_TMP_TAB,
                'DATE_BATCH',
                V_QUERY_STR
             );

        SET V_QUERY_STR =
                'SELECT * FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_TAB_NAME || ' ' ||
                'WHERE BATCH_NUMBER IN (SELECT DISTINCT BATCH_NUMBER FROM '
                    || V_TMP_SCHEMA || '.' || V_TMP_TAB || '_DATE_BATCH) ' ||
                '  AND COMPANY_CODE = ''' || V_COMPANY_ESC || ''' ' ||
                '  AND FACTOR_VERSION = ''' || V_FACTOR_ESC || ''' ' ||
                '  AND LCI_ELEMENT_CODE IS NOT NULL';

        CALL BG00MAC102.P_CREATE_TEMP_TABLE(
                V_TMP_SCHEMA,
                V_TMP_TAB,
                'ENERGY_RESULT',
                V_QUERY_STR
             );
    ELSE

        SET V_QUERY_STR =
                'SELECT DISTINCT UPDATE_DATE, ''' || V_BATCHNO_ESC || ''' AS BATCH_NUMBER ' ||
                ' FROM ' || V_UPDATE_SRC || '';

        CALL BG00MAC102.P_CREATE_TEMP_TABLE(
                V_TMP_SCHEMA,
                V_TMP_TAB,
                'DATE_BATCH',
                V_QUERY_STR
             );

        -- Non-auto-batch path unchanged, just quote-escaped values
        SET V_QUERY_STR =
                'SELECT * ' ||
                'FROM ' || V_TMP_SCHEMA || '.' || V_MAIN_CAT_TAB_NAME || ' ' ||
                'WHERE BATCH_NUMBER = ''' || V_BATCHNO_ESC || ''' ' ||
                '  AND COMPANY_CODE = ''' || V_COMPANY_ESC || ''' ' ||
                '  AND FACTOR_VERSION = ''' || V_FACTOR_ESC || ''' ' ||
                '  AND LCI_ELEMENT_CODE IS NOT NULL';

        CALL BG00MAC102.P_CREATE_TEMP_TABLE(
                V_TMP_SCHEMA,
                V_TMP_TAB,
                'ENERGY_RESULT',
                V_QUERY_STR
             );

    END IF;

END;
