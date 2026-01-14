CREATE OR REPLACE PROCEDURE BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_CALC(IN V_PCR VARCHAR(6),
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
                                                                    IN V_SUBCLASS_RESULT_DIST_TAB_NAME VARCHAR(100),
                                                                    IN V_SUBCLASS_FILTER_CONDITION VARCHAR(1000))
    SPECIFIC P_ADS_FACT_LCA_SUBCLASS_CALC
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

    DECLARE V_LOG_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --日志表所在SCHEMA
    DECLARE V_ROUTINE_NAME VARCHAR(128) DEFAULT 'P_ADS_FACT_LCA_SUBCLASS_CALC'; --存储过程名
    DECLARE V_PARM_INFO VARCHAR(4096) DEFAULT NULL;
    DECLARE SQLCODE INTEGER;
    DECLARE SQLSTATE CHAR (5);

    ------------------------------------日志变量定义------------------------------------
    DECLARE V_TMP_SCHEMA VARCHAR(32) DEFAULT 'BG00MAC102'; --临时表SCHEMA
    DECLARE V_TMP_TAB VARCHAR(50) DEFAULT 'T_ADS_TEMP_LCA_SUBCLASS_CALC'; --临时表名
    DECLARE V_QUERY_STR CLOB(1 M);
    --查询SQL
    --完整的临时表名
    ------------------------------------存储过程变量定义---------------------------------

    --开始时间
    SET V_START_TIMESTAMP = CURRENT_TIMESTAMP;

    --删除此存储过程创建的所有临时表（如果上次执行出错的话，有可能有些临时表没删）
    CALL BG00MAC102.P_DROP_TEMP_TABLE(V_TMP_SCHEMA, V_TMP_TAB);
    COMMIT;

    ------------------------------------处理逻辑(开始)------------------------------------

    TRUNCATE TABLE BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG IMMEDIATE;

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'begin', null, CURRENT_TIMESTAMP);

    --取活动数据
    CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_READ_ACTI_DATA(V_PCR,
                                                           V_COMPANY_CODE,
                                                           V_START_YM,
                                                           V_END_YM,
                                                           V_FACTOR_YEAR,
                                                           V_FACTOR_VERSION,
                                                           V_AUTO_BATCH,
                                                           V_BATCH_SUFFIX,
                                                           V_MAIN_CAT_TAB_NAME,
                                                           V_MAIN_CAT_BATCH_NUMBER,
                                                           V_SUBCLASS_TAB_NAME,
                                                           V_SUBCLASS_RESULT_TAB_NAME,
                                                           V_SUBCLASS_RESULT_DIST_TAB_NAME,
                                                           V_SUBCLASS_FILTER_CONDITION);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('data', 'end', null, CURRENT_TIMESTAMP);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('energy', 'begin', null, CURRENT_TIMESTAMP);

    CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_READ_ENERGY_RESULT(V_PCR,
                                                               V_COMPANY_CODE,
                                                               V_START_YM,
                                                               V_END_YM,
                                                               V_FACTOR_YEAR,
                                                               V_FACTOR_VERSION,
                                                               V_AUTO_BATCH,
                                                               V_BATCH_SUFFIX,
                                                               V_MAIN_CAT_TAB_NAME,
                                                               V_MAIN_CAT_BATCH_NUMBER,
                                                               V_SUBCLASS_TAB_NAME,
                                                               V_SUBCLASS_RESULT_TAB_NAME,
                                                               V_SUBCLASS_RESULT_DIST_TAB_NAME);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('energy', 'end', null, CURRENT_TIMESTAMP);


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'begin', null, CURRENT_TIMESTAMP);


    CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_READ_FACTOR(V_PCR,
                                                        V_COMPANY_CODE,
                                                        V_START_YM,
                                                        V_END_YM,
                                                        V_FACTOR_YEAR,
                                                        V_FACTOR_VERSION,
                                                        V_AUTO_BATCH,
                                                        V_BATCH_SUFFIX,
                                                        V_MAIN_CAT_TAB_NAME,
                                                        V_MAIN_CAT_BATCH_NUMBER,
                                                        V_SUBCLASS_TAB_NAME,
                                                        V_SUBCLASS_RESULT_TAB_NAME,
                                                        V_SUBCLASS_RESULT_DIST_TAB_NAME);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('factor', 'end', null, CURRENT_TIMESTAMP);


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('dist', 'begin', null, CURRENT_TIMESTAMP);


    CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_JOIN_DIST(V_PCR,
                                                      V_COMPANY_CODE,
                                                      V_START_YM,
                                                      V_END_YM,
                                                      V_FACTOR_YEAR,
                                                      V_FACTOR_VERSION,
                                                      V_AUTO_BATCH,
                                                      V_BATCH_SUFFIX,
                                                      V_MAIN_CAT_TAB_NAME,
                                                      V_MAIN_CAT_BATCH_NUMBER,
                                                      V_SUBCLASS_TAB_NAME,
                                                      V_SUBCLASS_RESULT_TAB_NAME,
                                                      V_SUBCLASS_RESULT_DIST_TAB_NAME);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('dist', 'end', null, CURRENT_TIMESTAMP);


    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('recursion', 'begin', null, CURRENT_TIMESTAMP);


    CALL BG00MAC102.P_ADS_FACT_LCA_SUBCLASS_RECURSION(V_PCR,
                                                      V_COMPANY_CODE,
                                                      V_START_YM,
                                                      V_END_YM,
                                                      V_FACTOR_YEAR,
                                                      V_FACTOR_VERSION,
                                                      V_AUTO_BATCH,
                                                      V_BATCH_SUFFIX,
                                                      V_MAIN_CAT_TAB_NAME,
                                                      V_MAIN_CAT_BATCH_NUMBER,
                                                      V_SUBCLASS_TAB_NAME,
                                                      V_SUBCLASS_RESULT_TAB_NAME,
                                                      V_SUBCLASS_RESULT_DIST_TAB_NAME);

    INSERT INTO BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_CALC_DEBUG(proc_name, step_desc, var_value, log_time)
    VALUES ('recursion', 'end', null, CURRENT_TIMESTAMP);

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

