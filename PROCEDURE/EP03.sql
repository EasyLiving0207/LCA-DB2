drop table BG00MAC102.T_ADS_WH_LCA_SUBCLASS_CALC_JOB_INDEX;

create table BG00MAC102.T_ADS_WH_LCA_SUBCLASS_CALC_JOB_INDEX
(
    REC_ID                        VARCHAR(64),
    PCR                           VARCHAR(64),
    MODEL                         VARCHAR(64),
    PROCEDURE                     VARCHAR(256),
    COMPANY_CODE                  VARCHAR(8),
    START_MONTH                   VARCHAR(6),
    END_MONTH                     VARCHAR(6),
    FACTOR_YEAR                   VARCHAR(4),
    FACTOR_VERSION                VARCHAR(100),
    MAIN_CAT_TAB_NAME             VARCHAR(256),
    MAIN_CAT_BATCH_NUMBER         VARCHAR(64),
    BATCH_SUFFIX                  VARCHAR(64),
    SUBCLASS_PRODUCT_CAT          VARCHAR(64),
    SUBCLASS_TAB_NAME             VARCHAR(256),
    SUBCLASS_RESULT_TAB_NAME      VARCHAR(256),
    SUBCLASS_RESULT_DIST_TAB_NAME VARCHAR(256),
    JOB_DESC                      VARCHAR(256),
    JOB_START_TIME                VARCHAR(32),
    JOB_END_TIME                  VARCHAR(32),
    JOB_FINISHED                  BOOLEAN,
    REC_CREATOR                   VARCHAR(32),
    REC_CREATE_TIME               VARCHAR(32),
    REC_REVISOR                   VARCHAR(32),
    REC_REVISE_TIME               VARCHAR(32)
)
    distribute by hash (REC_ID);

comment on table BG00MAC102.T_ADS_WH_LCA_SUBCLASS_CALC_JOB_INDEX is '明细产品计算任务维护表';

SET CURRENT SCHEMA = BG00MAC102;


WITH EDGE AS (SELECT 'A' AS PARENT, 'B' AS CHILD, 0.5 AS UNIT_COST
              FROM SYSIBM.SYSDUMMY1
              UNION ALL
              SELECT 'B', 'C', 2.0
              FROM SYSIBM.SYSDUMMY1),
     RAW_IMPACT AS (SELECT 'A' AS NODE, 'GWP' AS LCI, 6.0 AS C1
                    FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 'A', 'GWP', 4.0
                    FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 'B', 'GWP', 3.0
                    FROM SYSIBM.SYSDUMMY1
                    UNION ALL
                    SELECT 'C', 'GWP', 1.0
                    FROM SYSIBM.SYSDUMMY1),
     DIRECT_IMPACT_AGG AS (SELECT NODE, LCI, SUM(C1) AS C1
                           FROM RAW_IMPACT
                           GROUP BY NODE, LCI),

     FLOW (NODE, LCI, C1, PATH) AS (

         -- Base: pre-aggregated seeds
         SELECT NODE,
                LCI,
                C1,
                CAST(NODE AS VARCHAR(100)) AS PATH
         FROM DIRECT_IMPACT_AGG

         UNION ALL

         -- Recursive propagation
         SELECT e.CHILD                  AS NODE,
                f.LCI,
                f.C1 * e.UNIT_COST       AS C1,
                f.PATH || '>' || e.CHILD AS PATH
         FROM FLOW f,
              EDGE e
         WHERE e.PARENT = f.NODE
           AND LOCATE('>' || e.CHILD || '>', '>' || f.PATH || '>') = 0)

SELECT *
FROM FLOW;
