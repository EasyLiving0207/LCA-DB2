WITH TAB_COL AS (SELECT TABNAME,
                        COLNAME,
                        REMARKS,
                        TYPENAME,
                        LENGTH,
                        SCALE,
                        NULLS,
                        COLNO
                 FROM SYSCAT.COLUMNS
                 WHERE TABSCHEMA = 'BG00MAC102'
                   AND tabname IN ('T_ADS_DIM_LCA_PCR',
                                   'T_ADS_DIM_LCA_PCR_INDICATOR',
                                   'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                                   'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                                   'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                                   'T_ADS_DIM_LCA_IMPACT_METHOD',
                                   'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                                   'T_ADS_DIM_LCA_DATABASE_VERSION',
                                   'T_ADS_DIM_LCA_SYSTEM_MODEL',
                                   'T_ADS_DIM_LCA_DATASET',
                                   'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                                   'T_ADS_DIM_LCA_DATASET_PRODUCT',
                                   'T_ADS_DIM_LCA_GEOGRAPHY',
                                   'T_ADS_DIM_LCA_UNIT_PROCESS',
                                   'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                                   'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                                   'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                                   'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                                   'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                                   'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                                   'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                                   'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                                   'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                                   'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                                   'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                                   'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                                   'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                                   'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION')
                 ORDER BY TABNAME, COLNO),
     FK_COL AS (SELECT r.constname,
                       r.tabname,
                       fk_col.colname,
                       r.REFTABNAME,
                       pk_col.colname AS REFCOLNAME,
                       r.deleterule,
                       r.updaterule
                FROM syscat.references r
                         JOIN syscat.keycoluse fk_col
                              ON r.constname = fk_col.constname
                                  AND r.tabschema = fk_col.tabschema
                         JOIN syscat.keycoluse pk_col
                              ON r.refkeyname = pk_col.constname
                                  AND r.reftabschema = pk_col.tabschema
                WHERE r.tabname IN ('T_ADS_DIM_LCA_PCR',
                                    'T_ADS_DIM_LCA_PCR_INDICATOR',
                                    'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                                    'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                                    'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                                    'T_ADS_DIM_LCA_IMPACT_METHOD',
                                    'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                                    'T_ADS_DIM_LCA_DATABASE_VERSION',
                                    'T_ADS_DIM_LCA_SYSTEM_MODEL',
                                    'T_ADS_DIM_LCA_DATASET',
                                    'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                                    'T_ADS_DIM_LCA_DATASET_PRODUCT',
                                    'T_ADS_DIM_LCA_GEOGRAPHY',
                                    'T_ADS_DIM_LCA_UNIT_PROCESS',
                                    'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                                    'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                                    'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                                    'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                                    'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                                    'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                                    'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                                    'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION')
                ORDER BY r.tabname, r.constname, fk_col.colseq),
     PK_COL as (SELECT tabname,
                       colname,
                       colseq,
                       'Y' as PK
                FROM syscat.keycoluse
                WHERE constname IN (SELECT constname
                                    FROM syscat.tabconst
                                    WHERE type = 'P')
                  AND tabname IN ('T_ADS_DIM_LCA_PCR',
                                  'T_ADS_DIM_LCA_PCR_INDICATOR',
                                  'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                                  'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                                  'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                                  'T_ADS_DIM_LCA_IMPACT_METHOD',
                                  'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                                  'T_ADS_DIM_LCA_DATABASE_VERSION',
                                  'T_ADS_DIM_LCA_SYSTEM_MODEL',
                                  'T_ADS_DIM_LCA_DATASET',
                                  'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                                  'T_ADS_DIM_LCA_DATASET_PRODUCT',
                                  'T_ADS_DIM_LCA_GEOGRAPHY',
                                  'T_ADS_DIM_LCA_UNIT_PROCESS',
                                  'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                                  'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                                  'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                                  'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                                  'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                                  'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                                  'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                                  'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                                  'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                                  'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                                  'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                                  'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                                  'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                                  'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION')
                ORDER BY tabname, colseq),
     COL_REMARKS AS (SELECT * FROM T_TEMP_LCA_COLNAME_REMARKS),
     TABS AS (SELECT TABNAME, REMARKS AS TABREMARKS
              FROM SYSCAT.TABLES
              WHERE TABSCHEMA = 'BG00MAC102'),
     TAB_LIST AS (select TAB_COL.TABNAME,
                         TABS.TABREMARKS,
                         TAB_COL.COLNAME,
                         TAB_COL.REMARKS,
--        COL_REMARKS.REMARKS                                    AS REMARKS_DEFAULT,
--        CASE
--            WHEN TAB_COL.REMARKS = COL_REMARKS.REMARKS THEN TRUE
--            ELSE FALSE END                                     AS REMARKS_CONSIST,
                         TAB_COL.TYPENAME,
                         TAB_COL.LENGTH,
                         TAB_COL.SCALE,
                         TAB_COL.NULLS,
                         PK_COL.PK,
                         TAB_COL.COLNO + 1                                      AS COLNO,
                         FK_COL.REFTABNAME,
                         FK_COL.REFCOLNAME,
                         CONSTNAME,
                         DELETERULE,
                         UPDATERULE,
                         COUNT(REFTABNAME) OVER ( PARTITION BY TAB_COL.TABNAME) AS REFTABCOUNT
                  from TAB_COL
                           LEFT JOIN PK_COL ON TAB_COL.TABNAME = PK_COL.TABNAME
                      AND TAB_COL.COLNAME = PK_COL.COLNAME
                           LEFT JOIN FK_COL ON TAB_COL.TABNAME = FK_COL.TABNAME
                      AND TAB_COL.COLNAME = FK_COL.COLNAME
                           LEFT JOIN COL_REMARKS ON TAB_COL.COLNAME = COL_REMARKS.COLNAME
                           LEFT JOIN TABS ON TAB_COL.TABNAME = TABS.TABNAME
                  ORDER BY REFTABCOUNT, TABNAME, COLNO)
SELECT TAB_LIST.*, SEQ.SEQ_NO
FROM TAB_LIST
         LEFT JOIN T_TEMP_LCA_TAB_SEQUENCE SEQ ON TAB_LIST.TABNAME = SEQ.TABNAME
ORDER BY SEQ_NO, COLNO
;



create table BG00MAC102.T_TEMP_LCA_COLNAME_REMARKS
(
    COLNAME VARCHAR(255),
    REMARKS VARCHAR(255)
);

CREATE TABLE T_TEMP_LCA_TAB_SEQUENCE
(

    SEQ_NO  INTEGER      NOT NULL,
    TABNAME VARCHAR(100) NOT NULL,

    CONSTRAINT PK_T_TEMP_LCA_TAB_SEQUENCE
        PRIMARY KEY (SEQ_NO)

);


create table BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION
(
    DATABASE_VERSION VARCHAR(100) not null
        primary key,
    DATABASE_NAME    VARCHAR(100) not null,
    FULL_VERSION     VARCHAR(20)  not null,
    RELEASE_TIME     VARCHAR(100),
    SOURCE           VARCHAR(100),
    DESCRIPTION      VARCHAR(100),
    IS_ACTIVE        BOOLEAN default TRUE
)
    distribute by hash (DATABASE_VERSION);

comment on table BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION is 'LCA数据库版本';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.DATABASE_VERSION is '数据库版本';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.DATABASE_NAME is '数据库名称';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.FULL_VERSION is '版本号';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.RELEASE_TIME is '发布时间';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.SOURCE is '来源';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.DESCRIPTION is '描述';

comment on column BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION.IS_ACTIVE is '是否生效';





