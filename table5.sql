-- auto-generated definition

drop table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV_SUM;

create table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV_SUM
(
    REC_ID                VARCHAR(64),
    SUBCLASS_TAB_NAME     VARCHAR(64),
    COMPANY_CODE          VARCHAR(8),
    BATCH_NUMBER          VARCHAR(64),
    MAIN_CAT_BATCH_NUMBER VARCHAR(64),
    MAT_NO                VARCHAR(64),
    MAT_TRACK_NO          VARCHAR(64),
    MAT_SEQ_NO            BIGINT,
    FAMILY_CODE           VARCHAR(100),
    UNIT_CODE             VARCHAR(100),
    UNIT_NAME             VARCHAR(256),
    PRODUCT_CODE          VARCHAR(100),
    PRODUCT_NAME          VARCHAR(256),
    PRODUCT_VALUE         DECIMAL(27, 6),
    SOURCE_FAMILY_CODE    VARCHAR(100),
    SOURCE_UNIT_CODE      VARCHAR(100),
    SOURCE_UNIT_NAME      VARCHAR(256),
    TYPE_CODE             VARCHAR(20),
    TYPE_NAME             VARCHAR(64),
    C1                    DOUBLE,
    C2                    DOUBLE,
    C3                    DOUBLE,
    C4                    DOUBLE,
    C5                    DOUBLE,
    C_CYCLE               DOUBLE,
    FLAG                  VARCHAR(32),
    REC_CREATOR           VARCHAR(32),
    REC_CREATE_TIME       VARCHAR(32),
    REC_REVISOR           VARCHAR(32),
    REC_REVISE_TIME       VARCHAR(32)
)
    distribute by hash (REC_ID);

comment on table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_RESULT_DIST_PREV_SUM is 'LCA细类按物料分类结果含上游工序';