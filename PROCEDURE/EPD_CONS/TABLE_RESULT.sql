DROP TABLE BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_TA_HP0001;

create table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_TA_HP0001
(
    REC_ID                VARCHAR(64),
    SUBCLASS_TAB_NAME     VARCHAR(64),
    COMPANY_CODE          VARCHAR(8),
    MAIN_CAT_BATCH_NUMBER VARCHAR(64),
    UPDATE_DATE           VARCHAR(6),
    INDEX_CODE            VARCHAR(1000),
    MAT_NO                VARCHAR(64),
    MAT_TRACK_NO          VARCHAR(64),
    MAT_SEQ_NO            BIGINT,
    FAMILY_CODE           VARCHAR(1000),
    UNIT_CODE             VARCHAR(100),
    UNIT_NAME             VARCHAR(256),
    PRODUCT_CODE          VARCHAR(100),
    PRODUCT_NAME          VARCHAR(256),
    PRODUCT_VALUE         DECIMAL(27, 6),
    LCI_ELEMENT_CODE      VARCHAR(256),
    LCI_ELEMENT_CNAME     VARCHAR(256),
    C_CYCLE               DOUBLE,
    C1                    DOUBLE,
    C2                    DOUBLE,
    C3                    DOUBLE,
    C4                    DOUBLE,
    C5                    DOUBLE,
    C_INSITE              DOUBLE,
    C_OUTSITE             DOUBLE,
    REC_CREATOR           VARCHAR(32),
    REC_CREATE_TIME       VARCHAR(32),
    REC_REVISOR           VARCHAR(32),
    REC_REVISE_TIME       VARCHAR(32)
)
    distribute by hash (REC_ID);

comment on table BG00MAC102.T_ADS_FACT_LCA_SUBCLASS_EPD_CONS_MONTH_RESULT_TA_HP0001 is 'EPD宝山细类月度结果';


COMMIT;
