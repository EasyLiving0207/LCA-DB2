create table BG00MAC102.T_ADS_WH_LCA_BACKGROUND_FACTOR_LIBRARY
(
    REC_ID            VARCHAR(32) default NULL,
    PCR               VARCHAR(32),
    VERSION           VARCHAR(100),
    UUID              VARCHAR(32),
    NAME              VARCHAR(100),
    BACKGROUND        VARCHAR(500),
    UNIT              VARCHAR(50),
    FLAG              VARCHAR(32),
    LCI_ELEMENT_CODE  VARCHAR(32),
    LCI_ELEMENT_NAME  VARCHAR(500),
    LCI_ELEMENT_CNAME VARCHAR(500),
    LCI_ELEMENT_VALUE DECIMAL(30, 16),
    REC_CREATOR       VARCHAR(32),
    REC_CREATE_TIME   VARCHAR(32),
    REC_REVISOR       VARCHAR(32),
    REC_REVISE_TIME   VARCHAR(32),
    REMARK            VARCHAR(100)
)
    distribute by hash (REC_ID);



create table BG00MAC102.T_ADS_WH_LCA_ITEM_CONTRAST
(
    REC_ID          VARCHAR(32) default '',
    PCR             VARCHAR(32) default NULL,
    VERSION         VARCHAR(100),
    COMPANY_CODE    VARCHAR(20),
    ITEM_CODE       VARCHAR(100),
    ITEM_NAME       VARCHAR(100),
    UUID            VARCHAR(32),
    FLAG            VARCHAR(32),
    REC_CREATOR     VARCHAR(32),
    REC_CREATE_TIME VARCHAR(32),
    REC_REVISOR     VARCHAR(32),
    REC_REVISE_TIME VARCHAR(32),
    REMARK          VARCHAR(100)
)
    distribute by hash (REC_ID);




