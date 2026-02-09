create table BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY
(
    REC_ID                    VARCHAR(16)    default '',
    PUBLISH_DATE              DATE,
    BEGIN_DATE                VARCHAR(6)     default '',
    END_DATE                  VARCHAR(6)     default '',
    SOURCE                    VARCHAR(64)    default '',
    VER_CODE                  VARCHAR(20)    default '',
    SOURCE_ITEM_MAIN_CAT_CODE VARCHAR(64)    default '',
    SOURCE_ITEM_MAIN_CAT      VARCHAR(64)    default '',
    SOURCE_ITEM_SUB_CAT_CODE  VARCHAR(64)    default '',
    SOURCE_ITEM_SUB_CAT       VARCHAR(150)   default '',
    IS_PRODUCTION             BOOLEAN,
    SOURCE_ITEM_CODE          VARCHAR(64)    default '',
    SOURCE_ITEM_NAME          VARCHAR(256)   default '',
    SOURCE_ITEM_ENAME         VARCHAR(256)   default '',
    NCV                       DECIMAL(24, 6) default 0,
    NCV_UNIT                  VARCHAR(64)    default '',
    C_CONTENT                 DECIMAL(24, 6),
    C_CONTENT_UNIT            VARCHAR(64)    default '',
    OXIDATION_RATE            DECIMAL(24, 6),
    EF                        DECIMAL(24, 8) default NCV * C_CONTENT,
    EF_OXIDATION              DECIMAL(24, 8) default,
    EF_UNIT                   VARCHAR(64)    default '',
    REMARK                    VARCHAR(256)   default '',
    CO2_CONTENT               DECIMAL(24, 6),
    ITEM_ID                 VARCHAR(50) ,
    REC_CREATOR               VARCHAR(32)    default '',
    REC_CREATE_TIME           VARCHAR(32)    default '',
    REC_REVISOR               VARCHAR(32)    default '',
    REC_REVISE_TIME           VARCHAR(32)    default ''
)
    distribute by hash (REC_ID);



comment on table BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY is '直排因子库表';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.REC_ID is '记录ID';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.PUBLISH_DATE is '发布日期';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.BEG_DATE is '起始生效时间';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.END_DATE is '结束生效时间';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE is '来源';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.VER_CODE is '版本代码';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_MAIN_CAT_CODE is '来源项主分类代码';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_MAIN_CAT is '来源项主分类';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_SUB_CAT_CODE is '来源项次分类代码';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_SUB_CAT is '来源项次分类';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.DATA_TYPE is '数据类型';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_CODE is '因子子代码';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_NAME is '因子名称';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.SOURCE_ITEM_ENAME is '因子英文名';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.NCV is '低位发热量';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.NCV_UNIT is '低位发热量单位';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.C_CONTENT is '含碳量';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.C_CONTENT_UNIT is '含碳量单位';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.OXIDATION_RATE is '氧化因子';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.EF is '碳排放因子';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.EF_OXIDATION is '碳排放因子（计算氧化因子）';
comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.EF_UNIT is '碳排放因子单位';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.REMARK is '备注';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.REC_CREATOR is '记录创建人';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.REC_CREATE_TIME is '记录创建时间';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.REC_REVISOR is '记录修改人';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.REC_REVISE_TIME is '记录修改时刻';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.CO2_CONTENT is '含CO2量';

comment on column BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY.ITEM_CODE is '因子代码';

grant select on table BG00MAC102.T_ADS_WH_ZP_FACTOR_LIBRARY to BG00MAC102;

