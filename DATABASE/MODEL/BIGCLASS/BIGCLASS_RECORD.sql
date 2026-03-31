create table BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD
(
    BIGCLASS_REC_ID              VARCHAR(36)                               not null
        primary key,
    PCR                          VARCHAR(100)                              not null
        constraint FK_BIGCLASS_CALC_RECORD_PCR
            references BG00MAC102.T_ADS_DIM_LCA_PCR,
    DATABASE_VERSION             VARCHAR(100)                              not null
        constraint FK_BIGCLASS_CALC_RECORD_DATABASE_VERSION
            references BG00MAC102.T_ADS_DIM_LCA_DATABASE_VERSION,
    COMPANY_CODE                 VARCHAR(20)                               not null,
    START_YM                     VARCHAR(8),
    END_YM                       VARCHAR(8),
    CERTIFICATION_NUMBER         VARCHAR(100)    default NULL,
    USING_GREEN_ELECTRICITY      BOOLEAN         default FALSE             not null,
    GREEN_ELECTRICITY_PROPORTION DECIMAL(20, 10) default NULL,
    RECORD_TIME                  TIMESTAMP(6)    default CURRENT TIMESTAMP not null,
    IS_CURRENT                   BOOLEAN         default TRUE              not null,
    REMARK                       VARCHAR(255)    default NULL
)
    distribute by hash (BIGCLASS_REC_ID);

comment on table BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD is 'LCA大类产品计算记录';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.BIGCLASS_REC_ID is '大类记录ID';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.PCR is '产品种类规则';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.DATABASE_VERSION is '数据库版本';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.COMPANY_CODE is '基地代码';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.START_YM is '开始年月';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.END_YM is '结束年月';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.CERTIFICATION_NUMBER is '认证编号';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.USING_GREEN_ELECTRICITY is '是否使用绿电';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.GREEN_ELECTRICITY_PROPORTION is '绿电比例';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.RECORD_TIME is '存证时间';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.IS_CURRENT is '是否为最新';

comment on column BG00MAC102.T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD.REMARK is '备注';










