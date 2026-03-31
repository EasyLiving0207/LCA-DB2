DROP TABLE BG00MAC102.T_ADS_DIM_LCA_PCR;

CREATE TABLE BG00MAC102.T_ADS_DIM_LCA_PCR
(
    PCR                   VARCHAR(100) NOT NULL PRIMARY KEY,
    PCR_NAME              VARCHAR(255) NOT NULL,
    PCR_CNAME             VARCHAR(255),
    PRODUCT_CATEGORY_CODE VARCHAR(255),
    PCR_VERSION           VARCHAR(100),
    REGISTER_NUMBER       VARCHAR(100),
    VALIDATE_FROM         DATE,
    VALIDATE_UNTIL        DATE,
    EPD_VALIDITY_PERIOD   VARCHAR(100),
    REFERENCE_STANDARD    VARCHAR(255),
    DESCRIPTION           VARCHAR(5000),
    PCR_URL               VARCHAR(500),
    PUBLISHER             VARCHAR(255),
    CONTACT               VARCHAR(255),
    IS_ACTIVE             BOOLEAN DEFAULT TRUE
) DISTRIBUTE BY HASH (PCR) ORGANIZE BY ROW;

-- CREATE UNIQUE INDEX IDX_DATABASE_VERSION ON BG00MAC102.T_ADS_DIM_LCA_PCR (DATABASE_VERSION_ID, DATABASE_VERSION);

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_PCR (PCR, PCR_NAME, PCR_CNAME, PRODUCT_CATEGORY_CODE, PCR_VERSION, REGISTER_NUMBER,
                                          VALIDATE_FROM, VALIDATE_UNTIL, EPD_VALIDITY_PERIOD, REFERENCE_STANDARD,
                                          DESCRIPTION, PCR_URL, PUBLISHER, CONTACT)
VALUES ('CONS', 'Construction Steel and Steel Structural Products', '建筑用钢及钢结构产品',
        'UN CPC 412 | UN CPC 421', '2.0', '2024:01', '2025-06-18',
        '2030-06-17', '3 years',
        'EN15804:2012+A2:2019/AC:2021 | ISO:14025 | ISO/TS:14027 | ISO:14040/14044',
        '本 PCR 包含基于 EN 15804 标准的环境产品声明（EPD）的要求。本 PCR 适用于建筑用钢及钢结构产品，建筑用钢通常可分为钢结构用钢和钢筋混凝土结构用钢。钢结构用钢主要有普通碳素结构钢和低合金结构钢。 产品交付状态包括型钢、 钢板、 钢管和钢筋等。 基于本 PCR 开发的 EPD，可用于披露符合 EN 15804:2012+A2:2019/AC:2021 要求的建筑用钢及钢结构产品的环境影响。',
        'https://www.cisa-epd.com/pcr', 'EPD China', 'EPD@chinaisa.org.cn');

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_PCR (PCR, PCR_NAME, PCR_CNAME, PRODUCT_CATEGORY_CODE, PCR_VERSION, REGISTER_NUMBER,
                                          VALIDATE_FROM, VALIDATE_UNTIL, EPD_VALIDITY_PERIOD, REFERENCE_STANDARD,
                                          DESCRIPTION, PCR_URL, PUBLISHER, CONTACT)
VALUES ('BASIC', 'Basic and Special Steel Products', '普通钢铁产品及特殊钢产品',
        'UN CPC 4112 | UN CPC 412', '1.0', '2022:01', '2022-03-04',
        '2027-03-04', '3-5 years',
        'ISO:14025 | ISO/TS:14027 | ISO:14040/14044 | ISO 20915',
        '本 PCR 适用于半成品钢或中间钢产品，包括粗钢、普通钢铁产品和特殊钢产品（以下简称钢产品） 。钢产品按照生产工序、外形、尺寸和表面状态可分为：液态钢、钢锭和半成品、扁平产品、长材和其它产品（见 GB/T 15574）。',
        'https://www.cisa-epd.com/pcr', 'EPD China', 'EPD@chinaisa.org.cn');