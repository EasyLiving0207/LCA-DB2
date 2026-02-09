DROP TABLE BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL;

CREATE TABLE BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL
(
    SYSTEM_MODEL      VARCHAR(255) NOT NULL,
    SYSTEM_MODEL_NAME VARCHAR(255) NOT NULL,
    DESCRIPTION       VARCHAR(1000),
    IS_ACTIVE         BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (SYSTEM_MODEL)
) DISTRIBUTE BY HASH (SYSTEM_MODEL);

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL (SYSTEM_MODEL, SYSTEM_MODEL_NAME, DESCRIPTION,
                                                   IS_ACTIVE)
VALUES ('undefined', 'Undefined',
        'Undefined unit processes are unlinked and multi-output, meaning no suppliers are determined and no allocation is applied.',
        DEFAULT);

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL (SYSTEM_MODEL, SYSTEM_MODEL_NAME, DESCRIPTION,
                                                   IS_ACTIVE)
VALUES ('cutoff', 'Allocation, cut-off by classification',
        'Attributional system model in which wastes are the producer’s responsibility (“polluter pays”), and there is an incentivisation to use recyclable products, that are available burden free (cut-off).',
        DEFAULT);

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL (SYSTEM_MODEL, SYSTEM_MODEL_NAME, DESCRIPTION,
                                                   IS_ACTIVE)
VALUES ('EN15804', 'Allocation, cut-off, EN15804',
        'Attributional interpretation of EN15804, ISO21930 and ISO14025, developed to support the needs of Environmental Product Declaration (EPD) practitioners.',
        DEFAULT);

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL (SYSTEM_MODEL, SYSTEM_MODEL_NAME, DESCRIPTION,
                                                   IS_ACTIVE)
VALUES ('apos', 'Allocation at the point of substitution',
        'Attributional system model in which the responsibility over wastes (burdens) are shared between producers and sub-sequent users of the valuable products generated in the treatment processes.',
        DEFAULT);

INSERT INTO BG00MAC102.T_ADS_DIM_LCA_SYSTEM_MODEL (SYSTEM_MODEL, SYSTEM_MODEL_NAME, DESCRIPTION,
                                                   IS_ACTIVE)
VALUES ('consequential', 'Substitution, consequential, long-term',
        'Consequential system model in which substitution is applied to resolve multi-functionality and demand is met by unconstrained or marginal suppliers.',
        DEFAULT);


