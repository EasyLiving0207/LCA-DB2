-- DB2 LUW - Check Primary Key Information for LCA Tables

-- Version 1: Simple primary key list
SELECT tabname AS table_name,
       colname AS primary_key_column,
       colseq  AS column_sequence
FROM syscat.keycoluse
WHERE constname IN (SELECT constname
                    FROM syscat.tabconst
                    WHERE type = 'P')
  AND tabname IN (
                  'T_ADS_DIM_LCA_PCR',
                  'T_ADS_DIM_LCA_PCR_INDICATOR',
                  'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                  'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                  'V_ADS_LCA_PCR_INDICATOR',
                  'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                  'T_ADS_DIM_LCA_IMPACT_METHOD',
                  'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                  'V_ADS_LCA_IMPACT_INDICATOR',
                  'T_ADS_DIM_LCA_DATABASE_VERSION',
                  'T_ADS_DIM_LCA_SYSTEM_MODEL',
                  'T_ADS_DIM_LCA_DATASET',
                  'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                  'T_ADS_DIM_LCA_DATASET_PRODUCT',
                  'T_ADS_DIM_LCA_GEOGRAPHY',
                  'V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW',
                  'T_ADS_DIM_LCA_UNIT_PROCESS',
                  'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                  'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                  'V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT',
                  'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                  'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                  'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                  'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                  'V_ADS_LCA_ELEMENTARY_FLOW_IMPACT',
                  'V_ADS_LCA_PCR_ELEMENTARY_FLOW_IMPACT',
                  'T_ADS_DIM_EMISSION_FACTOR_SOURCE_DOC',
                  'T_ADS_DIM_EMISSION_FACTOR_CATEGORY',
                  'T_ADS_DIM_EMISSION_FACTOR_DQR_CATEGORY',
                  'T_ADS_FACT_EMISSION_FACTOR_LIBRARY',
                  'T_ADS_FACT_EMISSION_FACTOR_DQR',
                  'V_ADS_EMISSION_FACTOR_CATEGORY',
                  'V_ADS_EMISSION_FACTOR_LIBRARY',
                  'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                  'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                  'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                  'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                  'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                  'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                  'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                  'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION'
    )
ORDER BY tabname, colseq;


-- Version 2: Detailed primary key with constraint information
SELECT t.tabschema AS schema_name,
       t.tabname   AS table_name,
       t.constname AS pk_constraint_name,
       t.enforced  AS enforced,
       k.colname   AS primary_key_column,
       k.colseq    AS column_sequence,
       c.typename  AS data_type,
       c.length    AS column_length,
       c.scale     AS column_scale,
       c.nulls     AS nullable
FROM syscat.tabconst t
         JOIN syscat.keycoluse k
              ON t.constname = k.constname
                  AND t.tabschema = k.tabschema
         JOIN syscat.columns c
              ON k.tabname = c.tabname
                  AND k.colname = c.colname
                  AND k.tabschema = c.tabschema
WHERE t.type = 'P'
  AND t.tabname IN (
                    'T_ADS_DIM_LCA_PCR',
                    'T_ADS_DIM_LCA_PCR_INDICATOR',
                    'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                    'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                    'V_ADS_LCA_PCR_INDICATOR',
                    'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                    'T_ADS_DIM_LCA_IMPACT_METHOD',
                    'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                    'V_ADS_LCA_IMPACT_INDICATOR',
                    'T_ADS_DIM_LCA_DATABASE_VERSION',
                    'T_ADS_DIM_LCA_SYSTEM_MODEL',
                    'T_ADS_DIM_LCA_DATASET',
                    'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                    'T_ADS_DIM_LCA_DATASET_PRODUCT',
                    'T_ADS_DIM_LCA_GEOGRAPHY',
                    'V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW',
                    'T_ADS_DIM_LCA_UNIT_PROCESS',
                    'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                    'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                    'V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT',
                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                    'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                    'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                    'V_ADS_LCA_ELEMENTARY_FLOW_IMPACT',
                    'V_ADS_LCA_PCR_ELEMENTARY_FLOW_IMPACT',
                    'T_ADS_DIM_EMISSION_FACTOR_SOURCE_DOC',
                    'T_ADS_DIM_EMISSION_FACTOR_CATEGORY',
                    'T_ADS_DIM_EMISSION_FACTOR_DQR_CATEGORY',
                    'T_ADS_FACT_EMISSION_FACTOR_LIBRARY',
                    'T_ADS_FACT_EMISSION_FACTOR_DQR',
                    'V_ADS_EMISSION_FACTOR_CATEGORY',
                    'V_ADS_EMISSION_FACTOR_LIBRARY',
                    'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                    'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                    'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                    'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION'
    )
ORDER BY t.tabname, k.colseq;


-- Version 3: Check if a specific column is a primary key
-- Replace 'YOUR_COLUMN_NAME' and 'YOUR_TABLE_NAME' with actual values
SELECT t.tabname   AS table_name,
       t.constname AS pk_constraint_name,
       k.colname   AS column_name,
       k.colseq    AS column_position,
       CASE
           WHEN k.colname IS NOT NULL THEN 'YES - Is Primary Key'
           ELSE 'NO - Not a Primary Key'
           END     AS is_primary_key
FROM syscat.tabconst t
         LEFT JOIN syscat.keycoluse k
                   ON t.constname = k.constname
                       AND t.tabschema = k.tabschema
WHERE t.type = 'P'
  AND t.tabname IN (
                    'T_ADS_DIM_LCA_PCR',
                    'T_ADS_DIM_LCA_PCR_INDICATOR',
                    'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                    'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                    'V_ADS_LCA_PCR_INDICATOR',
                    'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                    'T_ADS_DIM_LCA_IMPACT_METHOD',
                    'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                    'V_ADS_LCA_IMPACT_INDICATOR',
                    'T_ADS_DIM_LCA_DATABASE_VERSION',
                    'T_ADS_DIM_LCA_SYSTEM_MODEL',
                    'T_ADS_DIM_LCA_DATASET',
                    'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                    'T_ADS_DIM_LCA_DATASET_PRODUCT',
                    'T_ADS_DIM_LCA_GEOGRAPHY',
                    'V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW',
                    'T_ADS_DIM_LCA_UNIT_PROCESS',
                    'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                    'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                    'V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT',
                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                    'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                    'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                    'V_ADS_LCA_ELEMENTARY_FLOW_IMPACT',
                    'V_ADS_LCA_PCR_ELEMENTARY_FLOW_IMPACT',
                    'T_ADS_DIM_EMISSION_FACTOR_SOURCE_DOC',
                    'T_ADS_DIM_EMISSION_FACTOR_CATEGORY',
                    'T_ADS_DIM_EMISSION_FACTOR_DQR_CATEGORY',
                    'T_ADS_FACT_EMISSION_FACTOR_LIBRARY',
                    'T_ADS_FACT_EMISSION_FACTOR_DQR',
                    'V_ADS_EMISSION_FACTOR_CATEGORY',
                    'V_ADS_EMISSION_FACTOR_LIBRARY',
                    'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                    'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                    'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                    'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION'
    )
ORDER BY t.tabname, k.colseq;


-- Version 4: Tables WITHOUT primary keys
SELECT tabname AS table_name
FROM syscat.tables
WHERE tabname IN (
                  'T_ADS_DIM_LCA_PCR',
                  'T_ADS_DIM_LCA_PCR_INDICATOR',
                  'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                  'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                  'V_ADS_LCA_PCR_INDICATOR',
                  'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                  'T_ADS_DIM_LCA_IMPACT_METHOD',
                  'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                  'V_ADS_LCA_IMPACT_INDICATOR',
                  'T_ADS_DIM_LCA_DATABASE_VERSION',
                  'T_ADS_DIM_LCA_SYSTEM_MODEL',
                  'T_ADS_DIM_LCA_DATASET',
                  'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                  'T_ADS_DIM_LCA_DATASET_PRODUCT',
                  'T_ADS_DIM_LCA_GEOGRAPHY',
                  'V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW',
                  'T_ADS_DIM_LCA_UNIT_PROCESS',
                  'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                  'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                  'V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT',
                  'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                  'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                  'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                  'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                  'V_ADS_LCA_ELEMENTARY_FLOW_IMPACT',
                  'V_ADS_LCA_PCR_ELEMENTARY_FLOW_IMPACT',
                  'T_ADS_DIM_EMISSION_FACTOR_SOURCE_DOC',
                  'T_ADS_DIM_EMISSION_FACTOR_CATEGORY',
                  'T_ADS_DIM_EMISSION_FACTOR_DQR_CATEGORY',
                  'T_ADS_FACT_EMISSION_FACTOR_LIBRARY',
                  'T_ADS_FACT_EMISSION_FACTOR_DQR',
                  'V_ADS_EMISSION_FACTOR_CATEGORY',
                  'V_ADS_EMISSION_FACTOR_LIBRARY',
                  'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                  'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                  'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                  'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                  'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                  'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                  'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                  'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION'
    )
  AND tabname NOT IN (SELECT tabname
                      FROM syscat.tabconst
                      WHERE type = 'P')
ORDER BY tabname;


-- Version 5: Grouped PK columns per table (composite keys shown together)
SELECT t.tabname                                                 AS table_name,
       t.constname                                               AS pk_constraint_name,
       LISTAGG(k.colname, ', ') WITHIN GROUP (ORDER BY k.colseq) AS primary_key_columns,
       k.colcount                                                AS number_of_pk_columns
FROM syscat.tabconst t
         JOIN syscat.keycoluse k
              ON t.constname = k.constname
                  AND t.tabschema = k.tabschema
WHERE t.type = 'P'
  AND t.tabname IN (
                    'T_ADS_DIM_LCA_PCR',
                    'T_ADS_DIM_LCA_PCR_INDICATOR',
                    'T_ADS_DIM_LCA_PCR_ALLOCATION_PROCESS',
                    'T_ADS_FACT_LCA_PCR_ALLOCATION_FACTOR',
                    'V_ADS_LCA_PCR_INDICATOR',
                    'T_ADS_DIM_LCA_IMPACT_CATEGORY',
                    'T_ADS_DIM_LCA_IMPACT_METHOD',
                    'T_ADS_DIM_LCA_IMPACT_INDICATOR',
                    'V_ADS_LCA_IMPACT_INDICATOR',
                    'T_ADS_DIM_LCA_DATABASE_VERSION',
                    'T_ADS_DIM_LCA_SYSTEM_MODEL',
                    'T_ADS_DIM_LCA_DATASET',
                    'T_ADS_DIM_LCA_DATASET_ACTIVITY',
                    'T_ADS_DIM_LCA_DATASET_PRODUCT',
                    'T_ADS_DIM_LCA_GEOGRAPHY',
                    'V_ADS_LCA_DATASET_ACTIVITY_OVERVIEW',
                    'T_ADS_DIM_LCA_UNIT_PROCESS',
                    'T_ADS_FACT_LCA_UNIT_PROCESS_IMPACT',
                    'T_ADS_BR_LCA_ITEM_UNIT_PROCESS',
                    'V_ADS_LCA_PCR_UNIT_PROCESS_IMPACT',
                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW',
                    'T_ADS_DIM_LCA_ELEMENTARY_FLOW_METHOD_MAP',
                    'T_ADS_FACT_LCA_ELEMENTARY_FLOW_IMPACT',
                    'T_ADS_BR_LCA_ITEM_ELEMENTARY_FLOW',
                    'V_ADS_LCA_ELEMENTARY_FLOW_IMPACT',
                    'V_ADS_LCA_PCR_ELEMENTARY_FLOW_IMPACT',
                    'T_ADS_DIM_EMISSION_FACTOR_SOURCE_DOC',
                    'T_ADS_DIM_EMISSION_FACTOR_CATEGORY',
                    'T_ADS_DIM_EMISSION_FACTOR_DQR_CATEGORY',
                    'T_ADS_FACT_EMISSION_FACTOR_LIBRARY',
                    'T_ADS_FACT_EMISSION_FACTOR_DQR',
                    'V_ADS_EMISSION_FACTOR_CATEGORY',
                    'V_ADS_EMISSION_FACTOR_LIBRARY',
                    'T_ADS_DIM_LCA_BIGCLASS_CALC_RECORD',
                    'T_ADS_FACT_LCA_UNIT_CONVERSION_FACTOR',
                    'T_ADS_FACT_LCA_BIGCLASS_PROC_PRODUCTION',
                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_DATA',
                    'T_ADS_FACT_LCA_BIGCLASS_ACTIVITY_LEVEL_IMPACT',
                    'T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX',
                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_RESULT',
                    'T_ADS_FACT_LCA_BIGCLASS_IMPACT_CONTRIBUTION'
    )
GROUP BY t.tabname, t.constname, k.colcount
ORDER BY t.tabname;
