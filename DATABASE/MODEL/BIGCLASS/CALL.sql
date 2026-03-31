CALL BG00MAC102.P_ADS_LCA_BIGCLASS_CALC_ALLOCATION('bd7265c4-2f27-4484-8939-c59eebb1fd6b',
                                                   'CONS',
                                                   'ecoinvent-v3.11',
                                                   'TA',
                                                   '202602',
                                                   '202602',
                                                   NULL,
                                                   FALSE,
                                                   NULL,
                                                   NULL);



SELECT SOURCE_PROC_KEY, TARGET_PROC_KEY, UNIT_COST
FROM BG00MAC102.T_ADS_FACT_LCA_BIGCLASS_INPUT_OUTPUT_MATRIX
WHERE BIGCLASS_REC_ID = 'bd7265c4-2f27-4484-8939-c59eebb1fd6b'
  and IS_INVERSED;
