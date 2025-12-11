SELECT SUBCLASS_TAB_NAME,
       UPDATE_DATE,
       MAT_NO,
       MAT_TRACK_NO,
       MAT_SEQ_NO,
       FAMILY_CODE,
       UNIT_CODE,
       UNIT_NAME,
       PRODUCT_CODE,
       PRODUCT_NAME,
       TYPE_CODE,
       TYPE_NAME,
       ITEM_CODE,
       ITEM_NAME,
       SUM(VALUE)     AS VALUE,
       UNITM_AC,
       PRODUCT_VALUE,
       SUM(UNIT_COST) AS UNIT_COST,
       SUM(C1)        AS C1,
       SUM(C2)        AS C2,
       SUM(C3)        AS C3,
       SUM(C4)        AS C4,
       SUM(C5)        AS C5,
       FACTOR_DIRECT,
       FACTOR_INDIRECT,
       FACTOR_TRANSPORT,
       FLAG
FROM (SELECT A.*
      FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RESULT_DIST A
               INNER JOIN (SELECT *
                           FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RANK_POST
                           WHERE RANK = 1) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
          AND A.UPDATE_DATE = B.UPDATE_DATE
          AND A.FAMILY_CODE = B.FAMILY_CODE
          AND A.UNIT_CODE = B.UNIT_CODE
      UNION
      SELECT A.SUBCLASS_TAB_NAME,
             A.UPDATE_DATE,
             A.MAT_NO,
             A.MAT_TRACK_NO,
             A.MAT_SEQ_NO,
             A.FAMILY_CODE,
             A.UNIT_CODE,
             A.UNIT_NAME,
             A.PRODUCT_CODE,
             A.PRODUCT_NAME,
             C.TYPE_CODE,
             C.TYPE_NAME,
             C.ITEM_CODE,
             C.ITEM_NAME,
             A.UNIT_COST * C.VALUE,
             C.UNITM_AC,
             A.PRODUCT_VALUE,
             A.UNIT_COST * C.UNIT_COST,
             A.UNIT_COST * C.C1,
             A.UNIT_COST * C.C2,
             A.UNIT_COST * C.C3,
             A.UNIT_COST * C.C4,
             A.UNIT_COST * C.C5,
             C.FACTOR_DIRECT,
             C.FACTOR_INDIRECT,
             C.FACTOR_TRANSPORT,
             C.FLAG
      FROM (SELECT * FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RESOURCE_MAIN) A
               INNER JOIN (SELECT *
                           FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RANK_POST
                           WHERE RANK = 1
                             AND SUM_FLAG = 0) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
          AND A.UPDATE_DATE = B.UPDATE_DATE
          AND A.FAMILY_CODE = B.FAMILY_CODE
          AND A.UNIT_CODE = B.UNIT_CODE
               INNER JOIN
           (SELECT * FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RESULT_DIST_PARALLEL) C
           ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE
      UNION
      SELECT A.SUBCLASS_TAB_NAME,
             A.UPDATE_DATE,
             A.MAT_NO,
             A.MAT_TRACK_NO,
             A.MAT_SEQ_NO,
             A.FAMILY_CODE,
             A.UNIT_CODE,
             A.UNIT_NAME,
             A.PRODUCT_CODE,
             A.PRODUCT_NAME,
             A.TYPE_CODE,
             A.TYPE_NAME,
             A.ITEM_CODE,
             A.ITEM_NAME,
             A.VALUE,
             A.UNITM_AC,
             A.PRODUCT_VALUE,
             A.UNIT_COST,
             A.UNIT_COST * C.C1,
             A.UNIT_COST * C.C2,
             A.UNIT_COST * C.C3,
             A.UNIT_COST * C.C4,
             A.UNIT_COST * C.C5,
             NULL,
             NULL,
             NULL,
             'PREV_SUM'
      FROM (SELECT * FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RESOURCE_MAIN) A
               INNER JOIN (SELECT *
                           FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RANK_POST
                           WHERE RANK = 1
                             AND SUM_FLAG = 1) B ON A.MAT_TRACK_NO = B.MAT_TRACK_NO
          AND A.UPDATE_DATE = B.UPDATE_DATE
          AND A.FAMILY_CODE = B.FAMILY_CODE
          AND A.UNIT_CODE = B.UNIT_CODE
               INNER JOIN (SELECT SUBCLASS_TAB_NAME,
                                  UPDATE_DATE,
                                  MAT_NO,
                                  MAT_TRACK_NO,
                                  MAT_SEQ_NO,
                                  FAMILY_CODE,
                                  UNIT_CODE,
                                  UNIT_NAME,
                                  PRODUCT_CODE,
                                  PRODUCT_NAME,
                                  PRODUCT_VALUE,
                                  SUM(C1) AS C1,
                                  SUM(C2) AS C2,
                                  SUM(C3) AS C3,
                                  SUM(C4) AS C4,
                                  SUM(C5) AS C5
                           FROM BG00MAC102.T_ADS_TEMP_LCA_SUBCLASS_SEQ_CALC_MONTH_RESULT_DIST_PARALLEL
                           GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO,
                                    MAT_SEQ_NO, FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME,
                                    PRODUCT_VALUE) C
                          ON B.MAT_TRACK_NO = C.MAT_TRACK_NO AND B.PREV_FAMILY_CODE = C.FAMILY_CODE)
GROUP BY SUBCLASS_TAB_NAME, UPDATE_DATE, MAT_NO, MAT_TRACK_NO, MAT_SEQ_NO,
         FAMILY_CODE, UNIT_CODE, UNIT_NAME, PRODUCT_CODE, PRODUCT_NAME, TYPE_CODE, TYPE_NAME,
         ITEM_CODE, ITEM_NAME, UNITM_AC, PRODUCT_VALUE, FACTOR_DIRECT, FACTOR_INDIRECT, FACTOR_TRANSPORT, FLAG
HAVING MAT_TRACK_NO = '20241119073547169976';


