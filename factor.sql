SELECT batch_number
FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
WHERE COMPANY_CODE = 'TA'
  AND FLAG = 'Y'
  AND TIME_FLAG = 'Y'
  AND YEAR = '2025'
  AND MONTH = '01'
  AND END_MONTH = '12';

SELECT DISTINCT a.COMPANY_CODE,
                a.START_YM,
                a.END_YM,
                a.BATCH_NUMBER,
                a.ITEM_CODE,
                a.ITEM_NAME,
                d.DATA_ITEM_NAME,
                d.BACKGROUND_DATA,
                b.DISCH_COEFF,
                d.NONRENEWABLE_SECONDARY_FUEL,
                d.ADP_FOSSIL_FUELS,
                d.ADP_MINERAL_ELEMENTS,
                d.POCP,
                d.SECONDARY_MATERIAL_UTILIZATION_VOLUME,
                d.RADIOACTIVE_SOLID_WASTE_DISPOSAL,
                d.EP,
                d.RENEWABLE_SECONDARY_FUEL,
                d.ODP,
                d.GWP,
                d.GWP_FOSSIL_ENERGY,
                d.GWP_BIOMASS,
                d.GWP_LAND_USE,
                d.AP,
                d.HAZARDOUS_SOLID_WASTE_DISPOSAL,
                d.NONHAZARDOUS_SOLID_WASTE_DISPOSAL,
                d.FRESHWATER_CONSUMPTION,
                d.PRIMARY_ENERGY_NONRENEWABLE_SUM,
                d.PRIMARY_ENERGY_NONRENEWABLE_CARRIER,
                d.PRIMARY_ENERGY_NONRENEWABLE_MATERIALS,
                d.PRIMARY_ENERGY_RENEWABLE_SUM,
                d.PRIMARY_ENERGY_RENEWABLE_CARRIER,
                d.PRIMARY_ENERGY_RENEWABLE_MATERIALS,
                e.CUSTOMS_TRANS_VALUE,
                e.TRAIN_TRANS_VALUE,
                e.TRUCK_CAR_TRANS_VALUE,
                e.RIVER_CAR_TRANS_VALUE,
                c.FLAG
FROM (SELECT DISTINCT COMPANY_CODE,
                      START_YM,
                      END_YM,
                      BATCH_NUMBER,
                      LCA_DATA_ITEM_CODE as ITEM_CODE,
                      LCA_DATA_ITEM_NAME as ITEM_NAME
      FROM T_ADS_FACT_LCA_PROC_DATA
      WHERE COMPANY_CODE = 'TA'
        and BATCH_NUMBER = (SELECT batch_number
                            FROM BG00MAC102.T_ADS_WH_LCA_BATCH_CONTROL
                            WHERE COMPANY_CODE = 'TA'
                              AND FLAG = 'Y'
                              AND TIME_FLAG = 'Y'
                              AND YEAR = '2024'
                              AND MONTH = '01'
                              AND END_MONTH = '12'))
         AS a
         LEFT JOIN
     (SELECT DISTINCT *
      FROM BG00MAC102.T_ADS_WH_LCA_MAT_DATA
      WHERE ORG_CODE = 'TA'
        and START_TIME = '2025')
         AS b
     ON a.ITEM_CODE = b.ITEM_CODE

         LEFT JOIN
     (SELECT *
      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_DATA_CONTRAST
      WHERE BASE_CODE = 'TA'
        AND START_TIME = '2025')
         AS c
     ON a.ITEM_CODE = c.data_code

         LEFT JOIN
     (SELECT *
      FROM BG00MAC102.T_ADS_WH_PROC_BACKGROUND_UNCERT_ASSES_NEW
      WHERE company_code = 'TA'
        AND start_time = '2025')
         AS d
     ON c.uuid = d.uuid

         LEFT JOIN
     (SELECT *
      FROM BG00MAC102.T_ADS_WH_LCA_TRANS_DATA
      WHERE company_code = 'TA'
        AND start_time = '2025')
         AS e
     ON a.ITEM_CODE = e.LCA_DATA_ITEM_CODE;