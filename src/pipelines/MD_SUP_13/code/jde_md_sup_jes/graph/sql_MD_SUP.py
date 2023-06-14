from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_jes.config.ConfigStore import *
from jde_md_sup_jes.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    with cte as (
    SELECT
        ROW_NUMBER() OVER (partition by f0401_adt.a6an8 ORDER BY f0116_adt.aleftb desc) row,
        '{Config.sourceSystem}' AS SRC_SYS_CD,
        f0401_adt.a6an8 AS SUP_NUM,
        TRIM(f0101_adt.abalph) AS SUP_NM1,
        CAST(NULL AS STRING) AS SUP_NM2,
        CAST(NULL AS STRING) AS SUP_NM3,
        CAST(NULL AS STRING) AS SUP_NM4,
        CAST(NULL AS STRING) AS SUP_SHRT_NM,
        TRIM(f0116_adt.alctr) AS CTRY_CD,
        CAST(NULL AS STRING) AS TAX_JURIS_CD,
        CAST(NULL AS STRING) AS TRSPN_ZN_CD,
        TRIM(f0116_adt.alcty1) AS CITY_NM,
        TRIM(f0116_adt.aladd4) AS DSTRC_NM,
        CAST(NULL AS STRING) AS PSTL_BOX_CD,
        CAST(NULL AS STRING) AS PSTL_BOX_PSTL_CD_NUM,
        TRIM(f0116_adt.aladdz) AS PSTL_CD_NUM,
        TRIM(f0116_adt.aladds) AS RGN_NM,
        TRIM(f0116_adt.aladd1) AS ADDR_LINE_1_TXT,
        CAST(NULL AS STRING) AS CTRY_SHRT_NM,
        CAST(NULL AS STRING) AS ADDR_NUM,
        CAST(NULL AS STRING) AS GLN1_NBR,
        CAST(NULL AS STRING) AS SGMNT_CD,
        CASE
          WHEN LOWER(TRIM(f0401_adt.a6upmj)) = 'CAST(NULL AS STRING)' OR TRIM(f0401_adt.a6upmj) = '' OR TRIM(f0401_adt.a6upmj) = '0' THEN CAST(NULL AS timestamp)
          ELSE TO_TIMESTAMP(substr(CAST(DATE_ADD(CONCAT(SUBSTR(CAST(CAST(TRIM(f0401_adt.a6upmj) AS INT) + 1900000 AS string), 1, 4), '-01-01'), CAST(SUBSTR(CAST(CAST(TRIM(f0401_adt.a6upmj) AS INT) + 1900000 AS string), 5) AS INT ) -1)
          AS STRING), 1, 10),'yyyy-MM-dd')
        END AS CRT_ON_DTTM,
        TRIM(f0101_adt.abat1) AS SUP_TYPE_CD,
        TRIM(f0101_adt.aban8) AS CUST_NUM,
        CAST(NULL AS STRING) AS ALT_PAYEE_NUM,
        CAST(NULL AS STRING) AS DEL_IND,
        CAST(NULL AS STRING) AS PSTNG_BLK_IND,
        CAST(NULL AS STRING) AS PRCH_BLK_IND,
        CAST(NULL AS STRING) AS TAX_NUM1,
        CAST(NULL AS STRING) AS TAX_NUM2,
        CAST(NULL AS STRING) AS ONE_TIME_ACCT_IND,
        CAST(NULL AS STRING) AS TRAD_PTNR_CO_CD,
        TRIM(f0101_adt.abtax) AS VAT_NUM,
        CAST(NULL AS STRING) AS NTRL_PRSN_IND,
        CAST(NULL AS STRING) AS PLNT_CD,
        CAST(NULL AS STRING) AS PMT_BLK_IND,
        CAST(NULL AS STRING) AS STD_CARR_ACCS_CD,
        CAST(NULL AS STRING) AS TAX_TYPE_CD,
        CAST(NULL AS STRING) AS TAX_NUM_TYPE_CD,
        TRIM(f0101_adt.abtx2) AS TAX_NUM3,
        CAST(NULL AS STRING) AS GLN2_NBR,
        CAST(NULL AS STRING) AS TAX_NUM4,
        CAST(NULL AS STRING) AS TAX_NUM5,
        CAST(NULL AS STRING) AS GLN3_NBR,
        CAST(NULL AS STRING) AS BLOK_SUP_IND,
        CAST(NULL AS STRING) AS POD_IND,
        TRIM(f0101_adt.abalky) AS EXTRNL_MFR_CD,
        f0401_adt._upt_ as _l0_upt_
    FROM {Config.sourceDatabase}.f0401_adt f0401_adt
 LEFT JOIN {Config.sourceDatabase}.f0101_adt f0101_adt on f0401_adt.a6an8=f0101_adt.aban8 AND f0101_adt._deleted_ = 'F'
 LEFT JOIN {Config.sourceDatabase}.f0116_adt f0116_adt on f0401_adt.a6an8=f0116_adt.alan8 AND f0116_adt._deleted_ = 'F'
 WHERE f0401_adt._deleted_ = 'F'
)
SELECT 
    cte.SRC_SYS_CD, 
    cte.SUP_NUM, 
    cte.SUP_NM1, 
    cte.SUP_NM2, 
    cte.SUP_NM3, 
    cte.SUP_NM4, 
    cte.SUP_SHRT_NM, 
    cte.CTRY_CD, 
    cte.TAX_JURIS_CD, 
    cte.TRSPN_ZN_CD, 
    cte.CITY_NM, 
    cte.DSTRC_NM, 
    cte.PSTL_BOX_CD, 
    cte.PSTL_BOX_PSTL_CD_NUM, 
    cte.PSTL_CD_NUM, 
    cte.RGN_NM, 
    cte.ADDR_LINE_1_TXT, 
    cte.CTRY_SHRT_NM, 
    cte.ADDR_NUM, 
    cte.GLN1_NBR, 
    cte.SGMNT_CD, 
    cte.CRT_ON_DTTM, 
    cte.SUP_TYPE_CD, 
    cte.CUST_NUM, 
    cte.ALT_PAYEE_NUM, 
    cte.DEL_IND, 
    cte.PSTNG_BLK_IND, 
    cte.PRCH_BLK_IND, 
    cte.TAX_NUM1, 
    cte.TAX_NUM2, 
    cte.ONE_TIME_ACCT_IND, 
    cte.TRAD_PTNR_CO_CD, 
    cte.VAT_NUM, 
    cte.NTRL_PRSN_IND, 
    cte.PLNT_CD, 
    cte.PMT_BLK_IND, 
    cte.STD_CARR_ACCS_CD, 
    cte.TAX_TYPE_CD, 
    cte.TAX_NUM_TYPE_CD, 
    cte.TAX_NUM3, 
    cte.GLN2_NBR, 
    cte.TAX_NUM4, 
    cte.TAX_NUM5, 
    cte.GLN3_NBR, 
    cte.BLOK_SUP_IND, 
    cte.POD_IND, 
    cte.EXTRNL_MFR_CD,
    cte._l0_upt_
FROM 
    cte 
WHERE 
    row = 1
  
 
"""
    )

    return out0
