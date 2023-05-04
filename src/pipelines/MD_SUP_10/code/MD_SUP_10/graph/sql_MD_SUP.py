from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_SUP_10.config.ConfigStore import *
from MD_SUP_10.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    select '{Config.sourceSystem}' AS SRC_SYS_CD,
f0401.a6an8 AS SUP_NUM,
trim(f0101.abalph) AS SUP_NM1,
trim(f0111.wwmlnm) AS SUP_NM2,
NULL AS SUP_NM3,
NULL AS SUP_NM4,
NULL AS SUP_SHRT_NM,
trim(f0116.alctr) AS CTRY_CD,
NULL AS TAX_JURIS_CD,
NULL AS TRSPN_ZN_CD,
trim(f0116.alcty1) AS CITY_NM,
trim(f0116.aladd4) AS DSTRC_NM,
NULL AS PSTL_BOX_CD,
NULL AS PSTL_BOX_PSTL_CD_NUM,
trim(f0116.aladdz) AS PSTL_CD_NUM,
trim(f0116.aladds) AS RGN_NM,
trim(f0116.aladd1) AS ADDR_LINE_1_TXT,
NULL AS CTRY_SHRT_NM,
NULL AS ADDR_NUM,
NULL AS GLN1_NBR,
NULL AS SGMNT_CD,
CASE WHEN LOWER(TRIM(f0401.a6upmj)) = 'null' OR TRIM(f0401.a6upmj) = '' OR TRIM(f0401.a6upmj) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f0401.a6upmj) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f0401.a6upmj) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS CRT_ON_DTTM,
trim(f0101.abat1) AS SUP_TYPE_CD,
trim(f0101.aban8) AS CUST_NUM,
NULL AS ALT_PAYEE_NUM,
NULL AS DEL_IND,
NULL AS PSTNG_BLK_IND,
NULL AS PRCH_BLK_IND,
NULL AS TAX_NUM1,
NULL AS TAX_NUM2,
NULL AS ONE_TIME_ACCT_IND,
NULL AS TRAD_PTNR_CO_CD,
trim(f0101.abtax) AS VAT_NUM,
NULL AS NTRL_PRSN_IND,
NULL AS PLNT_CD,
NULL AS PMT_BLK_IND,
NULL AS STD_CARR_ACCS_CD,
NULL AS TAX_TYPE_CD,
NULL AS TAX_NUM_TYPE_CD,
trim(f0101.abtx2) AS TAX_NUM3,
NULL AS GLN2_NBR,
NULL AS TAX_NUM4,
NULL AS TAX_NUM5,
NULL AS GLN3_NBR,
NULL AS BLOK_SUP_IND,
NULL AS POD_IND,

f0401._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.f0401  f0401
 left join {Config.sourceDatabase}.f0101 on f0401.a6an8=f0101.aban8 AND   f0101._deleted_ = 'F'   
 left join {Config.sourceDatabase}.f0111 on f0401.a6an8=f0111.wwan8 AND   f0111._deleted_ = 'F'   and f0111.wwmlnm = 0
 left join {Config.sourceDatabase}.f0116 on f0401.a6an8=f0116.alan8 AND   f0116._deleted_ = 'F' 
WHERE  f0401._deleted_ = 'F'


  
 
"""
    )

    return out0
