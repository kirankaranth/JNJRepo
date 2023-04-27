from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_SUP_9.config.ConfigStore import *
from MD_SUP_9.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    select '{Config.sourceSystem}' AS SRC_SYS_CD,
f0401_adt.a6an8 AS SUP_NUM,
trim(f0101_adt.abalph) AS SUP_NM1,
trim(f0111.wwmlnm) AS SUP_NM2,
NULL AS SUP_NM3,
NULL AS SUP_NM4,
NULL AS SUP_SHRT_NM,
trim(f0116_adt.alctr) AS CTRY_CD,
NULL AS TAX_JURIS_CD,
NULL AS TRSPN_ZN_CD,
trim(f0116_adt.alcty1) AS CITY_NM,
trim(f0116_adt.aladd4) AS DSTRC_NM,
NULL AS PSTL_BOX_CD,
NULL AS PSTL_BOX_PSTL_CD_NUM,
trim(f0116_adt.aladdz) AS PSTL_CD_NUM,
trim(f0116_adt.aladds) AS RGN_NM,
trim(f0116_adt.aladd1) AS ADDR_LINE_1_TXT,
NULL AS CTRY_SHRT_NM,
NULL AS ADDR_NUM,
NULL AS GLN1_NBR,
NULL AS SGMNT_CD,
CASE WHEN LOWER(TRIM(f0116_adt.ALUPMJ)) = 'null' OR TRIM(f0116_adt.ALUPMJ) = '' OR TRIM(f0116_adt.ALUPMJ) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f0116_adt.ALUPMJ) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f0116_adt.ALUPMJ) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS CRT_ON_DTTM,
trim(f0101_adt.abat1) AS SUP_TYPE_CD,
trim(f0101_adt.aban8) AS CUST_NUM,
NULL AS ALT_PAYEE_NUM,
NULL AS DEL_IND,
NULL AS PSTNG_BLK_IND,
NULL AS PRCH_BLK_IND,
NULL AS TAX_NUM1,
NULL AS TAX_NUM2,
NULL AS ONE_TIME_ACCT_IND,
NULL AS TRAD_PTNR_CO_CD,
trim(f0101_adt.abtax) AS VAT_NUM,
NULL AS NTRL_PRSN_IND,
NULL AS PLNT_CD,
NULL AS PMT_BLK_IND,
NULL AS STD_CARR_ACCS_CD,
NULL AS TAX_TYPE_CD,
NULL AS TAX_NUM_TYPE_CD,
trim(f0101_adt.abtx2) AS TAX_NUM3,
NULL AS GLN2_NBR,
NULL AS TAX_NUM4,
NULL AS TAX_NUM5,
NULL AS GLN3_NBR,
NULL AS BLOK_SUP_IND,
NULL AS POD_IND,
trim(f0101_adt.abalky) AS EXTRNL_MFR_CD,
f0401_adt._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.f0401_adt  f0401_adt
 left join {Config.sourceDatabase}.f0101_adt on f0401_adt.a6an8=f0101_adt.aban8 AND   f0101_adt._deleted_ = 'F'   
 left join {Config.sourceDatabase}.f0111     on f0401_adt.a6an8=f0111.wwan8     AND   f0111._deleted_ = 'F'   and f0111.wwmlnm = 0
 left join {Config.sourceDatabase}.f0116_adt on f0401_adt.a6an8=f0116_adt.alan8 AND   f0116_adt._deleted_ = 'F' 
WHERE  f0401_adt._deleted_ = 'F'  
 
"""
    )

    return out0
