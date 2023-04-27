from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_SUP_11.config.ConfigStore import *
from MD_SUP_11.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD,
  LFA1.lifnr AS SUP_NUM,
  TRIM(LFA1.name1) AS SUP_NM1,
  TRIM(LFA1.name2) AS SUP_NM2,
  TRIM(LFA1.name3) AS SUP_NM3,
  TRIM(LFA1.name4) AS SUP_NM4,
  TRIM(LFA1.sortl) AS SUP_SHRT_NM,
  TRIM(LFA1.land1) AS CTRY_CD,
  TRIM(LFA1.txjcd) AS TAX_JURIS_CD,
  TRIM(LFA1.lzone) AS TRSPN_ZN_CD,
  TRIM(LFA1.ort01) AS CITY_NM,
  TRIM(LFA1.ort02) AS DSTRC_NM,
  TRIM(LFA1.pfach) AS PSTL_BOX_CD,
  TRIM(LFA1.pstl2) AS PSTL_BOX_PSTL_CD_NUM,
  TRIM(LFA1.pstlz) AS PSTL_CD_NUM,
  TRIM(LFA1.regio) AS RGN_NM,
  TRIM(LFA1.stras) AS ADDR_LINE_1_TXT,
  TRIM(T005T.landx) AS CTRY_SHRT_NM,
  TRIM(LFA1.adrnr)  AS ADDR_NUM,
  TRIM(LFA1.bbbnr) AS GLN1_NBR,
  TRIM(LFA1.brsch) AS SGMNT_CD,
  CASE
    WHEN TRIM(LFA1.erdat) = '00000000' THEN NULL
    ELSE TO_TIMESTAMP(TRIM(LFA1.erdat), \"yyyyMMdd\")
  END AS CRT_ON_DTTM,
  TRIM(LFA1.ktokk) AS SUP_TYPE_CD,
  TRIM(LFA1.kunnr) AS CUST_NUM,
  TRIM(LFA1.lnrza) AS ALT_PAYEE_NUM,
  TRIM(LFA1.loevm) AS DEL_IND,
  TRIM(LFA1.sperr) AS PSTNG_BLK_IND,
  TRIM(LFA1.sperm) AS PRCH_BLK_IND,
  TRIM(LFA1.stcd1) AS TAX_NUM1,
  TRIM(LFA1.stcd2) AS TAX_NUM2,
  TRIM(LFA1.xcpdk) AS ONE_TIME_ACCT_IND,
  TRIM(LFA1.vbund) AS TRAD_PTNR_CO_CD,
  TRIM(LFA1.stceg) AS VAT_NUM,
  TRIM(LFA1.stkzn) AS NTRL_PRSN_IND,
  TRIM(LFA1.werks) AS PLNT_CD,
  TRIM(LFA1.sperz) AS PMT_BLK_IND,
  TRIM(LFA1.scacd) AS STD_CARR_ACCS_CD,
  TRIM(LFA1.fityp) AS TAX_TYPE_CD,
  TRIM(LFA1.stcdt) AS TAX_NUM_TYPE_CD,
  TRIM(LFA1.stcd3) AS TAX_NUM3,
  TRIM(LFA1.bbsnr) AS GLN2_NBR,
  TRIM(LFA1.stcd4) AS TAX_NUM4,
  TRIM(LFA1.stcd5) AS TAX_NUM5,
  TRIM(LFA1.bubkz) AS GLN3_NBR,
  TRIM(LFA1.nodel) AS BLOK_SUP_IND,
  TRIM(LFA1.podkzb) AS POD_IND,
  TRIM(LFA1.emnfr) AS EXTRNL_MFR_CD,
    lfa1._upt_ as _l0_upt
FROM
  {Config.sourceDatabase}.LFA1 LFA1
  LEFT JOIN {Config.sourceDatabase}.T005T T005T ON LFA1.LAND1 = T005T.LAND1
  AND T005T._deleted_ = 'F'
  AND T005T.mandt = '{Config.MANDT}'
  AND T005T.SPRAS = 'E'
WHERE
  LFA1._deleted_ = 'F'
  AND LFA1.mandt = '{Config.MANDT}'  
 
"""
    )

    return out0
