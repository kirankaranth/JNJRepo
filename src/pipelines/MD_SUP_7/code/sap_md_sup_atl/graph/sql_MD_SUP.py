from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_atl.config.ConfigStore import *
from sap_md_sup_atl.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    select
'{Config.sourceSystem}' AS SRC_SYS_CD,
    trim(LFA1.lifnr) AS SUP_NUM,
    trim(LFA1.name1) AS SUP_NM1,
    trim(LFA1.name2) AS SUP_NM2,
    trim(LFA1.name3) AS SUP_NM3,
    trim(LFA1.name4) AS SUP_NM4,
    trim(LFA1.sortl) AS SUP_SHRT_NM,
    trim(LFA1.land1) AS CTRY_CD,
    trim(LFA1.txjcd) AS TAX_JURIS_CD,
    trim(LFA1.lzone) AS TRSPN_ZN_CD,
    trim(LFA1.ort01) AS CITY_NM,
    trim(LFA1.ort02) AS DSTRC_NM,
    trim(LFA1.pfach) AS PSTL_BOX_CD,
    trim(LFA1.pstl2) AS PSTL_BOX_PSTL_CD_NUM,
    trim(LFA1.pstlz) AS PSTL_CD_NUM,
    trim(LFA1.regio) AS RGN_NM,
    trim(LFA1.stras) AS ADDR_LINE_1_TXT,
    trim(T005T.landx) AS CTRY_SHRT_NM,
    trim(LFA1.adrnr) AS ADDR_NUM,
    trim(LFA1.bbbnr) AS GLN1_NBR,
    trim(LFA1.brsch) AS SGMNT_CD,
    CASE
      WHEN (LFA1.erdat)='00000000' THEN NULL
      ELSE TO_TIMESTAMP((LFA1.erdat),\"yyyyMMdd\")
    END AS CRT_ON_DTTM,
    trim(LFA1.ktokk) AS SUP_TYPE_CD,
    trim(LFA1.kunnr) AS CUST_NUM,
    trim(LFA1.lnrza) AS ALT_PAYEE_NUM,
    trim(LFA1.loevm) AS DEL_IND,
    trim(LFA1.sperr) AS PSTNG_BLK_IND,
    trim(LFA1.sperm) AS PRCH_BLK_IND,
    trim(LFA1.stcd1) AS TAX_NUM1,
    trim(LFA1.stcd2) AS TAX_NUM2,
    trim(LFA1.xcpdk) AS ONE_TIME_ACCT_IND,
    trim(LFA1.vbund) AS TRAD_PTNR_CO_CD,
    trim(LFA1.stceg) AS VAT_NUM,
    trim(LFA1.stkzn) AS NTRL_PRSN_IND,
    trim(LFA1.werks) AS PLNT_CD,
    trim(LFA1.sperz) AS PMT_BLK_IND,
    trim(LFA1.scacd) AS STD_CARR_ACCS_CD,
    trim(LFA1.fityp) AS TAX_TYPE_CD,
    trim(LFA1.stcdt) AS TAX_NUM_TYPE_CD,
    trim(LFA1.stcd3) AS TAX_NUM3,
    trim(LFA1.bbsnr) AS GLN2_NBR,
    trim(LFA1.stcd4) AS TAX_NUM4,
    trim(LFA1.stcd5) AS TAX_NUM5,
    trim(LFA1.bubkz) AS GLN3_NBR,
    trim(LFA1.nodel) AS BLOK_SUP_IND,
    trim(LFA1.podkzb) AS POD_IND,
    trim(LFA1.emnfr) AS EXTRNL_MFR_CD,
    lfa1._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.LFA1 LFA1
LEFT JOIN {Config.sourceDatabase}.T005T T005T
on LFA1.LAND1=T005T.LAND1
AND T005T._deleted_ = 'F'
AND T005T.spras = 'E'
and T005T.MANDT='100'
WHERE LFA1._deleted_ = 'F'  and LFA1.MANDT='100'  
 
"""
    )

    return out0
