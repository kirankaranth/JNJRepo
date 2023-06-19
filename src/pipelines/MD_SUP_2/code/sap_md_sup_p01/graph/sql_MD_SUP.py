from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_p01.config.ConfigStore import *
from sap_md_sup_p01.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    select
'{Config.sourceSystem}' AS SRC_SYS_CD,
    LFA1.lifnr AS SUP_NUM,
    LFA1.name1 AS SUP_NM1,
    LFA1.name2 AS SUP_NM2,
    LFA1.name3 AS SUP_NM3,
    LFA1.name4 AS SUP_NM4,
    LFA1.sortl AS SUP_SHRT_NM,
    LFA1.land1 AS CTRY_CD,
    LFA1.txjcd AS TAX_JURIS_CD,
    LFA1.lzone AS TRSPN_ZN_CD,
    LFA1.ort01 AS CITY_NM,
    LFA1.ort02 AS DSTRC_NM,
    LFA1.pfach AS PSTL_BOX_CD,
    LFA1.pstl2 AS PSTL_BOX_PSTL_CD_NUM,
    LFA1.pstlz AS PSTL_CD_NUM,
    LFA1.regio AS RGN_NM,
    LFA1.stras AS ADDR_LINE_1_TXT,
    T005T.landx AS CTRY_SHRT_NM,
    LFA1.adrnr AS ADDR_NUM,
    LFA1.bbbnr AS GLN1_NBR,
    LFA1.brsch AS SGMNT_CD,
    CASE
      WHEN(LFA1.erdat)='00000000' THEN NULL
      ELSE TO_TIMESTAMP((LFA1.erdat),\"yyyyMMdd\")
    END AS CRT_ON_DTTM,
    LFA1.ktokk AS SUP_TYPE_CD,
    LFA1.kunnr AS CUST_NUM,
    LFA1.lnrza AS ALT_PAYEE_NUM,
    LFA1.loevm AS DEL_IND,
    LFA1.sperr AS PSTNG_BLK_IND,
    LFA1.sperm AS PRCH_BLK_IND,
    LFA1.stcd1 AS TAX_NUM1,
    LFA1.stcd2 AS TAX_NUM2,
    LFA1.xcpdk AS ONE_TIME_ACCT_IND,
    LFA1.vbund AS TRAD_PTNR_CO_CD,
    LFA1.stceg AS VAT_NUM,
    LFA1.stkzn AS NTRL_PRSN_IND,
    LFA1.werks AS PLNT_CD,
    LFA1.sperz AS PMT_BLK_IND,
    LFA1.scacd AS STD_CARR_ACCS_CD,
    LFA1.fityp AS TAX_TYPE_CD,
    LFA1.stcdt AS TAX_NUM_TYPE_CD,
    LFA1.stcd3 AS TAX_NUM3,
    LFA1.bbsnr AS GLN2_NBR,
    LFA1.stcd4 AS TAX_NUM4,
    LFA1.stcd5 AS TAX_NUM5,
    LFA1.bubkz AS GLN3_NBR,
    LFA1.nodel AS BLOK_SUP_IND,
    LFA1.podkzb AS POD_IND,
    NULL AS EXTRNL_MFR_CD,
    lfa1._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.LFA1 LFA1
LEFT JOIN {Config.sourceDatabase}.T005T T005T
on LFA1.LAND1=T005T.LAND1
AND T005T._deleted_ = 'F'
AND T005T.spras = 'E'
and T005T.MANDT='020'
WHERE LFA1._deleted_ = 'F'  and LFA1.MANDT='020'  
 
"""
    )

    return out0
