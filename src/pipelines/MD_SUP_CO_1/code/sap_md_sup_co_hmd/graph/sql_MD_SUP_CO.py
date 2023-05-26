from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_co_hmd.config.ConfigStore import *
from sap_md_sup_co_hmd.udfs.UDFs import *

def sql_MD_SUP_CO(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
    LFB1.lifnr AS SUP_NUM,
    LFB1.bukrs AS CO_CD,
    CASE WHEN LFB1.erdat= '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE TO_TIMESTAMP(LFB1.erdat,'yyyyMMdd') END AS CRT_ON_DTTM,
    TRIM(LFB1.sperr) AS PSTNG_BLK_IND,
    TRIM(LFB1.loevm) AS DEL_IND,
    TRIM(LFB1.akont) AS LDGR_ACCT_CD,
    TRIM(LFB1.zwels) AS PMT_METH_CD,
    TRIM(LFB1.zahls) AS PMT_BLK_IND,
    TRIM(LFB1.zterm) AS PMT_TERM_CD,
    TRIM(LFB1.nodel) AS BLOK_SUP_IND,
    NULL AS OWN_EXPLN_OF_TERM_OF_PMT,
    lfb1._upt_ as _l0_upt_,
    lfb1._deleted_,
    lfb1._upt_
FROM {Config.sourceDatabase}.LFB1 LFB1
WHERE LFB1._deleted_ = 'F'
AND LFB1.mandt = {Config.MANDT}
"""
    )

    return out0
