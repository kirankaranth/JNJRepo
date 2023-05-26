from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_co_bba_bbn_bbl_hcs_mrs_atl_svs_fsn.config.ConfigStore import *
from sap_md_sup_co_bba_bbn_bbl_hcs_mrs_atl_svs_fsn.udfs.UDFs import *

def sql_MD_SUP_CO(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
lfb1.lifnr AS SUP_NUM,
lfb1.bukrs AS CO_CD,
CASE WHEN lfb1.erdat = '00000000' THEN CAST(NULL AS TIMESTAMP)
ELSE TO_TIMESTAMP(lfb1.erdat,\"yyyyMMdd\") END AS CRT_ON_DTTM,
trim(lfb1.sperr) AS PSTNG_BLK_IND,
trim(lfb1.loevm) AS DEL_IND,
trim(lfb1.akont) AS LDGR_ACCT_CD,
trim(lfb1.zwels) AS PMT_METH_CD,
trim(lfb1.zahls) AS PMT_BLK_IND,
trim(lfb1.zterm) AS PMT_TERM_CD,
trim(lfb1.nodel) AS BLOK_SUP_IND,
NULL AS OWN_EXPLN_OF_TERM_OF_PMT,
lfb1._upt_ as _l0_upt_,
lfb1._deleted_
FROM {Config.sourceDatabase}.lfb1 lfb1
WHERE lfb1._deleted_ = 'F'
AND lfb1.mandt = {Config.MANDT}
  
 
"""
    )

    return out0
