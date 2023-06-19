from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.config.ConfigStore import *
from sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.udfs.UDFs import *

def sql_MD_BOM_ITM_NODE(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD
, stas.stlty as BOM_CAT_CD
, stas.stlnr AS BOM_NUM
, stas.stlal AS ALT_BOM_NUM
, stas.stlkn AS BOM_ITM_NODE_NUM
, stas.stasz AS BOM_ITM_NODE_CNTR_NBR
, case when stas.datuv = '00000000' then CAST(NULL AS timestamp)
else to_timestamp(stas.datuv, \"yyyyMMdd\") end as  BOM_ITM_VLD_FROM_DTTM
, TRIM(stas.aennr) as CHG_NUM
, case when stas.andat = '00000000' then CAST(NULL AS timestamp)
else to_timestamp(stas.andat, \"yyyyMMdd\") end as  CRT_DTTM
, case when stas.aedat = '00000000' then CAST(NULL AS timestamp)
else to_timestamp(stas.aedat, \"yyyyMMdd\") end as  CHG_DTTM
, TRIM(stas.lkenz) as DEL_IND,
stas._upt_ as _l0_upt_,
stas._deleted_
FROM  {Config.sourceDatabase}.stas as stas  WHERE stas._deleted_ = 'F'
and stas.MANDT = {Config.MANDT} 
"""
    )

    return out0
