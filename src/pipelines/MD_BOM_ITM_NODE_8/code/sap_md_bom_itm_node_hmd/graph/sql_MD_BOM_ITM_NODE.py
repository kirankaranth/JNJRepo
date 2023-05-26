from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_itm_node_hmd.config.ConfigStore import *
from sap_md_bom_itm_node_hmd.udfs.UDFs import *

def sql_MD_BOM_ITM_NODE(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT
 '{Config.sourceSystem}' SRC_SYS_CD,
STAS.stlty as BOM_CAT_CD,
STAS.stlnr as BOM_NUM,
STAS.stlal as ALT_BOM_NUM,
STAS.stlkn as BOM_ITM_NODE_NUM,
STAS.stasz as BOM_ITM_NODE_CNTR_NBR,
case when STAS.datuv = '00000000' then null else to_timestamp(STAS.datuv,\"yyyyMMdd\") end AS BOM_ITM_VLD_FROM_DTTM,
TRIM(STAS.aennr) as CHG_NUM,
case when STAS.andat = '00000000' then null else to_timestamp(STAS.andat,\"yyyyMMdd\") end AS CRT_DTTM,
case when STAS.aedat = '00000000' then null else to_timestamp(STAS.aedat,\"yyyyMMdd\") end AS CHG_DTTM,
TRIM(STAS.lkenz) as DEL_IND,
stas._upt_ as _l0_upt_,
stas._deleted_
FROM {Config.sourceDatabase}.STAS STAS
where
STAS._deleted_=\"F\" AND
STAS.MANDT=100 
 
"""
    )

    return out0
