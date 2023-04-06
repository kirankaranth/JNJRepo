from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.config.ConfigStore import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("BOM_CAT_CD"), 
        col("BOM_NUM"), 
        col("ALT_BOM_NUM"), 
        col("BOM_CNTR_NBR"), 
        col("BOM_VLD_FROM_DTTM"), 
        col("CHG_NUM"), 
        col("CRT_DTTM"), 
        col("CHG_DTTM"), 
        col("BOM_TXT"), 
        col("BOM_VLD_TO_DTTM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM")
    )
