from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.udfs.UDFs import *

def MFG_ORDER(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MFG_ORDER")
