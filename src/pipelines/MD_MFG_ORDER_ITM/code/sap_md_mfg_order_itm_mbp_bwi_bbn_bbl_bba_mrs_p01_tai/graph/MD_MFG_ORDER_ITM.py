from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.config.ConfigStore import *
from sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.udfs.UDFs import *

def MD_MFG_ORDER_ITM(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MFG_ORDER_ITM")
