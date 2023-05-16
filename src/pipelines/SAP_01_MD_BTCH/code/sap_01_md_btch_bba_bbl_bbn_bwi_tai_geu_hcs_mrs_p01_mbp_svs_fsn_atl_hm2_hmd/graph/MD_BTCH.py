from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd.config.ConfigStore import *
from sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd.udfs.UDFs import *

def MD_BTCH(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_BTCH")
