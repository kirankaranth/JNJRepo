from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.config.ConfigStore import *
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.udfs.UDFs import *

def MD_MATL_LOC(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_LOC")
