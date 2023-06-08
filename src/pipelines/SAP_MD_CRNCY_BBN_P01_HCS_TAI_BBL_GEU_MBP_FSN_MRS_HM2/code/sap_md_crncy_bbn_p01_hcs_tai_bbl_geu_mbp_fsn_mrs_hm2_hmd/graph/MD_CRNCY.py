from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.config.ConfigStore import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.udfs.UDFs import *

def MD_CRNCY(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_CRNCY")
