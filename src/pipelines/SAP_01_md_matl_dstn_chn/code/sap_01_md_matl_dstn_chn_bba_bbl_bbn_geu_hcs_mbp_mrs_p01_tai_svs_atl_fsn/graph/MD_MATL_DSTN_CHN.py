from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.udfs.UDFs import *

def MD_MATL_DSTN_CHN(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_DSTN_CHN")
