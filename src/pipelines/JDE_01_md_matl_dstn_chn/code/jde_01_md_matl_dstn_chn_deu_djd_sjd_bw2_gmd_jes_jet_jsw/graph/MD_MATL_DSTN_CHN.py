from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw.config.ConfigStore import *
from jde_01_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw.udfs.UDFs import *

def MD_MATL_DSTN_CHN(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_DSTN_CHN")
