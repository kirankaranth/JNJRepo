from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_mvmt_hdr_gmd_v2.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_gmd_v2.udfs.UDFs import *

def MD_MATL_MVMT_HDR(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MATL_MVMT_HDR")
