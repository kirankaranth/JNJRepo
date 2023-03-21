from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def MD_MATL_LOC_SWAP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.SRC_SYS_CD}'")\
        .option("overwriteSchema", True)\
        .option("path", "dbfs:/mnt/dev_curdelta/l1_prophecy/md_material/md_matl_loc_swap")\
        .mode("overwrite")\
        .saveAsTable(f"l1_md_prophecy.md_matl_loc_swap")
