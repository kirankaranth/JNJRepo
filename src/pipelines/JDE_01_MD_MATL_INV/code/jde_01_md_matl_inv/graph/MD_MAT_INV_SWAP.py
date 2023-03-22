from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def MD_MAT_INV_SWAP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", "dbfs:/mnt/dev_curdelta/l1_prophecy/md_material/md_matl_inv_swap")\
        .mode("overwrite")\
        .saveAsTable(f"l1_md_prophecy.md_matl_inv_swap")
