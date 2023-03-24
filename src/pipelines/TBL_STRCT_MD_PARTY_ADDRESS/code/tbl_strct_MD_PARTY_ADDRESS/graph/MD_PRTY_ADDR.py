from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PARTY_ADDRESS.config.ConfigStore import *
from tbl_strct_MD_PARTY_ADDRESS.udfs.UDFs import *

def MD_PRTY_ADDR(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("mergeSchema", True)\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_PRTY_ADDR")\
        .mode("append")\
        .partitionBy("SRC_SYS_CD")\
        .saveAsTable(f"{Config.targetSchema}.MD_PRTY_ADDR")
