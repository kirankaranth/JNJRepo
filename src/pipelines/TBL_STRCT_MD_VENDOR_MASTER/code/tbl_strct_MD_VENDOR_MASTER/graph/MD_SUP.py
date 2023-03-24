from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_VENDOR_MASTER.config.ConfigStore import *
from tbl_strct_MD_VENDOR_MASTER.udfs.UDFs import *

def MD_SUP(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_SUP")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SUP")
