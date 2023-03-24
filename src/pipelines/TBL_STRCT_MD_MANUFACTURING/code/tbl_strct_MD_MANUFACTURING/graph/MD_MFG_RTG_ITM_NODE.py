from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MANUFACTURING.config.ConfigStore import *
from tbl_strct_MD_MANUFACTURING.udfs.UDFs import *

def MD_MFG_RTG_ITM_NODE(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_MFG_RTG_ITM_NODE")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_MFG_RTG_ITM_NODE")
