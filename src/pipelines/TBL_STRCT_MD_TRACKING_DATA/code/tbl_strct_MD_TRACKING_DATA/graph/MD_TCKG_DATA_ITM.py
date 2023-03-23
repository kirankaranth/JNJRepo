from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_TRACKING_DATA.config.ConfigStore import *
from tbl_strct_MD_TRACKING_DATA.udfs.UDFs import *

def MD_TCKG_DATA_ITM(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_TCKG_DATA_ITM")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_TCKG_DATA_ITM")