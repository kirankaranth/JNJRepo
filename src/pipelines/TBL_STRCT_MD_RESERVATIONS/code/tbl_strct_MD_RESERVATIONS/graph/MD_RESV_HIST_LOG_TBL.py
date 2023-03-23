from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_RESERVATIONS.config.ConfigStore import *
from tbl_strct_MD_RESERVATIONS.udfs.UDFs import *

def MD_RESV_HIST_LOG_TBL(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_RESV_HIST_LOG_TBL")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_RESV_HIST_LOG_TBL")
