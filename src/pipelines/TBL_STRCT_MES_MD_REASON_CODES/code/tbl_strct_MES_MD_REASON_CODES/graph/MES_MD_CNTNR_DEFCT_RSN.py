from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_REASON_CODES.config.ConfigStore import *
from tbl_strct_MES_MD_REASON_CODES.udfs.UDFs import *

def MES_MD_CNTNR_DEFCT_RSN(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("overwriteSchema", True)\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MES_MD_CNTNR_DEFCT_RSN")\
        .mode("overwrite")\
        .partitionBy("SRC_SYS_CD")\
        .saveAsTable(f"{Config.targetSchema}.MES_MD_CNTNR_DEFCT_RSN")
