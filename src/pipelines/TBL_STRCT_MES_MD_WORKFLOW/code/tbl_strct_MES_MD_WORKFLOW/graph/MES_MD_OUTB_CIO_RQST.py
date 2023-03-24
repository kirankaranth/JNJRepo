from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_WORKFLOW.config.ConfigStore import *
from tbl_strct_MES_MD_WORKFLOW.udfs.UDFs import *

def MES_MD_OUTB_CIO_RQST(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("mergeSchema", True)\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MES_MD_OUTB_CIO_RQST")\
        .mode("append")\
        .partitionBy("SRC_SYS_CD")\
        .saveAsTable(f"{Config.targetSchema}.MES_MD_OUTB_CIO_RQST")
