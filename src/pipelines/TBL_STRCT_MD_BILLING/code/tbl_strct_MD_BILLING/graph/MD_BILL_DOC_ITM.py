from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BILLING.config.ConfigStore import *
from tbl_strct_MD_BILLING.udfs.UDFs import *

def MD_BILL_DOC_ITM(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("mergeSchema", True)\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_BILL_DOC_ITM")\
        .mode("append")\
        .partitionBy("SRC_SYS_CD")\
        .saveAsTable(f"{Config.targetSchema}.MD_BILL_DOC_ITM")
