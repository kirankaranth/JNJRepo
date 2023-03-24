from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SALES_ORDER.config.ConfigStore import *
from tbl_strct_MD_SALES_ORDER.udfs.UDFs import *

def MD_SLS_RQR_INDIV_REC(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_SLS_RQR_INDIV_REC")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SLS_RQR_INDIV_REC")
