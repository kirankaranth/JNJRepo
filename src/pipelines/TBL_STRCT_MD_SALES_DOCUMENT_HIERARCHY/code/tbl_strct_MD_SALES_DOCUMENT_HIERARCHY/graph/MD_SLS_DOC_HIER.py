from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SALES_DOCUMENT_HIERARCHY.config.ConfigStore import *
from tbl_strct_MD_SALES_DOCUMENT_HIERARCHY.udfs.UDFs import *

def MD_SLS_DOC_HIER(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("optimizeWrite", True)\
        .option("overwriteSchema", True)\
        .option("path", f"/mnt/{Config.targetEnv}_curdelta/{Config.targetApp}/{Config.targetDomain}/MD_SLS_DOC_HIER")\
        .mode("overwrite")\
        .partitionBy("SRC_SYS_CD")\
        .saveAsTable(f"{Config.targetSchema}.MD_SLS_DOC_HIER")
