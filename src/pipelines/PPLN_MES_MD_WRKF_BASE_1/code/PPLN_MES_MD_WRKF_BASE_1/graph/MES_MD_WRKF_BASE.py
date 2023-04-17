from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_WRKF_BASE_1.config.ConfigStore import *
from PPLN_MES_MD_WRKF_BASE_1.udfs.UDFs import *

def MES_MD_WRKF_BASE(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MES_MD_WRKF_BASE")
