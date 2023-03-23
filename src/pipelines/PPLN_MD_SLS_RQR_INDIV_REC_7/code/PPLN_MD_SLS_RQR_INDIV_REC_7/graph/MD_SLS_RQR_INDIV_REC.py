from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MD_SLS_RQR_INDIV_REC_7.config.ConfigStore import *
from PPLN_MD_SLS_RQR_INDIV_REC_7.udfs.UDFs import *

def MD_SLS_RQR_INDIV_REC(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SLS_RQR_INDIV_REC")
