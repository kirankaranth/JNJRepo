from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_prch_delv_cnfrms_mbp.config.ConfigStore import *
from sap_md_prch_delv_cnfrms_mbp.udfs.UDFs import *

def MD_PRCH_DELV_CNFRMS(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_PRCH_DELV_CNFRMS")
