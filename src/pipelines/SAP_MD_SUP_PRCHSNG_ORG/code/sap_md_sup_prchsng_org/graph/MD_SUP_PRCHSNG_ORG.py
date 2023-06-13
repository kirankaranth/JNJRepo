from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_prchsng_org.config.ConfigStore import *
from sap_md_sup_prchsng_org.udfs.UDFs import *

def MD_SUP_PRCHSNG_ORG(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_SUP_PRCHSNG_ORG")
