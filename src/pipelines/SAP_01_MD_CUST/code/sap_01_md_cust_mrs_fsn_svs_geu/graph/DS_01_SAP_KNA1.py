from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_mrs_fsn_svs_geu.config.ConfigStore import *
from sap_01_md_cust_mrs_fsn_svs_geu.udfs.UDFs import *

def DS_01_SAP_KNA1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.kna1")
