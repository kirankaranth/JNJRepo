from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_hmd_hm2.config.ConfigStore import *
from sap_01_md_cust_hmd_hm2.udfs.UDFs import *

def DS_SAP_03_KNA1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.KNA1")
