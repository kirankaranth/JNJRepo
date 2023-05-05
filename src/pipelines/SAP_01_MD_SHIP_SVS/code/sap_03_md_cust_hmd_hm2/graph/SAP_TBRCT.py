from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_03_md_cust_hmd_hm2.config.ConfigStore import *
from sap_03_md_cust_hmd_hm2.udfs.UDFs import *

def SAP_TBRCT(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tbrct")
