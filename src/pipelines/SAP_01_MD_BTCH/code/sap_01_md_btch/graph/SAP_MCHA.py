from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_btch.config.ConfigStore import *
from sap_01_md_btch.udfs.UDFs import *

def SAP_MCHA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mcha")
