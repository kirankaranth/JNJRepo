from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def INOB(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.DBTABLE4}")
