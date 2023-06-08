from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_mbp.config.ConfigStore import *
from sap_md_matl_mbp.udfs.UDFs import *

def MARA_MBP(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mara")
