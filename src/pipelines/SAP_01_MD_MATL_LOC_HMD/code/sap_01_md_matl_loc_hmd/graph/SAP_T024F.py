from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc_hmd.config.ConfigStore import *
from sap_01_md_matl_loc_hmd.udfs.UDFs import *

def SAP_T024F(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.T024F")
