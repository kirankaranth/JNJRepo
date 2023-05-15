from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_tai.config.ConfigStore import *
from sap_md_co_cd_tai.udfs.UDFs import *

def DS_SAP_TAI_KNB1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"tai.knb1")
