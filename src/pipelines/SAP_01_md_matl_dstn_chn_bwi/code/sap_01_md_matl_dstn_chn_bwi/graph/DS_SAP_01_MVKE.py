from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_dstn_chn_bwi.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_bwi.udfs.UDFs import *

def DS_SAP_01_MVKE(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
