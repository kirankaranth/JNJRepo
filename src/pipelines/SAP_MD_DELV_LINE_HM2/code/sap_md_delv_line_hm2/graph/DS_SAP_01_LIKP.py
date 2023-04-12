from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *

def DS_SAP_01_LIKP(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.likp")
