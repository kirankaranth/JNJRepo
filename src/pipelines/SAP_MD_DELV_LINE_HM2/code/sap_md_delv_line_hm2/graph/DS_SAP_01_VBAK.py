from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *

def DS_SAP_01_VBAK(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.vbak WHERE _deleted_ = 'F'")
