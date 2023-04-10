from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bwi_tai.config.ConfigStore import *
from sap_md_delv_line_bwi_tai.udfs.UDFs import *

def DS_SAP_02_VBUP(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.vbup WHERE _deleted_ = 'F'")
