from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_mbp.config.ConfigStore import *
from sap_md_delv_line_mbp.udfs.UDFs import *

def DS_SAP_05_LIKP(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM mbp.likp WHERE _deleted_ = 'F'")
