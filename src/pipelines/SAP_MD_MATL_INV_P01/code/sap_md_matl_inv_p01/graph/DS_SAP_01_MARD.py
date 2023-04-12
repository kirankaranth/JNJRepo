from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_p01.config.ConfigStore import *
from sap_md_matl_inv_p01.udfs.UDFs import *

def DS_SAP_01_MARD(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.mchb WHERE _deleted_ = 'F'")