from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_inv.config.ConfigStore import *
from sap_01_md_matl_inv.udfs.UDFs import *

def DS_SAP_01_MARA(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM hm2.mara WHERE _deleted_ = 'F'")
