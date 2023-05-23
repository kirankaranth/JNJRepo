from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut_hist.config.ConfigStore import *
from sap_md_matl_valut_hist.udfs.UDFs import *

def DS_SAP_02_MARA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"bba.mara")
