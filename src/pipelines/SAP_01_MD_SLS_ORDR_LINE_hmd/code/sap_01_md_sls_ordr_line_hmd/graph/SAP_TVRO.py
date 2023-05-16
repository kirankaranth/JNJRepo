from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_hmd.config.ConfigStore import *
from sap_01_md_sls_ordr_line_hmd.udfs.UDFs import *

def SAP_TVRO(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tvro")
