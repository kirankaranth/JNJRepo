from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_hcs.config.ConfigStore import *
from sap_01_md_sls_ordr_line_hcs.udfs.UDFs import *

def SAP_TVST(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tvst")
