from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_geu.config.ConfigStore import *
from sap_01_md_sls_ordr_line_geu.udfs.UDFs import *

def SAP_TVAPT(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tvapt")
