from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_crncy_svs_bwi_atl.config.ConfigStore import *
from sap_md_crncy_svs_bwi_atl.udfs.UDFs import *

def DS_SAP_01_TCURC(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.tcurc")
