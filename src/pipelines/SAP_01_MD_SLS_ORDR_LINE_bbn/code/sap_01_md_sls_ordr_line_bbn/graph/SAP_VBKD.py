from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_bbn.config.ConfigStore import *
from sap_01_md_sls_ordr_line_bbn.udfs.UDFs import *

def SAP_VBKD(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.vbkd WHERE _deleted_ = 'F'")
