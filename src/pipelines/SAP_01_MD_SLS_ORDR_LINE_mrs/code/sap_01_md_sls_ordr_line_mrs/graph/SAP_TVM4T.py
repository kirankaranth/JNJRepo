from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_mrs.config.ConfigStore import *
from sap_01_md_sls_ordr_line_mrs.udfs.UDFs import *

def SAP_TVM4T(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.tvm4t WHERE spras = 'E'")
