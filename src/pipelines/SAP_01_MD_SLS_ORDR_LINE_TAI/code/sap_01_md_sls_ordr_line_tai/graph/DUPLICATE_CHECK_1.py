from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_tai.config.ConfigStore import *
from sap_01_md_sls_ordr_line_tai.udfs.UDFs import *

def DUPLICATE_CHECK_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
