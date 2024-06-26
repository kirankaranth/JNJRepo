from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_btch.config.ConfigStore import *
from sap_md_btch.udfs.UDFs import *

def SAP_MCH1_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
