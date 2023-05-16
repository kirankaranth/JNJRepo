from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bom.config.ConfigStore import *
from sap_md_matl_bom.udfs.UDFs import *

def Filter_SAP_01_MAST(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_DELETED_") == lit("F"))))
