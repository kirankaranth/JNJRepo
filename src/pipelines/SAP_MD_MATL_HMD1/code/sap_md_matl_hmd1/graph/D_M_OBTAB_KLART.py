from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def D_M_OBTAB_KLART(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT)))
          & ((col("OBTAB") == lit("MARA")) & (col("KLART") == lit("001")))
        )
    )
