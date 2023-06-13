from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_mvmt_hdr_gmd_v2.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_gmd_v2.udfs.UDFs import *

def FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("_deleted_") == lit("F")) & (trim(col("DRSY")) == lit("00"))) & (col("DRRT") == lit("DT")))
    )
