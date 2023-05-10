from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bwi_tai.config.ConfigStore import *
from sap_md_delv_line_bwi_tai.udfs.UDFs import *

def SELECT_LIKP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("VBELN"), col("LFART"), col("WADAT_IST"))
