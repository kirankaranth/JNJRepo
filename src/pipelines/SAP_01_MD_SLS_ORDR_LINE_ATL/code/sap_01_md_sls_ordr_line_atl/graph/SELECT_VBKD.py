from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_atl.config.ConfigStore import *
from sap_01_md_sls_ordr_line_atl.udfs.UDFs import *

def SELECT_VBKD(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("VBELN"), col("POSNR"), col("INCO1"), col("INCO2"), col("BSTKD"), col("BSTDK"), col("PRSDT"))
