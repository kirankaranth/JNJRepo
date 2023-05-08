from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_btch.config.ConfigStore import *
from sap_01_md_btch.udfs.UDFs import *

def Reformat_MCH1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("SRC_SYS_CD"), col("SRC_TBL_NM"))
