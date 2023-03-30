from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sk_sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sk_sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def MANDT_Filter_Parameter(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("MANDT") == lit(Config.MANDT)))
