from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sap_02_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_02_md_ser_num_stock_sgmnt.udfs.UDFs import *

def MANDT_Filter_Parameter(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("MANDT") == lit(Config.MANDT)))
