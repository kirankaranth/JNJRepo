from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def DUP_PK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
