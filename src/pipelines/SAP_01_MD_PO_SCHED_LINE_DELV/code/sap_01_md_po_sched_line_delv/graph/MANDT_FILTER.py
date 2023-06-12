from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_po_sched_line_delv.config.ConfigStore import *
from sap_01_md_po_sched_line_delv.udfs.UDFs import *

def MANDT_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("MANDT") == lit(Config.MANDT)))
