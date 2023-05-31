from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_bba_fsn.config.ConfigStore import *
from sap_md_mfg_order_bba_fsn.udfs.UDFs import *

def AFKO(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.AFKO")
