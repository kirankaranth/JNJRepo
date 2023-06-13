from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_geu.config.ConfigStore import *
from sap_md_mfg_order_geu.udfs.UDFs import *

def LIST_STATUS(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT \r\n    OBJNR AS JEST_OBJNR, \r\n    CAST(collect_list(all STAT) AS string) AS STAT_LIST,\r\n    CAST(collect_list(all TXT04) AS string) AS TXT04_LIST  \r\nFROM \r\n    in0 \r\nGROUP BY OBJNR"
    )

    return df1
