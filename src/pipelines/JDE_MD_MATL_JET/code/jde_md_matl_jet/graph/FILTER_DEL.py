from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def FILTER_DEL(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "/* I am using a custom SQL for the filter as I will use it to both filter and select fields. This is because having _deleted_ come through \r\nthe join gem twice was an ambiguity issue given I now have to use it in the data (not hard code).  */\r\nSELECT \r\n    XBITM,\r\n    XB_T003,\r\n    XB_T162\r\nFROM\r\n    in0\r\nWHERE\r\n    _deleted_ = 'F'"
    )

    return df1
