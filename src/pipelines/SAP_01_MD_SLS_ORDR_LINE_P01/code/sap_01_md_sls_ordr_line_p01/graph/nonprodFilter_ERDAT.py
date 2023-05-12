from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_p01.config.ConfigStore import *
from sap_01_md_sls_ordr_line_p01.udfs.UDFs import *

def nonprodFilter_ERDAT(spark: SparkSession, in0: DataFrame) -> DataFrame:

    if Config.nonProdFilter:
        out0 = in0.filter((col("ERDAT") > lit(f"{Config.nonProdFilterDate}")))
    else:
        out0 = in0

    return out0
