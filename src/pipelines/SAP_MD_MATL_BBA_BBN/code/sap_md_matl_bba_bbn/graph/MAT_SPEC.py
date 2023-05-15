from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bba_bbn.config.ConfigStore import *
from sap_md_matl_bba_bbn.udfs.UDFs import *

def MAT_SPEC(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("cabn_filter") == lit("MATERIAL_SPEC")))
