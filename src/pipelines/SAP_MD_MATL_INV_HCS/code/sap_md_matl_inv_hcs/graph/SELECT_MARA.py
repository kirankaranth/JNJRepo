from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_hcs.config.ConfigStore import *
from sap_md_matl_inv_hcs.udfs.UDFs import *

def SELECT_MARA(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("MATNR"), col("MEINS"))
