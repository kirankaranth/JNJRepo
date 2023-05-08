from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_btch.config.ConfigStore import *
from sap_01_md_btch.udfs.UDFs import *

def SchemaTransform_MCH1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("SRC_SYS_CD", lit("sourceSystem")).withColumn("SRC_TBL_NM", lit("DBTable2"))
