from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from ngem_material_cross_reference.config.ConfigStore import *
from ngem_material_cross_reference.udfs.UDFs import *

def DS_NGEM_MATERIAL_CROSS_REFERENCE(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"p360.material_cross_reference")
