from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_gmd_v2.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_gmd_v2.udfs.UDFs import *

def DS_JDE_F0005(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.f0005")
