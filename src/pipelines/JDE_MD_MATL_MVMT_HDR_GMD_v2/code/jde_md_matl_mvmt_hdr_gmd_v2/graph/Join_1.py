from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_gmd_v2.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_gmd_v2.udfs.UDFs import *

def Join_1(spark: SparkSession, F4111: DataFrame, F0005: DataFrame, ) -> DataFrame:
    return F4111\
        .alias("F4111")\
        .join(F0005.alias("F0005"), (trim(col("f4111.ildct")) == trim(col("f0005.DRKY"))), "left_outer")\
        .select(col("F4111.ILUKID").alias("ILUKID"), col("F4111.ILTRDJ").alias("ILTRDJ"), col("F4111.ILRCD").alias("ILRCD"), col("F4111.ILDGL").alias("ILDGL"), col("F4111.ILTREX").alias("ILTREX"), col("F4111.ILDCT").alias("ILDCT"), col("F4111.ILDCTO").alias("ILDCTO"), col("F0005.DRDL01").alias("DRDL01"), col("F4111.ILCRDJ").alias("ILCRDJ"), col("F4111._upt_").alias("_upt_"), col("F4111._pk_").alias("_pk_"))
