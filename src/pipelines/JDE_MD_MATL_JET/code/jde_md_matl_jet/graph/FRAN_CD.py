from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def FRAN_CD(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("_deleted_") == lit("F")) & (trim(col("DRRT")) == lit(Config.FRAN_CD_FILTER))))
