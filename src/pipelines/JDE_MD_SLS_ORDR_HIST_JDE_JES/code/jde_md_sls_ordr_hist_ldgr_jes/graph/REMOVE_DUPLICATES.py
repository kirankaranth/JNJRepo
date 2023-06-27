from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_ordr_hist_ldgr_jes.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_jes.udfs.UDFs import *

def REMOVE_DUPLICATES(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy("SRC_SYS_CD", "ORDR_TYPE", "UPDT_DTTM", "TIME_OF_DAY", "LINE_NUM", "ORDR_CO", "DOC_NUM")\
            .orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
