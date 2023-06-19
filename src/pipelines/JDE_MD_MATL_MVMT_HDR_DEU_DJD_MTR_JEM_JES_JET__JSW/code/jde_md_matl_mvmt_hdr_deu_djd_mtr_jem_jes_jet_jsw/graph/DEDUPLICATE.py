from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy(
              "SRC_SYS_CD", 
              "MATL_MVMT_NUM", 
              "MATL_MVMT_YR", 
              "SPLT_GUID_PART1", 
              "SPLT_GUID_PART2", 
              "SPLT_GUID_PART3", 
              "SPLT_GUID_PART4", 
              "SPLT_GUID_PART5", 
              "SPLT_GUID_PART6"
            )\
            .orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
