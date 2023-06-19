from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.udfs.UDFs import *

def DEDUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("MATL_MVMT_NUM"), 
        col("MATL_MVMT_YR"), 
        col("SPLT_GUID_PART1"), 
        col("SPLT_GUID_PART2"), 
        col("SPLT_GUID_PART3"), 
        col("SPLT_GUID_PART4"), 
        col("SPLT_GUID_PART5"), 
        col("SPLT_GUID_PART6")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
