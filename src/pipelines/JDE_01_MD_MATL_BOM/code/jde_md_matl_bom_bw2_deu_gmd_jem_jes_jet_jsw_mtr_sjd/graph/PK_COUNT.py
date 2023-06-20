from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.config.ConfigStore import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.udfs.UDFs import *

def PK_COUNT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("PLNT_CD"), 
        col("BOM_USG_CD"), 
        col("BOM_NUM"), 
        col("ALT_BOM_NUM")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
