from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("DELV_NUM"), 
        col("DELV_LINE_NBR"), 
        col("DELV_TYP_CD"), 
        col("CO_CD"), 
        col("DOC_REF_NUM"), 
        col("ORDR_SFX"), 
        col("MATCH_TYPE"), 
        col("SRC_TBL_NM")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
