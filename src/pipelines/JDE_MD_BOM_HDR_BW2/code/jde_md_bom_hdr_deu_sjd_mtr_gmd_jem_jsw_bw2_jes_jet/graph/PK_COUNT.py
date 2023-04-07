from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.config.ConfigStore import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.udfs.UDFs import *

def PK_COUNT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("BOM_CAT_CD"), col("BOM_NUM"), col("ALT_BOM_NUM"), col("BOM_CNTR_NBR"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
