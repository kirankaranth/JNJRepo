from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("CRNCY_CD"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
