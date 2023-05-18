from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.config.ConfigStore import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("CO_CD"), col("CUST_NUM"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
