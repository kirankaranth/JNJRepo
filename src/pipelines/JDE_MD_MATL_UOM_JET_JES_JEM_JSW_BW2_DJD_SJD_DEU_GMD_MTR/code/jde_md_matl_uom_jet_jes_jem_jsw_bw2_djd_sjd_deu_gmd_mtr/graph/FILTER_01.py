from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.config.ConfigStore import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.udfs.UDFs import *

def FILTER_01(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((col("_deleted_") == lit("F")) & (trim(col("DRSY")) == lit("00")))
          & ((trim(col("DRRT")) == lit("UM")) & (length(trim(col("DRKY"))) > lit(0)))
        )
    )
