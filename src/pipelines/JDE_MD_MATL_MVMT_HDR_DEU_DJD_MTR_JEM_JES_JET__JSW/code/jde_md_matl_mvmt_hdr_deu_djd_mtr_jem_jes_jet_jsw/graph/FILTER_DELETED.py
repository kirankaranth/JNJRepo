from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.udfs.UDFs import *

def FILTER_DELETED(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
