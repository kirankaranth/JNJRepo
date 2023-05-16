from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def ITEM_BASE_UOM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("UMITM", col("UMITM"))\
        .withColumn("IMLITM", lookup("IMLITM_LU", col("UMITM")).getField("IMLITM"))\
        .withColumn("IMUOM1", lookup("IMUOM1_LU", col("UMITM")).getField("IMUOM1"))
