from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def IMLITM_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''IMITM''']
    valueColumns = ['''IMLITM''']
    createLookup("IMLITM_LU", in0, spark, keyColumns, valueColumns)
