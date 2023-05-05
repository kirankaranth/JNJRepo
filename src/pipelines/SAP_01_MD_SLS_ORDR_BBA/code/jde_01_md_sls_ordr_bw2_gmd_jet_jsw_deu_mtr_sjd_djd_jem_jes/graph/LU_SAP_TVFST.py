from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def LU_SAP_TVFST(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''FAKSP''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVFST", in0, spark, keyColumns, valueColumns)
