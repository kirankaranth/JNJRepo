from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def LU_SAP_TVV1T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''KVGR1''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVV1T", in0, spark, keyColumns, valueColumns)
