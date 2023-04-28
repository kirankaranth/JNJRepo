from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def Lookup_05_T006T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''DIMID''']
    valueColumns = ['''TXDIM''']
    createLookup("LU_T006T_TXDIM", in0, spark, keyColumns, valueColumns)
