from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def Lookup_03_T006A(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MSEHI''']
    valueColumns = ['''MSEHT''']
    createLookup("LU_T006A_MSEHT", in0, spark, keyColumns, valueColumns)
