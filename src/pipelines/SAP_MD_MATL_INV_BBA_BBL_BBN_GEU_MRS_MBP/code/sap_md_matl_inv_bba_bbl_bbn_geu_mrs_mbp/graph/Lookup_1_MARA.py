from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp.config.ConfigStore import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp.udfs.UDFs import *

def Lookup_1_MARA(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MATNR''']
    valueColumns = ['''MEINS''']
    createLookup("LU_MARA_MEINS", in0, spark, keyColumns, valueColumns)
