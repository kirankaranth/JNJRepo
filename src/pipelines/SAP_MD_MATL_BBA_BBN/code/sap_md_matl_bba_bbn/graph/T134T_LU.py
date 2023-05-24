from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bba_bbn.config.ConfigStore import *
from sap_md_matl_bba_bbn.udfs.UDFs import *

def T134T_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MTART''']
    valueColumns = ['''MTBEZ''']
    createLookup("T134T_LU", in0, spark, keyColumns, valueColumns)
