from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *

def SAP_T134T_LU_JNJ(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MTART''']
    valueColumns = ['''MTBEZ''']
    createLookup("SAP_T134T_LU_JNJ", in0, spark, keyColumns, valueColumns)
