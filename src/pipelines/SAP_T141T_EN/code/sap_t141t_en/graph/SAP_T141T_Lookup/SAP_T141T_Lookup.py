from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *

def SAP_T141T_Lookup(spark: SparkSession) -> None:
    df_SAP_T141T_EN = SAP_T141T_EN(spark)
    LU_T141T_EN(spark, df_SAP_T141T_EN)
