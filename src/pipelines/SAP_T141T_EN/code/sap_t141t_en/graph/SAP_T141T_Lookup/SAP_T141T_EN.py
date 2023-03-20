from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_t141t_en.config.ConfigStore import *
from sap_t141t_en.udfs.UDFs import *

def SAP_T141T_EN(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM hm2.t141t WHERE _deleted_ = 'F' and SPRAS = 'E'")
