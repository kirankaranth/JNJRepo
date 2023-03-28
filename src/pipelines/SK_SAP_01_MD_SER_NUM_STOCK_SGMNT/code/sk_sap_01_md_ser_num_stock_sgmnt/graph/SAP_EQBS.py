from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sk_sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sk_sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def SAP_EQBS(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bba.eqbs WHERE _deleted_ = 'F'")
