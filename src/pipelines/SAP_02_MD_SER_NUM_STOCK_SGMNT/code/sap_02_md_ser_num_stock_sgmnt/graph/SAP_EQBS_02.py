from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_02_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_02_md_ser_num_stock_sgmnt.udfs.UDFs import *

def SAP_EQBS_02(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.DBNAME}.eqbs WHERE _deleted_ = 'F'")
