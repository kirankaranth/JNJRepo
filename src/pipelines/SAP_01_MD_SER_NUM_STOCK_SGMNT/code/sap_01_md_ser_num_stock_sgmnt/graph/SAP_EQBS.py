from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def SAP_EQBS(spark: SparkSession) -> DataFrame:
    return spark.sql(f'SELECT * FROM {f"{Config.sourceDatabase}.eqbs"} WHERE _deleted_ = 'F'')
