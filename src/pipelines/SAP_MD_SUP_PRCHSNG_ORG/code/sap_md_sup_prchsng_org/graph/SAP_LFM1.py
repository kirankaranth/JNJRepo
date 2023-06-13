from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sup_prchsng_org.config.ConfigStore import *
from sap_md_sup_prchsng_org.udfs.UDFs import *

def SAP_LFM1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.lfm1")
