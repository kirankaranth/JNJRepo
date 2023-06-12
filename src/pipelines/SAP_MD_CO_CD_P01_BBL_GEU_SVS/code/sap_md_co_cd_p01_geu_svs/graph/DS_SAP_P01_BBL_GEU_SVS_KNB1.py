from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_p01_geu_svs.config.ConfigStore import *
from sap_md_co_cd_p01_geu_svs.udfs.UDFs import *

def DS_SAP_P01_BBL_GEU_SVS_KNB1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.knb1")
