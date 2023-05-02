from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_cust_hcs.config.ConfigStore import *
from sap_01_md_cust_hcs.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_cust_hcs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVAST = SAP_TVAST(spark)
    df_MANDT_FILTER_TVAST = MANDT_FILTER_TVAST(spark, df_SAP_TVAST)
    LU_SAP_TVAST(spark, df_MANDT_FILTER_TVAST)
    df_DS_SAP_02_KNA1 = DS_SAP_02_KNA1(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_02_KNA1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_CUST(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_CUST_HCS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_CUST_HCS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
