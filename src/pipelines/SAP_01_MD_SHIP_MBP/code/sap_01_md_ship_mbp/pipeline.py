from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_ship_mbp.config.ConfigStore import *
from sap_01_md_ship_mbp.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_ship_mbp.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VTTK = SAP_VTTK(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VTTK)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE = DUPLICATE(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE)
    MD_SHIP(spark, df_SET_FIELD_ORDER_FORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SHIP_MBP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SHIP_MBP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
