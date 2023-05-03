from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_ship.config.ConfigStore import *
from sap_01_md_ship.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_ship.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VTTK = SAP_VTTK(spark)
    df_SAP_VTTK = collectMetrics(
        spark, 
        df_SAP_VTTK, 
        "graph", 
        "wPtVFhlVP0NkypCCCDBH2$$mcFyb5uLSOceFjRaGmwOU", 
        "bcX1xNM46AhoG-V9ZbCvT$$Rq-qgzvt7P7Aah4z47_dH"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VTTK)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_FORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_FORMAT, 
        "graph", 
        "DIGNpZvPT1ZdJ6jFVrfIy$$PW7jhEBfGmtJPTp89B_-S", 
        "WAX9BeKQuT9N-RUQ8Q5WL$$veH4UKD11IS8apEuRQxMK"
    )
    df_DUPLICATE = DUPLICATE(spark, df_SET_FIELD_ORDER_FORMAT)
    MD_SHIP(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "CK7r5Bl9QpVq4RL8jLJvN$$cuPPsnJqJUzaLYuD9ce64", 
        "iLwW7UPIPBlz3dRvW2WZU$$JXlGG2rZpzLyL-PEuUSyy"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SHIP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SHIP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
