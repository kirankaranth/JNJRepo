from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_PRCH_DELV_CNFRMS_7.config.ConfigStore import *
from MD_PRCH_DELV_CNFRMS_7.udfs.UDFs import *
from prophecy.utils import *
from MD_PRCH_DELV_CNFRMS_7.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_DELV_CNFRMS = sql_MD_PRCH_DELV_CNFRMS(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_DELV_CNFRMS)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "f4bd76ca-61a8-42d2-8cfa-ba010e2e6836", 
        "5e61a7f8-1b79-4daf-a4e7-933fc8a31818"
    )
    MD_PRCH_DELV_CNFRMS(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_PRCH_DELV_CNFRMS_7")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_PRCH_DELV_CNFRMS_7")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
