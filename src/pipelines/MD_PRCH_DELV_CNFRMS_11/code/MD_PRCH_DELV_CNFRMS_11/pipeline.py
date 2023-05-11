from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_PRCH_DELV_CNFRMS_11.config.ConfigStore import *
from MD_PRCH_DELV_CNFRMS_11.udfs.UDFs import *
from prophecy.utils import *
from MD_PRCH_DELV_CNFRMS_11.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_DELV_CNFRMS = sql_MD_PRCH_DELV_CNFRMS(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_DELV_CNFRMS)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "826f387c-6b44-4292-a99e-20091fd0811d", 
        "6d549831-d75f-414d-90ab-b461545e0233"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_PRCH_DELV_CNFRMS_11")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_PRCH_DELV_CNFRMS_11")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
