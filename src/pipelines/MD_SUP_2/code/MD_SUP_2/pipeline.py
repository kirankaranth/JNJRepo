from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_SUP_2.config.ConfigStore import *
from MD_SUP_2.udfs.UDFs import *
from prophecy.utils import *
from MD_SUP_2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP = sql_MD_SUP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "06f1b7ef-5887-4e45-9ff6-41f8b3a482f4", 
        "a746d54e-7351-4dd1-940f-57af8ee151ce"
    )
    MD_SUP(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
