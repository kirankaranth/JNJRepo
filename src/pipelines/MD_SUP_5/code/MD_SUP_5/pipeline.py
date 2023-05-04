from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_SUP_5.config.ConfigStore import *
from MD_SUP_5.udfs.UDFs import *
from prophecy.utils import *
from MD_SUP_5.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP = sql_MD_SUP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "debb168d-7dd7-4df5-904b-d5da872e7476", 
        "cb88e248-c97d-4d15-81cb-b5b1f9fd7cf5"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_5")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_5")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
