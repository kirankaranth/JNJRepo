from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_sup_3_tai.config.ConfigStore import *
from md_sup_3_tai.udfs.UDFs import *
from prophecy.utils import *
from md_sup_3_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP = sql_MD_SUP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "3a16d24d-1d41-46c8-9d1c-4a0909360e10", 
        "06f18eb1-aa9f-48ca-95b5-252f16ea4e92"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_3")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_3")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
