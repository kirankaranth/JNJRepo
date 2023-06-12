from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_mbp.config.ConfigStore import *
from sap_md_delv_mbp.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_mbp.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "b49e90cd-4c26-45de-a3b2-1b0e70e24e78", 
        "076fd973-3623-4e9b-b9cf-e58a1ce8a089"
    )
    MD_DELV(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_8")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_8")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
