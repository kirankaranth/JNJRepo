from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_bwi.config.ConfigStore import *
from sap_md_delv_bwi.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_bwi.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_sql_MD_DELV = collectMetrics(
        spark, 
        df_sql_MD_DELV, 
        "graph", 
        "fc07c147-25e4-4855-b3fd-ab019eb84adf", 
        "e47bfef6-68d5-445d-8f8b-fc04d670e080"
    )
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "9a12d2ef-b3ac-4f2b-be4e-e122071cf8de", 
        "744bc30d-6065-4bee-b19c-72c2a4a87948"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_3")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_3")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
