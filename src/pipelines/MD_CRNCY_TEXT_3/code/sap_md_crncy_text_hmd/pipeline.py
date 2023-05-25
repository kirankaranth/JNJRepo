from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_crncy_text_hmd.config.ConfigStore import *
from sap_md_crncy_text_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_crncy_text_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_CRNCY_TEXT = sql_MD_CRNCY_TEXT(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_CRNCY_TEXT)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "a24210f4-451b-4376-87ff-d96a44f09bed", 
        "855635e4-b41e-4303-9ba6-faaf946b5748"
    )
    MD_CRNCY_TEXT(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_CRNCY_TEXT_3")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_CRNCY_TEXT_3")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
