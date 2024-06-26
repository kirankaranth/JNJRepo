from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sup_co_bw2_jes_djd.config.ConfigStore import *
from jde_md_sup_co_bw2_jes_djd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_sup_co_bw2_jes_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP_CO = sql_MD_SUP_CO(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP_CO)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "2d5fefe9-3418-4bb9-9702-b8542fc59889", 
        "fce3f41c-e335-488a-8fea-01a5f6399c7a"
    )
    MD_SUP_CO(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_CO_9")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_CO_9")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
