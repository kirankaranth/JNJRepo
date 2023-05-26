from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem.config.ConfigStore import *
from jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem.udfs.UDFs import *
from prophecy.utils import *
from jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP_CO = sql_MD_SUP_CO(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP_CO)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "b7a1c03c-d9fa-425b-8742-164ee9407f3e", 
        "914b9934-c446-497d-83d3-fcd648ebacf1"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_CO_6")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_CO_6")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
