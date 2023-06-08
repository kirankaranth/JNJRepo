from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sup_mtr_deu_jem_jsw_sjd.config.ConfigStore import *
from jde_md_sup_mtr_deu_jem_jsw_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_sup_mtr_deu_jem_jsw_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP = sql_MD_SUP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "d3a02106-30f7-4c79-b7e2-3511954b3684", 
        "1d48af1b-9fc9-4f51-849a-eb0178d90b18"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_12")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_12")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
