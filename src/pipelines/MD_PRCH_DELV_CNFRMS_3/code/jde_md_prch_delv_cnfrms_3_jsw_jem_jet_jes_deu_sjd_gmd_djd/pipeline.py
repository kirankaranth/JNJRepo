from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd.config.ConfigStore import *
from jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_PRCH_DELV_CNFRMS = sql_MD_PRCH_DELV_CNFRMS(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_PRCH_DELV_CNFRMS)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "97de2387-dc47-4255-8dd6-e656454a335c", 
        "f25e5cfb-48a5-449e-ba5d-1b2096151568"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_PRCH_DELV_CNFRMS_3")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_PRCH_DELV_CNFRMS_3")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
