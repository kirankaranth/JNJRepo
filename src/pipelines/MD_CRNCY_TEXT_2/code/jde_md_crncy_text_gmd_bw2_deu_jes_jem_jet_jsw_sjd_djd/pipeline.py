from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.config.ConfigStore import *
from jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_CRNCY_TEXT = sql_MD_CRNCY_TEXT(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_CRNCY_TEXT)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "2c271c36-75ae-413d-b271-f5087bfd4995", 
        "9940ed50-0cdc-45fb-9b54-532f0201d543"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_CRNCY_TEXT_2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_CRNCY_TEXT_2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
