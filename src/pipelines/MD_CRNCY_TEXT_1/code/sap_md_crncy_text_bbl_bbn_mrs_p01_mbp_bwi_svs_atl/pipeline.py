from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_crncy_text_bbl_bbn_mrs_p01_mbp_bwi_svs_atl.config.ConfigStore import *
from sap_md_crncy_text_bbl_bbn_mrs_p01_mbp_bwi_svs_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_crncy_text_bbl_bbn_mrs_p01_mbp_bwi_svs_atl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_CRNCY_TEXT = sql_MD_CRNCY_TEXT(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_CRNCY_TEXT)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "2de11f0d-505b-4b66-b17b-15b49d23e6c2", 
        "06a16724-f530-433f-8f90-eeabfb3aaf57"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_CRNCY_TEXT_1")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_CRNCY_TEXT_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
