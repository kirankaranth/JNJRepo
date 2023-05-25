from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.config.ConfigStore import *
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0101 = JDE_F0101(spark)
    df_JDE_F0101 = collectMetrics(
        spark, 
        df_JDE_F0101, 
        "graph", 
        "w9aGBN7VhkHE3g6lNMXeP$$BbnUI2G1UEp4aaGdZ0qui", 
        "VJ94OcEjhbturTFJnNte4$$Uo6ETY2Lk5UFdbLI18HKI"
    )
    df_JDE_F0101_FILTER = JDE_F0101_FILTER(spark, df_JDE_F0101)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F0101_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "KmmJf7wa0IHg_1ar3Nj1C$$xNQ6p-vL0gtK3RIyDYMSQ", 
        "P0yp7kcAubRlQ319J3FXQ$$9jnW2Zgc1zpxzsk2OnPaB"
    )
    MD_CUST(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_CUST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_CUST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
