from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.config.ConfigStore import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0116 = JDE_F0116(spark)
    df_JDE_F0116 = collectMetrics(
        spark, 
        df_JDE_F0116, 
        "graph", 
        "NbDhgrZZOVotu_LgAcRUq$$qUg9PowtJa7-G65aFEy1b", 
        "O_DQKOyEBnZfHrkQX_FOc$$0eQyIngIn3fmq1gm2QvL9"
    )
    df_JDE_F0116_FILTER = JDE_F0116_FILTER(spark, df_JDE_F0116)
    df_JDE_F0006 = JDE_F0006(spark)
    df_JDE_F0006 = collectMetrics(
        spark, 
        df_JDE_F0006, 
        "graph", 
        "UFITQd1H2EiaZZOJIuYQm$$wj2_8cIdgj0ToOFDRfS1Q", 
        "q1yiwG83r7EbrIkavD8qL$$wRaCGs3nmvo8JeWl_xj_T"
    )
    df_JDE_F0006_FILTER = JDE_F0006_FILTER(spark, df_JDE_F0006)
    df_Join_JDE = Join_JDE(spark, df_JDE_F0006_FILTER, df_JDE_F0116_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_JDE)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "YtyZow5aU6_oUIUrbCOny$$tXl75YElEb9WBf2yki1OF", 
        "LJGMJ0y9HdVHL28DWn-QX$$5pcQIvezlxpfh-NlUo-DV"
    )
    MD_PLNT(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_FILTER = DUPLICATE_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_FILTER, 
        "graph", 
        "5RV7zOz6Cz5SjaIvx94wS$$Qi5C7O_AMnfMcFlNRVmp_", 
        "gT3k40mR7jB6-brvV9n6p$$ihFZPEcusDshPwqOnYT4z"
    )
    df_DUPLICATE_FILTER.cache().count()
    df_DUPLICATE_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_PLNT_DEU_DJD_SJD_JEM_JSW_JET_JES_BW2_GMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_PLNT_DEU_DJD_SJD_JEM_JSW_JET_JES_BW2_GMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
