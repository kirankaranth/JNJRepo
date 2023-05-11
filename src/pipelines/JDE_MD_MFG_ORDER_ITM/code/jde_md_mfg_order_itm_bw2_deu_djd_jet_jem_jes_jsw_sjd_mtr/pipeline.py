from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr.config.ConfigStore import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr.udfs.UDFs import *
from prophecy.utils import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F4801 = DS_JDE_01_F4801(spark)
    df_DS_JDE_01_F4801 = collectMetrics(
        spark, 
        df_DS_JDE_01_F4801, 
        "graph", 
        "syPP44vQKj9afFHNy1jJ5$$cYaMqOPkx8dFtj_qnf2vB", 
        "Ep0nrXkk64Q4-sbtODXsT$$DlmTI5H9CY-v9WtcTVhRe"
    )
    df_MANDT_FILTER_AFPO = MANDT_FILTER_AFPO(spark, df_DS_JDE_01_F4801)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER_AFPO)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "bJwqmXgZBT6f8lM3mGcZy$$iLAgOm8-9jxVdaTIain-3", 
        "GSQ6C9IVNrrRk2yOqKoFQ$$-C84XdzzxAkidXP8aHllm"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_MFG_ORDER_ITM(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "60kyEZ314T4Vf9KxicpdC$$Ge7rUY094F8lPLf9rj3l-", 
        "ZEYSERAFDSvQbdAis9nmW$$SRvVAkiOnSNNSjO3byZLL"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MFG_ORDER_ITM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MFG_ORDER_ITM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
